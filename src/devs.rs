use std::{io, sync::{Arc, LazyLock}};

use actix_web::{HttpResponse, Responder, Result, get, http::StatusCode, post, web::{Data, Json}};
use arc_swap::ArcSwapOption;
use redb::{Database, ReadableDatabase, ReadableTable, StorageError, TableDefinition, TypeName};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc::UnboundedSender, task::spawn_blocking};

use crate::{PendingDev, arc_str::ArcStr, env_var, unix_secs};

type DevCache = ArcSwapOption<Vec<DevPlayer>>;

static UPDATE_PASSWORD: LazyLock<String> = LazyLock::new(|| env_var("UPDATE_PASSWORD", "CHANGETHIS_UPDATE".to_owned ()));
static CREATE_PASSWORD: LazyLock<String> = LazyLock::new(|| env_var("CREATE_PASSWORD", "CHANGETHIS_CREATE".to_owned ()));
static DELETE_PASSWORD: LazyLock<String> = LazyLock::new(|| env_var("DELETE_PASSWORD", "CHANGETHIS_DELETE".to_owned ()));

pub const DEV_TABLE: TableDefinition<ArcStr, DevPlayer> = TableDefinition::new("dev_data");
pub const DEV_TTLS: TableDefinition<ArcStr, GenerationalDeleteTime> = TableDefinition::new("dev_ttls");

pub struct CachedDevs {
    pub devs: DevCache
}

impl CachedDevs {
    pub fn new() -> Self {
        Self {
            devs: ArcSwapOption::const_empty(),
        }
    }
}

#[get("/")]
pub async fn devs_list(
    db: Data<Database>,
    devs: Data<CachedDevs>
) -> Result<impl Responder> {
    #[allow(clippy::single_match_else)]
    let devs = match devs.devs.load().as_ref().cloned() {
        Some(devs) => devs,
        None => spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(io::Error::other)?;
            let table = read_txn.open_table(DEV_TABLE).map_err(io::Error::other)?;
            let items = table.iter().map_err(io::Error::other)?;
            let items = items
                .map(|res| {
                    let (_, value) = res?;
                    Ok::<_, StorageError>(value.value())
                })
                .collect::<Result<Vec<_>, _>>().map_err(io::Error::other)?;
            let items = Arc::new(items);
            
            devs.devs.store(Some(items.clone()));
            Ok::<_, io::Error>(items)
        }).await.map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))??,
    };

    Ok(HttpResponse::Ok().json(&*devs))   
}

#[post("/")]
pub async fn update_devs(
    body: Json<UpdateData>,
    db: Data<Database>,
    devs: Data<CachedDevs>,
    ttl_tx: Data<UnboundedSender<PendingDev>>
) -> Result<impl Responder> {
    let data = body.into_inner();

    let res = spawn_blocking(move || {
        match &data.password {
            password if password == &*UPDATE_PASSWORD => {
                let read_txn = db.begin_read().map_err(io::Error::other)?;
                let table = read_txn.open_table(DEV_TABLE).map_err(io::Error::other)?;
                let dev = table.get(&data.dev_name).map_err(io::Error::other)?;
                if dev.is_none() {
                    return Ok("User does not exist!".to_string())
                }
            }
            password if password == &*DELETE_PASSWORD => {
                let write_txn = db.begin_write().map_err(io::Error::other)?;
                let removed = {
                    let mut dev_table = write_txn.open_table(DEV_TABLE).map_err(io::Error::other)?;
                    dev_table.remove(&data.dev_name).map_err(io::Error::other)?.is_some()
                    // we dont delete from ttl table since if they get re-added with a later ttl, the generation may get reused 
                    // while the entry is still in the removal queue, removing them early.
                    // leaving them in the ttl table until the normal expiration prevents generation reuse
                };

                let removed = if removed {
                    write_txn.commit().map_err(io::Error::other)?;
                    devs.devs.store(None);
                    format!("Deleted user {}", data.dev_name)
                } else {
                    "User not found".to_string()
                };
                
                return Ok(removed);
            }
            password if password == &*CREATE_PASSWORD => {
                let read_txn = db.begin_read().map_err(io::Error::other)?;
                let table = read_txn.open_table(DEV_TABLE).map_err(io::Error::other)?;
                let dev = table.get(&data.dev_name).map_err(io::Error::other)?;
                if dev.is_some() {
                    return Ok("User already exists".to_string())
                }
            }
            _ => return Ok("Not Authorized!".to_string())
        }
        
        let write_txn = db.begin_write().map_err(io::Error::other)?;
        if let Some(delete_in) = &data.delete_in {
            let mut table = write_txn.open_table(DEV_TTLS).map_err(io::Error::other)?;
            let generation = match table.get(&data.dev_name).map_err(io::Error::other)? {
                Some(entry) => entry.value().generation.wrapping_add(1),
                None => 0,
            };
            
            let delete_at = unix_secs() + *delete_in;
            table.insert(&data.dev_name, GenerationalDeleteTime { generation, delete_at_unix: delete_at }).map_err(io::Error::other)?;
            ttl_tx.send(PendingDev { name: data.dev_name.clone(), delete_at, generation }).map_err(io::Error::other)?;
        }
        
        let player = data.dev();
        {
            let mut table = write_txn.open_table(DEV_TABLE).map_err(io::Error::other)?;
            table.insert(&player.dev_name, &player).map_err(io::Error::other)?;
        }
        
        write_txn.commit().map_err(io::Error::other)?;
        devs.devs.store(None);
        
        Ok::<_, io::Error>(format!("Added user {} with custom_name {}", player.dev_name, player.custom_name))
    }).await.map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))??;
    
    Ok(HttpResponse::Ok().body(res))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateData {
    #[serde(rename = "DevName")]
    dev_name: ArcStr,
    
    #[serde(rename = "Size")]
    size: [f32; 3],
    
    #[serde(rename = "CustomName")]
    custom_name: ArcStr,

    #[serde(rename = "Password")]
    password: String,

    #[serde(rename = "DeleteIn", default, skip_serializing_if = "Option::is_none")]
    delete_in: Option<u64>,
}

impl UpdateData {
    fn dev(self) -> DevPlayer {
        DevPlayer {
            dev_name: self.dev_name,
            size: self.size,
            custom_name: self.custom_name,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DevPlayer {
    #[serde(rename = "DevName")]
    dev_name: ArcStr,
    
    #[serde(rename = "Size")]
    size: [f32; 3],
    
    #[serde(rename = "CustomName")]
    custom_name: ArcStr,
}

impl redb::Value for DevPlayer {
    type SelfType<'a> = DevPlayer;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &[u8]) -> Self::SelfType<'a>{
        postcard::from_bytes(data).expect("Should not fail to deserialize bytes of DevPlayer")
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b
    {
        postcard::to_allocvec(value).expect("Should not fail to serialize DevPlayer to bytes")
    }

    fn type_name() -> redb::TypeName {
        TypeName::new("odindevs::dev_player")
    }
}

#[derive(Debug)]
pub struct GenerationalDeleteTime {
    pub generation: u64,
    pub delete_at_unix: u64,
}

impl redb::Value for GenerationalDeleteTime {
    type SelfType<'a> = GenerationalDeleteTime;
    type AsBytes<'a> = [u8; 16];

    fn fixed_width() -> Option<usize> {
        Some(size_of::<[u8; 16]>())
    }

    fn from_bytes<'a>(data: &[u8]) -> Self::SelfType<'a> {
        let bytes: [u8; 16] = data.try_into().unwrap();
        let compact = u128::from_be_bytes(bytes);
        Self {
            #[allow(clippy::cast_possible_truncation)]
            generation: compact as u64,
            delete_at_unix: (compact >> 64) as u64,
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b
    {
        let compact = u128::from(value.generation) | (u128::from(value.delete_at_unix) << 64);
        compact.to_be_bytes()
    }

    fn type_name() -> redb::TypeName {
        TypeName::new("odindevs::generational_delete_time")
    }
}