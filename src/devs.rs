use std::{io, sync::{Arc, LazyLock}};

use actix_web::{HttpResponse, Responder, Result, get, http::StatusCode, post, web::{Data, Json}};
use arc_swap::ArcSwapOption;
use redb::{Database, ReadableDatabase, ReadableTable, StorageError, TableDefinition, TypeName};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc::UnboundedSender, task::spawn_blocking};

use crate::{PendingDev, arc_str::ArcStr, env_var};

type DevCache = ArcSwapOption<Vec<DevPlayer>>;

static UPDATE_PASSWORD: LazyLock<String> = LazyLock::new(|| env_var("UPDATE_PASSWORD", "CHANGETHIS_UPDATE".to_owned ()));
static CREATE_PASSWORD: LazyLock<String> = LazyLock::new(|| env_var("CREATE_PASSWORD", "CHANGETHIS_CREATE".to_owned ()));

pub const DEV_TABLE: TableDefinition<ArcStr, DevPlayer> = TableDefinition::new("dev_data");
pub const DEV_TTLS: TableDefinition<ArcStr, u64> = TableDefinition::new("dev_ttls");

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
                    return Ok("Nope".to_string())
                }
            }
            password if password == &*CREATE_PASSWORD => {}
            _ => {
                return Ok("Nope!".to_string());
            }
        }
        
        let write_txn = db.begin_write().map_err(io::Error::other)?;
        if let Some(delete_time) = &data.delete_at_unix {
            let mut table = write_txn.open_table(DEV_TTLS).map_err(io::Error::other)?;
            table.insert(&data.custom_name, delete_time).map_err(io::Error::other)?;
            ttl_tx.send(PendingDev { name: data.custom_name.clone(), delete_at: *delete_time }).map_err(io::Error::other)?;
        }
        
        let player = data.dev();
        {
            let mut table = write_txn.open_table(DEV_TABLE).map_err(io::Error::other)?;
            table.insert(&player.custom_name, &player).map_err(io::Error::other)?;
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

    #[serde(rename = "DeleteAtUnix", default, skip_serializing_if = "Option::is_none")]
    delete_at_unix: Option<u64>,
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