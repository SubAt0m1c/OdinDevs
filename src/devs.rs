use std::{fmt::format, sync::{Arc, LazyLock}};

use actix_web::{HttpResponse, Responder, Result, body, get, http::StatusCode, post, web::{Bytes, Data, Json}};
use arc_swap::ArcSwapOption;
use redb::{Database, ReadableDatabase, ReadableTable, StorageError, TableDefinition, TypeName};
use serde::{Deserialize, Serialize};
use simd_json::deserialize;
use tokio::task::spawn_blocking;

use crate::env_var;

type DevCache = ArcSwapOption<Vec<DevPlayer>>;

static UPDATE_PASSWORD: LazyLock<String> = LazyLock::new(|| env_var("UPDATE_PASSWORD", "CHANGETHIS_UPDATE".to_owned ()));
static CREATE_PASSWORD: LazyLock<String> = LazyLock::new(|| env_var("CREATE_PASSWORD", "CHANGETHIS_CREATE".to_owned ()));

pub const DEV_TABLE: TableDefinition<String, DevPlayer> = TableDefinition::new("dev_data");

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
    let read_txn = db.begin_read().map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))?;
    let table = read_txn.open_table(DEV_TABLE).map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))?;

    #[allow(clippy::single_match_else)]
    let devs = match devs.devs.load().as_ref().cloned() {
        Some(devs) => devs,
        None => {
            let items = table.iter().map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))?;
            let items = items
                .map(|res| {
                    let (_, value) = res?;
                    Ok::<_, StorageError>(value.value())
                })
                .collect::<Result<Vec<_>, _>>().map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))?;
            let items = Arc::new(items);
            
            devs.devs.store(Some(items.clone()));
            items
        }
    };

    Ok(HttpResponse::Ok().json(&*devs))   
}

#[post("/")]
pub async fn update_devs(
    body: Json<UpdateData>,
    db: Data<Database>,
    devs: Data<CachedDevs>,
) -> Result<impl Responder> {
    let data = body.into_inner();
    
    match &data.password {
        password if password == &*UPDATE_PASSWORD => {
            let read_txn = db.begin_read().map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))?;
            let table = read_txn.open_table(DEV_TABLE).map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))?;
            let dev = table.get(&data.dev_name).map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))?;
            if dev.is_none() {
                return Err(actix_web::error::ErrorNotFound("Nope"));
            }
        }
        password if password == &*CREATE_PASSWORD => {}
        _ => {
            return Err(actix_web::error::ErrorForbidden("Nope!"));
        }
    }

    let player = data.dev();
    let write_txn = db.begin_write().map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))?;
    {
        let mut table = write_txn.open_table(DEV_TABLE).map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))?;
        table.insert(&player.custom_name, &player).map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))?;
    }
    write_txn.commit().map_err(|e| actix_web::error::InternalError::new(e, StatusCode::INTERNAL_SERVER_ERROR))?;
    
    devs.devs.store(None);
    Ok(HttpResponse::Ok().json(format!("Added user {} with custom_name {}", player.dev_name, player.custom_name)))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateData {
    #[serde(rename = "DevName")]
    dev_name: String,
    
    #[serde(rename = "Size")]
    size: [f32; 3],
    
    #[serde(rename = "CustomName")]
    custom_name: String,

    #[serde(rename = "Password")]
    password: String,
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
    dev_name: String,
    
    #[serde(rename = "Size")]
    size: [f32; 3],
    
    #[serde(rename = "CustomName")]
    custom_name: String,
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