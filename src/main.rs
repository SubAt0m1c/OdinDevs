use std::{env, fmt::{Debug, Display}, io, str::FromStr};

use actix_web::{App, HttpResponse, HttpServer, http::header::ContentType, main, mime, web::Data};
use redb::Database;

use crate::devs::CachedDevs;

mod devs;

#[main]
async fn main() -> io::Result<()> {
    let db = Database::create("odindevs.redb").map_err(io::Error::other)?;
    
    let ip_addr: String = std::env::var("IP_ADDR").unwrap_or("127.0.0.1".to_string());
    println!("Listening on {ip_addr}:3000!");

    let data = Data::new(db);
    let cached = Data::new(CachedDevs::new());
    
    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .app_data(cached.clone())
            .service(devs::devs_list)
            .service(devs::update_devs)
    })
    .bind((ip_addr, 3000))?
    .run()
    .await
}


/// # Panics
/// panics if the environment variable is not parsable as `T`.
pub fn env_var<T>(key: &'static str, default: T) -> T
where
    T: FromStr + Display,
    T::Err: Debug
{
    match env::var(key) {
        Ok(str) => str.parse::<T>().unwrap_or_else(|e| panic!("{} should be a {}!: {e:?}", key, std::any::type_name::<T>())),
        Err(e) => {
            eprintln!("{e}: {key}, using {default} default.");
            default
        }
    }
}

pub fn json_response(data: Vec<u8>) -> HttpResponse {
    HttpResponse::Ok()
        .append_header(ContentType(mime::APPLICATION_JSON))
        .body(data)
}