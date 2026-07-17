use std::{collections::BinaryHeap, env, fmt::{Debug, Display}, io, str::FromStr, time::{Duration, SystemTime, UNIX_EPOCH}};

use actix_web::{App, HttpServer, main, web::Data};
use redb::{Database, ReadableDatabase, ReadableTable, StorageError};
use tokio::{sync::mpsc::{error::TryRecvError, unbounded_channel}, task::spawn_blocking, time::{Instant, sleep_until}};
use uuid::Uuid;

use crate::{devs::{CachedDevs, DEV_TABLE, DEV_TTLS}};

mod arc_str;
mod devs;

#[main]
async fn main() -> io::Result<()> {
    let ip_addr: String = std::env::var("IP_ADDR").unwrap_or("127.0.0.1".to_string());
    println!("Listening on {ip_addr}:3000!");

    let db = Database::create("odindevs.redb").map_err(io::Error::other)?;
    let database = Data::new(db);
    let cached = Data::new(CachedDevs::new());
    
    let thread_db = database.clone();
    let thread_devs = cached.clone();
    let (tx, mut rx) = unbounded_channel::<PendingDev>();
    tokio::spawn(async move {
        let mut pending_devs = load_db(&thread_db).unwrap_or_default();
        
        loop {
            loop {
                match rx.try_recv() {
                    Ok(msg) => pending_devs.push(msg),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => panic!("boom! (shouldnt drop the tx)")
                }
            }
    
            if pending_devs.is_empty() {
                match rx.recv().await {
                    Some(msg) => pending_devs.push(msg),
                    None => panic!("boom! (shouldnt drop the tx)")
                }
                continue;
            }
    
            let next = pending_devs.peek().expect("Should have verified heap is not empty").delete_at;
            let now = unix_secs();

            let mut to_delete: Vec<PendingDev> = Vec::new();
    
            if next <= now {
                while let Some(entry) = pending_devs.peek() {
                    if entry.delete_at > now { break }
    
                    let entry = pending_devs.pop().expect("Should have verified heap.peak() isn't None");
                    to_delete.push(entry);
                }

                if !to_delete.is_empty() {
                    let blocking_devs = thread_devs.clone();
                    let blocking_db = thread_db.clone();
                    spawn_blocking(move || {
                        let write_txn = blocking_db.begin_write().map_err(io::Error::other)?;
    
                        {
                            let mut entry_table = write_txn.open_table(DEV_TABLE).map_err(io::Error::other)?;
                            let mut ttl_table = write_txn.open_table(DEV_TTLS).map_err(io::Error::other)?;
                            
                            
                            for entry in to_delete {
                                {
                                    let Some(ttl) = ttl_table.get(&entry.uuid).map_err(io::Error::other)? else { continue };
                                    if ttl.value().generation != entry.generation { continue }
                                }
    
                                entry_table.remove(&entry.uuid).map_err(io::Error::other)?;
                                ttl_table.remove(&entry.uuid).map_err(io::Error::other)?;
                            }
                        }
    
                        write_txn.commit().map_err(io::Error::other)?;
                        blocking_devs.devs.swap(None);
                        Ok::<(), io::Error>(())
                    });
                }
                
                continue;
            }

            let duration_until_wake = Duration::from_secs(next.saturating_sub(now));
            let sleep = sleep_until(Instant::now() + duration_until_wake);
            
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(msg) => pending_devs.push(msg),
                        None => panic!("boom! (shouldnt drop the tx)")
                    }
                }
                () = sleep => {}
            }
        }
    });

    let reqwest = Data::new(reqwest::Client::new());
    
    let tx = Data::new(tx);
    HttpServer::new(move || {
        App::new()
            .app_data(database.clone())
            .app_data(cached.clone())
            .app_data(tx.clone())
            .app_data(reqwest.clone())
            
            .service(devs::devs_list)
            .service(devs::update_devs)
    })
    .bind((ip_addr, 3000))?
    .run()
    .await
}

/// min heap pending
#[derive(Debug)]
pub struct PendingDev {
    pub uuid: Uuid,
    pub delete_at: u64,
    pub generation: u64,
}

impl PartialOrd for PendingDev {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingDev {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.delete_at.cmp(&self.delete_at)
    }
}

impl Eq for PendingDev {}

impl PartialEq for PendingDev {
    fn eq(&self, other: &Self) -> bool {
        self.delete_at == other.delete_at
    }
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

pub(crate) fn unix_secs() -> u64 {
   SystemTime::now()
       .duration_since(UNIX_EPOCH)
       .unwrap_or(Duration::ZERO)
       .as_secs()
}

fn load_db(db: &Database) -> Result<BinaryHeap<PendingDev>, io::Error> {
    let read_txn = db.begin_read().map_err(io::Error::other)?;
    let read = read_txn.open_table(DEV_TTLS).map_err(io::Error::other)?;
    let iter = read.iter().map_err(io::Error::other)?;

    let items = iter
        .map(|res| {
            let (key, value) = res?;
            let delete_time = value.value();
            Ok::<_, StorageError>(PendingDev { uuid: key.value(), delete_at: delete_time.delete_at_unix, generation: delete_time.generation })
        })
        .collect::<Result<BinaryHeap<_>, _>>()
        .map_err(io::Error::other)?;
    
    Ok::<_, io::Error>(items)
}