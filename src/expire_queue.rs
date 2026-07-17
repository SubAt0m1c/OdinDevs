use std::{io, time::Duration};

use actix_web::web::Data;
use redb::{Database, ReadableTable};
use tokio::{sync::mpsc::{UnboundedReceiver, error::TryRecvError}, task::spawn_blocking, time::{Instant, sleep_until}};

use crate::{PendingDev, devs::{CachedDevs, DEV_TABLE, DEV_TTLS}, load_db, unix_secs};


pub async fn run_queue(thread_db: Data<Database>, devs: Data<CachedDevs>, mut rx: UnboundedReceiver<PendingDev>) {
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
                let blocking_devs = devs.clone();
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
}