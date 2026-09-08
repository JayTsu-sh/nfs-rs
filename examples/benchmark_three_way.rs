//! Persistent JSON-line worker for nfs_rs_compare3.py. Build in release mode.
//! All reported intervals exclude control IPC and content/inventory validation.
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use nfs_rs::{BufferedFile, Mount, OPEN_READ};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    io::{self, BufRead, Write},
    sync::Arc,
    time::Instant,
};

type Error = Box<dyn std::error::Error>;
fn field<'a>(v: &'a Value, key: &str) -> Result<&'a str, Error> {
    v[key]
        .as_str()
        .ok_or_else(|| format!("missing {key}").into())
}
fn kind(value: u32) -> &'static str {
    match value {
        1 => "file",
        2 => "directory",
        3 => "block_device",
        4 => "character_device",
        5 => "symlink",
        6 => "socket",
        7 => "fifo",
        _ => "unknown",
    }
}
// Diagnostic request-size override only; production negotiated limits are unchanged.
// Settle every request before returning an error so scratch cleanup cannot race writes.
async fn write_with_limit(
    mount: &dyn Mount,
    fh: Bytes,
    payload: Bytes,
    chunk: usize,
) -> nfs_rs::Result<Value> {
    if chunk == 0 || chunk > mount.get_max_write_size() as usize {
        return Err(nfs_rs::NfsError::InvalidInput(
            "invalid diagnostic chunk size".into(),
        ));
    }
    let batches = futures::stream::iter((0..payload.len()).step_by(chunk))
        .map(|start| {
            let fh = fh.clone();
            let payload = payload.clone();
            async move {
                let end = (start + chunk).min(payload.len());
                let mut at = start;
                let mut outcomes = Vec::new();
                while at < end {
                    let outcome = mount
                        .write(fh.clone(), at as u64, payload.slice(at..end))
                        .await?;
                    if outcome.count == 0 || outcome.count as usize > end - at {
                        return Err(nfs_rs::NfsError::Rpc("invalid short WRITE count".into()));
                    }
                    at += outcome.count as usize;
                    outcomes.push(outcome);
                }
                Ok(outcomes)
            }
        })
        .buffer_unordered(8)
        .collect::<Vec<nfs_rs::Result<Vec<nfs_rs::WriteOutcome>>>>()
        .await;
    let outcomes = batches
        .into_iter()
        .collect::<nfs_rs::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    mount
        .commit_write_batch(
            fh,
            0,
            payload
                .len()
                .try_into()
                .map_err(|_| nfs_rs::NfsError::InvalidInput("payload too large".into()))?,
            &outcomes,
        )
        .await?;
    Ok(json!({"chunk_bytes":chunk,"write_rpc_count":outcomes.len(),
        "unstable_replies":outcomes.iter().filter(|o|o.committed==nfs_rs::WriteCommitted::Unstable).count(),
        "data_sync_replies":outcomes.iter().filter(|o|o.committed==nfs_rs::WriteCommitted::DataSync).count(),
        "file_sync_replies":outcomes.iter().filter(|o|o.committed==nfs_rs::WriteCommitted::FileSync).count()}))
}

async fn execute(
    mount: &Arc<dyn Mount>,
    request: &Value,
    payload: &mut Bytes,
) -> Result<Value, Error> {
    match field(request, "op")? {
        "load" => {
            *payload = Bytes::from(std::fs::read(field(request, "local")?)?);
            Ok(json!({"bytes":payload.len()}))
        }
        "scan" => {
            let limit = request["max_dirs"].as_u64().ok_or("missing max_dirs")? as usize;
            let start = Instant::now();
            let mut queue = VecDeque::from([(".".to_string(), mount.lookup_path(".").await?.fh)]);
            let mut dirs = 0;
            let mut records = Vec::new();
            while dirs < limit {
                let Some((path, fh)) = queue.pop_front() else {
                    break;
                };
                let mut entries: Vec<_> = mount.readdirplus(fh).await.try_collect().await?;
                entries.retain(|e| e.file_name != "." && e.file_name != "..");
                entries.sort_by(|a, b| a.file_name.cmp(&b.file_name));
                for entry in entries {
                    let attr = entry.attr.ok_or("missing directory attributes")?;
                    let child = if path == "." {
                        entry.file_name
                    } else {
                        format!("{path}/{}", entry.file_name)
                    };
                    if attr.type_ == 2 {
                        let fh = if entry.handle.is_empty() {
                            attr.filehandle
                        } else {
                            entry.handle
                        };
                        if fh.is_empty() {
                            return Err("missing child directory handle".into());
                        }
                        queue.push_back((child.clone(), fh));
                    }
                    records.push((child, kind(attr.type_), attr.fileid, attr.filesize));
                }
                dirs += 1;
            }
            let seconds = start.elapsed().as_secs_f64();
            serde_json::to_writer(std::fs::File::create(field(request, "local")?)?, &records)?;
            Ok(
                json!({"seconds":seconds,"directories":dirs,"entries":records.len(),"truncated":!queue.is_empty()}),
            )
        }
        "write" => {
            let start = Instant::now();
            let file = mount
                .create_path(field(request, "path")?, Some(0o644))
                .await?;
            let result = if let Some(chunk) = request["chunk_bytes"].as_u64() {
                write_with_limit(
                    &**mount,
                    file.fh.clone(),
                    payload.clone(),
                    chunk.try_into()?,
                )
                .await
            } else {
                nfs_rs::write_all(&**mount, file.fh.clone(), 0, payload.clone())
                    .await
                    .map(|_| json!({}))
            };
            let closed = mount.close(file.fh).await;
            let mut result = result?;
            closed?;
            result["seconds"] = json!(start.elapsed().as_secs_f64());
            result["bytes"] = json!(payload.len());
            Ok(result)
        }
        "read" => {
            let start = Instant::now();
            let file = mount.open_path(field(request, "path")?, OPEN_READ).await?;
            let file = BufferedFile::new(mount.clone(), file.fh);
            let data = file.read_at(0, payload.len().try_into()?).await;
            let closed = file.close().await;
            let data = data?;
            closed?;
            let seconds = start.elapsed().as_secs_f64();
            if data != *payload {
                return Err("read content mismatch".into());
            }
            Ok(json!({"seconds":seconds,"bytes":data.len(),"verified":true}))
        }
        _ => Err("unknown operation".into()),
    }
}
#[tokio::main]
async fn main() -> Result<(), Error> {
    let url = std::env::args()
        .nth(1)
        .ok_or("usage: benchmark_three_way NFS_URL")?;
    let mount: Arc<dyn Mount> = Arc::from(nfs_rs::parse_url_and_mount(&url).await?);
    println!(
        "{}",
        json!({"max_read":mount.get_max_read_size(),"max_write":mount.get_max_write_size()})
    );
    io::stdout().flush()?;
    let mut payload = Bytes::new();
    for line in io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line?)?;
        if request["op"] == "quit" {
            break;
        }
        let response = execute(&mount, &request, &mut payload)
            .await
            .unwrap_or_else(|error| json!({"error":error.to_string()}));
        println!("{response}");
        io::stdout().flush()?;
    }
    mount.umount().await?;
    Ok(())
}
