use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use nfs_rs::{Mount, NFSVersion, OPEN_READ, parse_url_and_mount};

use super::backend::{Backend, BackendInfo, BenchError, FileHandle, Result};
use super::cli::CHUNK;

pub struct NfsRsBackend {
    mount: Arc<Box<dyn Mount>>,
}

impl NfsRsBackend {
    pub async fn connect(url: &str) -> Result<Self> {
        let mount = parse_url_and_mount(url).await?;
        Ok(Self {
            mount: Arc::new(mount),
        })
    }
}

fn protocol_label(version: NFSVersion) -> String {
    match version {
        NFSVersion::NFSv3 => "3".to_string(),
        NFSVersion::NFSv4p0 => "4.0".to_string(),
        NFSVersion::NFSv4p1 => "4.1".to_string(),
        other => format!("{other:?}"),
    }
}

#[async_trait]
impl Backend for NfsRsBackend {
    async fn mkdir(&self, path: &str) -> Result<()> {
        self.mount.mkdir_path(path, 0o755).await?;
        Ok(())
    }

    async fn create(&self, path: &str) -> Result<()> {
        let obj = self.mount.create_path(path, Some(0o644)).await?;
        Ok(self.mount.close(obj.fh).await?)
    }

    async fn stat(&self, path: &str) -> Result<()> {
        self.mount.getattr_path(path).await?;
        Ok(())
    }

    async fn access(&self, path: &str) -> Result<()> {
        self.mount.access_path(path, 4).await?;
        Ok(())
    }

    async fn chmod(&self, path: &str, mode: u32) -> Result<()> {
        Ok(self
            .mount
            .setattr_path(path, false, Some(mode), None, None, None, None, None)
            .await?)
    }

    async fn rename(&self, from: &str, to: &str) -> Result<()> {
        Ok(self.mount.rename_path(from, to).await?)
    }

    async fn readdir_count(&self, path: &str) -> Result<usize> {
        let mut stream = self.mount.readdir_path(path).await?;
        let mut n = 0usize;
        while let Some(entry) = stream.try_next().await? {
            if entry.file_name != "." && entry.file_name != ".." {
                n += 1;
            }
        }
        Ok(n)
    }

    async fn remove(&self, path: &str) -> Result<()> {
        Ok(self.mount.remove_path(path).await?)
    }

    async fn rmdir(&self, path: &str) -> Result<()> {
        Ok(self.mount.rmdir_path(path).await?)
    }

    async fn open_write(&self, path: &str) -> Result<Box<dyn FileHandle>> {
        let obj = self.mount.create_path(path, Some(0o644)).await?;
        Ok(Box::new(NfsFile {
            mount: Arc::clone(&self.mount),
            fh: obj.fh,
        }))
    }

    async fn open_read(&self, path: &str) -> Result<Box<dyn FileHandle>> {
        let obj = self.mount.open_path(path, OPEN_READ).await?;
        Ok(Box::new(NfsFile {
            mount: Arc::clone(&self.mount),
            fh: obj.fh,
        }))
    }

    fn chunk_size(&self) -> u64 {
        let negotiated = self
            .mount
            .get_max_read_size()
            .min(self.mount.get_max_write_size());
        u64::from(negotiated).min(CHUNK)
    }

    fn info(&self) -> BackendInfo {
        BackendInfo {
            backend: "nfsrs",
            protocol: Some(protocol_label(self.mount.version())),
            rsize: u64::from(self.mount.get_max_read_size()),
            wsize: u64::from(self.mount.get_max_write_size()),
        }
    }

    async fn drop_caches(&self) -> Result<bool> {
        Ok(false)
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(self.mount.umount().await?)
    }
}

struct NfsFile {
    mount: Arc<Box<dyn Mount>>,
    fh: Bytes,
}

#[async_trait]
impl FileHandle for NfsFile {
    async fn write_at(&self, offset: u64, data: Bytes) -> Result<()> {
        let mut done = 0usize;
        while done < data.len() {
            let n = self
                .mount
                .write(self.fh.clone(), offset + done as u64, data.slice(done..))
                .await? as usize;
            if n == 0 {
                return Err(BenchError::Other("server accepted zero bytes".into()));
            }
            done += n;
        }
        Ok(())
    }

    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let first = self.mount.read(self.fh.clone(), offset, len as u32).await?;
        if first.len() >= len || first.is_empty() {
            return Ok(first);
        }
        let mut buf = BytesMut::with_capacity(len);
        buf.extend_from_slice(&first);
        while buf.len() < len {
            let part = self
                .mount
                .read(
                    self.fh.clone(),
                    offset + buf.len() as u64,
                    (len - buf.len()) as u32,
                )
                .await?;
            if part.is_empty() {
                break;
            }
            buf.extend_from_slice(&part);
        }
        Ok(buf.freeze())
    }

    async fn sync(&self) -> Result<()> {
        Ok(self.mount.commit(self.fh.clone(), 0, 0).await?)
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(self.mount.close(self.fh).await?)
    }
}
