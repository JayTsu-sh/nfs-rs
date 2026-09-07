use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use nfs_rs::{BufferedFile, Mount, NFSVersion, OPEN_READ, parse_url_and_mount};

use super::backend::{Backend, BackendInfo, FileHandle, Result};
use super::cli::CHUNK;

pub struct NfsRsBackend {
    mount: Arc<dyn Mount>,
}

impl NfsRsBackend {
    pub async fn connect(url: &str) -> Result<Self> {
        let mount = parse_url_and_mount(url).await?;
        Ok(Self {
            mount: Arc::from(mount),
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
        Ok(Box::new(self.file(obj.fh)))
    }

    async fn open_read(&self, path: &str) -> Result<Box<dyn FileHandle>> {
        let obj = self.mount.open_path(path, OPEN_READ).await?;
        Ok(Box::new(self.file(obj.fh)))
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

impl NfsRsBackend {
    /// Files go through `BufferedFile`, so the mount's `readahead` /
    /// `writeback` URL parameters decide whether I/O is pipelined.
    fn file(&self, fh: Bytes) -> NfsFile {
        let io = BufferedFile::new(Arc::clone(&self.mount), fh.clone(), self.mount.io_options());
        NfsFile {
            mount: Arc::clone(&self.mount),
            fh,
            io,
        }
    }
}

struct NfsFile {
    mount: Arc<dyn Mount>,
    fh: Bytes,
    io: BufferedFile,
}

#[async_trait]
impl FileHandle for NfsFile {
    async fn write_at(&self, offset: u64, data: Bytes) -> Result<()> {
        Ok(self.io.write_at(offset, data).await?)
    }

    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        Ok(self.io.read_at(offset, len as u32).await?)
    }

    async fn sync(&self) -> Result<()> {
        self.io.flush().await?;
        Ok(self.mount.commit(self.fh.clone(), 0, 0).await?)
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(self.mount.close(self.fh).await?)
    }
}
