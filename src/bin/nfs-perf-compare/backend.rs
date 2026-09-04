use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BenchError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("nfs-rs error: {0}")]
    Nfs(#[from] nfs_rs::NfsError),
    #[error("data integrity: {0}")]
    Integrity(String),
    #[error("task failed: {0}")]
    Join(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, BenchError>;

#[derive(Debug, Clone)]
pub struct BackendInfo {
    pub backend: &'static str,
    pub protocol: Option<String>,
    pub rsize: u64,
    pub wsize: u64,
}

#[async_trait]
pub trait FileHandle: Send + Sync {
    async fn write_at(&self, offset: u64, data: Bytes) -> Result<()>;
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes>;
    async fn sync(&self) -> Result<()>;
    async fn close(self: Box<Self>) -> Result<()>;
}

/// All paths are relative to the backend root (export root or mount point).
#[async_trait]
pub trait Backend: Send + Sync {
    async fn mkdir(&self, path: &str) -> Result<()>;
    async fn create(&self, path: &str) -> Result<()>;
    async fn stat(&self, path: &str) -> Result<()>;
    async fn access(&self, path: &str) -> Result<()>;
    async fn chmod(&self, path: &str, mode: u32) -> Result<()>;
    async fn rename(&self, from: &str, to: &str) -> Result<()>;
    async fn readdir_count(&self, path: &str) -> Result<usize>;
    async fn remove(&self, path: &str) -> Result<()>;
    async fn rmdir(&self, path: &str) -> Result<()>;
    async fn open_write(&self, path: &str) -> Result<Box<dyn FileHandle>>;
    async fn open_read(&self, path: &str) -> Result<Box<dyn FileHandle>>;
    fn chunk_size(&self) -> u64;
    fn info(&self) -> BackendInfo;
    /// Ok(true) if caches were dropped, Ok(false) if not applicable or not permitted.
    async fn drop_caches(&self) -> Result<bool>;
    async fn shutdown(&self) -> Result<()>;
}
