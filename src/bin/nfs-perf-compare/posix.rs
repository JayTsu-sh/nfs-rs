use std::ffi::CString;
use std::fs::{File, OpenOptions, Permissions};
use std::io::{Error, ErrorKind};
use std::os::unix::fs::{FileExt, OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nix::libc;
use tokio::task::spawn_blocking;

use super::backend::{Backend, BackendInfo, BenchError, FileHandle, Result};
use super::cli::{CHUNK, IoMode};
use super::pattern::aligned_bytes;

pub struct PosixBackend {
    root: PathBuf,
    io: IoMode,
}

impl PosixBackend {
    pub fn new(root: PathBuf, io: IoMode) -> Self {
        Self { root, io }
    }

    fn abs(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }

    async fn open(&self, path: &str, write: bool) -> Result<Box<dyn FileHandle>> {
        let p = self.abs(path);
        let direct = self.io == IoMode::Direct;
        let file = blocking(move || {
            let mut options = OpenOptions::new();
            if write {
                options.write(true).create(true).truncate(true);
            } else {
                options.read(true);
            }
            if direct {
                options.custom_flags(libc::O_DIRECT);
            }
            options.open(p)
        })
        .await?;
        Ok(Box::new(PosixFile {
            file: Arc::new(file),
            direct,
        }))
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> Result<T> {
    spawn_blocking(f)
        .await
        .map_err(|e| BenchError::Join(e.to_string()))?
        .map_err(BenchError::Io)
}

#[async_trait]
impl Backend for PosixBackend {
    async fn mkdir(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || std::fs::create_dir(p)).await
    }

    async fn create(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(p)
                .map(drop)
        })
        .await
    }

    async fn stat(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || std::fs::metadata(p).map(drop)).await
    }

    async fn access(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || {
            let c = CString::new(p.as_os_str().as_encoded_bytes())
                .map_err(|e| Error::other(e.to_string()))?;
            // SAFETY: c is a valid NUL-terminated path; access(2) has no other preconditions.
            if unsafe { libc::access(c.as_ptr(), libc::R_OK) } == 0 {
                Ok(())
            } else {
                Err(Error::last_os_error())
            }
        })
        .await
    }

    async fn chmod(&self, path: &str, mode: u32) -> Result<()> {
        let p = self.abs(path);
        blocking(move || std::fs::set_permissions(p, Permissions::from_mode(mode))).await
    }

    async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let (f, t) = (self.abs(from), self.abs(to));
        blocking(move || std::fs::rename(f, t)).await
    }

    async fn readdir_count(&self, path: &str) -> Result<usize> {
        let p = self.abs(path);
        blocking(move || Ok(std::fs::read_dir(p)?.count())).await
    }

    async fn remove(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || std::fs::remove_file(p)).await
    }

    async fn rmdir(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || std::fs::remove_dir(p)).await
    }

    async fn open_write(&self, path: &str) -> Result<Box<dyn FileHandle>> {
        self.open(path, true).await
    }

    async fn open_read(&self, path: &str) -> Result<Box<dyn FileHandle>> {
        self.open(path, false).await
    }

    fn chunk_size(&self) -> u64 {
        CHUNK
    }

    fn info(&self) -> BackendInfo {
        BackendInfo {
            backend: "posix",
            protocol: None,
            rsize: CHUNK,
            wsize: CHUNK,
        }
    }

    async fn drop_caches(&self) -> Result<bool> {
        if self.io == IoMode::Direct {
            return Ok(false);
        }
        blocking(|| {
            // SAFETY: sync(2) has no preconditions.
            unsafe { libc::sync() };
            match std::fs::write("/proc/sys/vm/drop_caches", b"3\n") {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == ErrorKind::PermissionDenied => Ok(false),
                Err(e) => Err(e),
            }
        })
        .await
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

struct PosixFile {
    file: Arc<File>,
    direct: bool,
}

#[async_trait]
impl FileHandle for PosixFile {
    async fn write_at(&self, offset: u64, data: Bytes) -> Result<()> {
        let file = Arc::clone(&self.file);
        blocking(move || {
            let mut done = 0usize;
            while done < data.len() {
                let n = file.write_at(&data[done..], offset + done as u64)?;
                if n == 0 {
                    return Err(Error::other("short write"));
                }
                done += n;
            }
            Ok(())
        })
        .await
    }

    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let file = Arc::clone(&self.file);
        let direct = self.direct;
        blocking(move || {
            let (mut v, off) = if direct {
                aligned_bytes(len)
            } else {
                (vec![0u8; len], 0)
            };
            let mut done = 0usize;
            while done < len {
                let n = file.read_at(&mut v[off + done..off + len], offset + done as u64)?;
                if n == 0 {
                    break;
                }
                done += n;
            }
            Ok(Bytes::from(v).slice(off..off + done))
        })
        .await
    }

    async fn sync(&self) -> Result<()> {
        let file = Arc::clone(&self.file);
        blocking(move || file.sync_all()).await
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::{pattern_block, verify};

    #[tokio::test]
    async fn buffered_roundtrip_and_metadata_on_tmpdir() {
        let dir = std::env::temp_dir().join(format!("perfcmp-posix-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let b = PosixBackend::new(dir.clone(), IoMode::Buffered);
        b.mkdir("d").await.unwrap();
        b.create("d/f").await.unwrap();
        b.stat("d/f").await.unwrap();
        b.access("d/f").await.unwrap();
        b.chmod("d/f", 0o644).await.unwrap();
        b.rename("d/f", "d/g").await.unwrap();
        assert_eq!(b.readdir_count("d").await.unwrap(), 1);
        let block = pattern_block();
        let h = b.open_write("d/g").await.unwrap();
        h.write_at(0, block.slice(..8192)).await.unwrap();
        h.sync().await.unwrap();
        h.close().await.unwrap();
        let h = b.open_read("d/g").await.unwrap();
        let got = h.read_at(4096, 4096).await.unwrap();
        assert!(verify(&block, 4096, &got));
        h.close().await.unwrap();
        b.remove("d/g").await.unwrap();
        b.rmdir("d").await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}
