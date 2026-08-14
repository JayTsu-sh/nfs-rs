// Copyright 2025 NetApp Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use crate::Time;
use crate::error::{NfsError, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use futures::stream::Stream;
use std::pin::Pin;

pub(crate) fn block_on_compat<F: std::future::Future>(f: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(f))
        }
        _ => futures::executor::block_on(f),
    }
}

pub type ReaddirStream<'a> = Pin<Box<dyn Stream<Item = Result<ReaddirEntry>> + Send + 'a>>;

pub type ReaddirplusStream<'a> = Pin<Box<dyn Stream<Item = Result<ReaddirplusEntry>> + Send + 'a>>;

/// Access modes for [`Mount::open`].
pub const OPEN_READ: u32 = 1;
pub const OPEN_WRITE: u32 = 2;
pub const OPEN_BOTH: u32 = 3;

/// Negotiated and currently effective NFSv4.1 fore-channel bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Nfs41ChannelLimits {
    pub max_request_size: u32,
    pub max_response_size: u32,
    pub max_cached_response_size: u32,
    pub max_operations: u32,
    pub max_requests: u32,
    pub effective_highest_slot_id: u32,
}

/// Redacted NFSv4.1 callback lifecycle counters for reliability diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Nfs41CallbackStats {
    pub layout_recalls_received: u64,
    pub layout_returns_completed: u64,
}

/// Protocol-neutral features exposed by a mounted client.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MountCapabilities {
    pub acl: bool,
    pub named_attributes: bool,
    pub locks: bool,
    pub callbacks: bool,
    pub delegation_retention: bool,
    pub pnfs: bool,
    pub session_diagnostics: bool,
}

/// High-level lifecycle state of a mount.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MountLifecycleState {
    #[default]
    Ready,
    Reconnecting,
    Suspect,
    Recovering,
    Reclaiming,
    LostState,
    Closing,
    Closed,
}

/// Protocol-neutral mount health. Stateful engines may override the defaults.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MountHealth {
    pub lifecycle: MountLifecycleState,
    pub generation: u64,
    pub lease_healthy: Option<bool>,
    /// Server-advertised lease duration when the protocol exposes one.
    pub lease_seconds: Option<u32>,
    /// Number of validated background lease renewals in this generation.
    pub lease_renewals: u64,
    pub callback_healthy: Option<bool>,
}

/// Redacted callback counters shared by NFSv4 client engines.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CallbackStats {
    /// Delegations granted by the server and observed by the client.
    pub grants_received: u64,
    pub recalls_received: u64,
    pub returns_completed: u64,
    pub returns_failed: u64,
}

/// An opened object plus protocol-owned state hidden from callers.
#[derive(Debug, PartialEq)]
pub struct OpenFile {
    pub object: ObjRes,
    state: Option<Bytes>,
}

impl OpenFile {
    pub(crate) fn with_protocol_state(object: ObjRes, state: Bytes) -> Self {
        Self {
            object,
            state: Some(state),
        }
    }

    pub(crate) fn into_parts(self) -> (ObjRes, Option<Bytes>) {
        (self.object, self.state)
    }

    pub(crate) fn protocol_state(&self) -> Option<&Bytes> {
        self.state.as_ref()
    }
}

/// A byte-range lock together with everything required to release it safely.
#[derive(Debug, Eq, PartialEq)]
pub struct LockToken {
    pub(crate) fh: Bytes,
    pub(crate) stateid: Bytes,
    pub(crate) lock_type: u32,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) issuer: u64,
    pub(crate) generation: u64,
}

impl LockToken {
    pub(crate) fn new(
        fh: Bytes,
        stateid: Bytes,
        lock_type: u32,
        offset: u64,
        length: u64,
        issuer: u64,
        generation: u64,
    ) -> Self {
        Self {
            fh,
            stateid,
            lock_type,
            offset,
            length,
            issuer,
            generation,
        }
    }
}

/// Trait which defines the procedures that can be performed on an NFS mount.
///
/// NFS version agnostic.  However, since NFSv4 introduces procedures that are not present in NFSv3, invoking those
/// procedures will return an error when relevant [`Mount`] is NFSv3.
///
/// **Stale handles**: If the NFS server reboots, file handles become invalid and operations
/// return `NfsError::Nfs3` with `NFS3ERR_STALE`. Callers should re-mount via
/// [`parse_url_and_mount`](crate::parse_url_and_mount) to obtain fresh handles.
#[async_trait]
pub trait Mount: std::fmt::Debug + Send + Sync {
    /// Return features implemented by this mounted client.
    fn capabilities(&self) -> MountCapabilities {
        MountCapabilities::default()
    }

    /// Return a redacted, protocol-neutral lifecycle snapshot.
    fn health(&self) -> MountHealth {
        MountHealth::default()
    }

    /// Return protocol-neutral callback counters.
    async fn callback_stats(&self) -> CallbackStats {
        let stats = self.nfs41_callback_stats().await.unwrap_or_default();
        CallbackStats {
            grants_received: 0,
            recalls_received: stats.layout_recalls_received,
            returns_completed: stats.layout_returns_completed,
            returns_failed: 0,
        }
    }

    /// Utility function get_max_read_size returns maximum read chunk size.
    ///
    /// # Example
    ///
    /// ```
    /// async fn read_chunk(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes, offset: u64, size: u32) -> nfs_rs::Result<bytes::Bytes> {
    ///     let chunk_size = mount.get_max_read_size().min(size);
    ///     mount.read(fh, offset, chunk_size).await
    /// }
    /// ```
    fn get_max_read_size(&self) -> u32;

    /// Utility function get_max_write_size returns maximum write chunk size.
    ///
    /// # Example
    ///
    /// ```
    /// async fn write_chunk(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes, offset: u64, data: &[u8], size: u32) -> nfs_rs::Result<u32> {
    ///     let chunk_size = mount.get_max_write_size().min(size) as usize;
    ///     let data = data[0..chunk_size].to_vec();
    ///     mount.write(fh, offset, bytes::Bytes::from(data)).await
    /// }
    /// ```
    fn get_max_write_size(&self) -> u32;

    /// Return NFSv4.1 fore-channel limits, or `None` for other protocol versions.
    async fn nfs41_channel_limits(&self) -> Option<Nfs41ChannelLimits> {
        None
    }

    /// Return redacted callback counters, or `None` for other protocol versions.
    async fn nfs41_callback_stats(&self) -> Option<Nfs41CallbackStats> {
        None
    }

    /// Procedure NULL does not do any work. It is made available to allow server response testing and timing.
    ///
    /// # Example
    ///
    /// ```
    /// async fn is_mount_alive(mount: &dyn nfs_rs::Mount) -> nfs_rs::Result<bool> {
    ///     mount.null().await?;
    ///     Ok(true)
    /// }
    /// ```
    async fn null(&self) -> Result<()>;

    /// Procedure ACCESS determines the access rights that a user, as identified by the credentials in the request,
    /// has with respect to a file system object.
    ///
    /// # Example
    ///
    /// ```
    /// async fn check_access(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes, mode: u32) -> nfs_rs::Result<bool> {
    ///     let access = mount.access(fh, mode).await?;
    ///     Ok(mode == access)
    /// }
    /// ```
    async fn access(&self, fh: Bytes, mode: u32) -> Result<u32>;

    /// Same as [`Mount::access`] but instead of taking in a file handle, takes in a path for which file handle is
    /// obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn check_access_for_path(mount: &dyn nfs_rs::Mount, path: &str, mode: u32) -> nfs_rs::Result<bool> {
    ///     let access = mount.access_path(path, mode).await?;
    ///     Ok(mode == access)
    /// }
    /// ```
    async fn access_path(&self, path: &str, mode: u32) -> Result<u32> {
        let res = self.lookup_path(path).await?;
        self.access(res.fh, mode).await
    }

    /// Open a file for read/write access, returning a file handle and attributes.
    ///
    /// For NFSv4.1 this sends OPEN and tracks the stateid internally.
    /// For NFSv3 this is equivalent to [`Mount::lookup`] (stateless protocol).
    ///
    /// `access` is one of [`OPEN_READ`], [`OPEN_WRITE`], or [`OPEN_BOTH`].
    ///
    /// The returned [`ObjRes`] contains the file handle to use with
    /// [`Mount::read`] / [`Mount::write`]. Call [`Mount::close`] when done.
    async fn open(&self, dir_fh: Bytes, filename: &str, _access: u32) -> Result<ObjRes> {
        // Default: stateless lookup (NFSv3)
        self.lookup(dir_fh, filename).await
    }

    /// Same as [`Mount::open`] but takes a full path instead of dir_fh + filename.
    async fn open_path(&self, path: &str, _access: u32) -> Result<ObjRes> {
        // Default: stateless lookup (NFSv3)
        self.lookup_path(path).await
    }

    /// Stateful form of [`Mount::open`]. Future protocol engines can attach
    /// owner state without exposing wire stateids in the public API.
    async fn open_stateful(&self, dir_fh: Bytes, filename: &str, access: u32) -> Result<OpenFile> {
        Ok(OpenFile {
            object: self.open(dir_fh, filename, access).await?,
            state: None,
        })
    }

    /// Stateful form of [`Mount::open_path`].
    async fn open_path_stateful(&self, path: &str, access: u32) -> Result<OpenFile> {
        Ok(OpenFile {
            object: self.open_path(path, access).await?,
            state: None,
        })
    }

    /// Procedure CLOSE releases share reservations for a file (NFSv4).
    /// For NFSv3 this is a no-op since NFSv3 is stateless.
    /// The file handle should have been obtained via [`Mount::open`] or [`Mount::open_path`].
    async fn close(&self, _fh: Bytes) -> Result<()> {
        Ok(()) // Default: no-op for stateless protocols (NFSv3)
    }

    /// Close an object returned by a stateful open operation.
    async fn close_stateful(&self, file: OpenFile) -> Result<()> {
        let _protocol_state = file.state;
        self.close(file.object.fh).await
    }

    /// Procedure COMMIT forces or flushes data to stable storage that was previously written with a WRITE procedure
    /// call with the stable field set to UNSTABLE.
    ///
    /// # Example
    ///
    /// ```
    /// async fn write_and_flush(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes, offset: u64, data: &[u8]) -> nfs_rs::Result<()> {
    ///     let count = mount.write(fh.clone(), offset, bytes::Bytes::copy_from_slice(data)).await?;
    ///     mount.commit(fh, offset, count as u32).await // safe since `write` returns error if data.len() > u32::MAX
    /// }
    /// ```
    async fn commit(&self, fh: Bytes, offset: u64, count: u32) -> Result<()>;

    /// Same as [`Mount::commit`] but instead of taking in a file handle, takes in a path for which file handle is
    /// obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn write_to_path_and_flush(mount: &dyn nfs_rs::Mount, path: &str, offset: u64, data: &[u8]) -> nfs_rs::Result<()> {
    ///     let count = mount.write_path(path, offset, bytes::Bytes::copy_from_slice(data)).await?;
    ///     mount.commit_path(path, offset, count as u32).await // safe since `write_path` returns error if data.len() > u32::MAX
    /// }
    /// ```
    async fn commit_path(&self, path: &str, offset: u64, count: u32) -> Result<()> {
        let res = self.lookup_path(path).await?;
        self.commit(res.fh, offset, count).await
    }

    /// Procedure CREATE creates a regular file.
    ///
    /// # Examples
    ///
    /// ```
    /// async fn create_txt(mount: &dyn nfs_rs::Mount, dir_fh: bytes::Bytes, name: &str) -> nfs_rs::Result<nfs_rs::ObjRes> {
    ///     let mode = 0o640;
    ///     let filename = format!("{}.txt", name);
    ///     mount.create(dir_fh, &filename, Some(mode)).await
    /// }
    ///
    /// async fn create_sh(mount: &dyn nfs_rs::Mount, dir_fh: bytes::Bytes, name: &str) -> nfs_rs::Result<nfs_rs::ObjRes> {
    ///     let mode = 0o750;
    ///     let filename = format!("{}.sh", name);
    ///     mount.create(dir_fh, &filename, Some(mode)).await
    /// }
    ///
    /// async fn create_with_default_mode(mount: &dyn nfs_rs::Mount, dir_fh: bytes::Bytes, name: &str) -> nfs_rs::Result<nfs_rs::ObjRes> {
    ///     let filename = format!("{}.txt", name);
    ///     mount.create(dir_fh, &filename, None).await
    /// }
    /// ```
    async fn create(&self, dir_fh: Bytes, filename: &str, mode: Option<u32>) -> Result<ObjRes>;

    /// Same as [`Mount::create`] but instead of taking in directory file handle and filename, takes in a path for
    /// which directory file handle is obtained by performing one or more LOOKUP procedures.
    ///
    /// # Examples
    ///
    /// ```
    /// async fn create_txt(mount: &dyn nfs_rs::Mount, dir: &str, name: &str) -> nfs_rs::Result<nfs_rs::ObjRes> {
    ///     let mode = 0o640;
    ///     let path = format!("{}/{}.txt", dir, name);
    ///     mount.create_path(&path, Some(mode)).await
    /// }
    ///
    /// async fn create_sh(mount: &dyn nfs_rs::Mount, dir: &str, name: &str) -> nfs_rs::Result<nfs_rs::ObjRes> {
    ///     let mode = 0o750;
    ///     let path = format!("{}/{}.sh", dir, name);
    ///     mount.create_path(&path, Some(mode)).await
    /// }
    ///
    /// async fn create_path_with_default_mode(mount: &dyn nfs_rs::Mount, path: &str) -> nfs_rs::Result<nfs_rs::ObjRes> {
    ///     mount.create_path(path, None).await
    /// }
    /// ```
    async fn create_path(&self, path: &str, mode: Option<u32>) -> Result<ObjRes>;

    /// Procedure DELEGPURGE purges delegations (NFSv4 only; returns Unsupported on NFSv3).
    #[deprecated(note = "delegation lifecycle is managed internally by the mount")]
    async fn delegpurge(&self, _clientid: u64) -> Result<()> {
        Err(NfsError::Unsupported(
            "DELEGPURGE requires NFSv4".to_string(),
        ))
    }

    /// Procedure DELEGRETURN returns a delegation (NFSv4 only; returns Unsupported on NFSv3).
    #[deprecated(note = "delegation lifecycle is managed internally by the mount")]
    async fn delegreturn(&self, _stateid: u64) -> Result<()> {
        Err(NfsError::Unsupported(
            "DELEGRETURN requires NFSv4".to_string(),
        ))
    }

    /// Procedure LOCK acquires a byte-range lock (NFSv4.0/NFSv4.1 only; Unsupported on NFSv3).
    /// lock_type: 1=READ, 2=WRITE. Returns the lock stateid on success.
    async fn lock(&self, _fh: Bytes, _lock_type: u32, _offset: u64, _length: u64) -> Result<Bytes> {
        Err(NfsError::Unsupported(
            "LOCK requires NFSv4.0 or NFSv4.1".to_string(),
        ))
    }

    /// Test whether a byte range can be locked without acquiring it (NFSv4.0/NFSv4.1 LOCKT).
    async fn lock_test(
        &self,
        _fh: Bytes,
        _lock_type: u32,
        _offset: u64,
        _length: u64,
    ) -> Result<()> {
        Err(NfsError::Unsupported(
            "LOCKT requires NFSv4.0 or NFSv4.1".to_string(),
        ))
    }

    /// Procedure LOCKU releases a byte-range lock (NFSv4 only; returns Unsupported on NFSv3).
    async fn locku(
        &self,
        _fh: Bytes,
        _lock_stateid: Bytes,
        _lock_type: u32,
        _offset: u64,
        _length: u64,
    ) -> Result<()> {
        Err(NfsError::Unsupported("LOCKU requires NFSv4".to_string()))
    }

    /// Typed LOCK API that preserves the release parameters in one token.
    async fn lock_stateful(
        &self,
        fh: Bytes,
        lock_type: u32,
        offset: u64,
        length: u64,
    ) -> Result<LockToken> {
        let stateid = self.lock(fh.clone(), lock_type, offset, length).await?;
        Ok(LockToken::new(fh, stateid, lock_type, offset, length, 0, 0))
    }

    /// Acquire a typed lock through a specific independent open-owner.
    async fn lock_open_stateful(
        &self,
        opened: &OpenFile,
        lock_type: u32,
        offset: u64,
        length: u64,
    ) -> Result<LockToken> {
        self.lock_stateful(opened.object.fh.clone(), lock_type, offset, length)
            .await
    }

    /// Release a lock using the opaque token returned by [`Mount::lock_stateful`].
    async fn unlock_stateful(&self, token: LockToken) -> Result<()> {
        self.locku(
            token.fh,
            token.stateid,
            token.lock_type,
            token.offset,
            token.length,
        )
        .await
    }

    /// Retrieve the NFSv4 ACL for a file (NFSv4 only; returns Unsupported on NFSv3).
    async fn getacl(&self, _fh: Bytes) -> Result<Acl> {
        Err(NfsError::Unsupported("GETACL requires NFSv4".to_string()))
    }

    /// Retrieve the NFSv4 ACL by path (NFSv4 only; returns Unsupported on NFSv3).
    async fn getacl_path(&self, path: &str) -> Result<Acl> {
        let res = self.lookup_path(path).await?;
        self.getacl(res.fh).await
    }

    /// Set the NFSv4 ACL on a file (NFSv4 only; returns Unsupported on NFSv3).
    async fn setacl(&self, _fh: Bytes, _acl: &Acl) -> Result<()> {
        Err(NfsError::Unsupported("SETACL requires NFSv4".to_string()))
    }

    /// Set the NFSv4 ACL by path (NFSv4 only; returns Unsupported on NFSv3).
    async fn setacl_path(&self, path: &str, acl: &Acl) -> Result<()> {
        let res = self.lookup_path(path).await?;
        self.setacl(res.fh, acl).await
    }

    /// Query which ACE types the server supports (FATTR4_ACLSUPPORT, NFSv4 only).
    async fn aclsupport(&self, _fh: Bytes) -> Result<AclSupport> {
        Err(NfsError::Unsupported(
            "ACLSUPPORT requires NFSv4".to_string(),
        ))
    }

    /// Get a named attribute (xattr) value (NFSv4 only; returns Unsupported on NFSv3).
    async fn getxattr(&self, _fh: Bytes, _name: &str) -> Result<Bytes> {
        Err(NfsError::Unsupported(
            "Named attributes require NFSv4".to_string(),
        ))
    }

    /// Get a named attribute (xattr) value by path.
    async fn getxattr_path(&self, path: &str, name: &str) -> Result<Bytes> {
        let res = self.lookup_path(path).await?;
        self.getxattr(res.fh, name).await
    }

    /// Set a named attribute (xattr) value (NFSv4 only; returns Unsupported on NFSv3).
    async fn setxattr(&self, _fh: Bytes, _name: &str, _value: Bytes) -> Result<()> {
        Err(NfsError::Unsupported(
            "Named attributes require NFSv4".to_string(),
        ))
    }

    /// Set a named attribute (xattr) value by path.
    async fn setxattr_path(&self, path: &str, name: &str, value: Bytes) -> Result<()> {
        let res = self.lookup_path(path).await?;
        self.setxattr(res.fh, name, value).await
    }

    /// List named attribute (xattr) names (NFSv4 only; returns Unsupported on NFSv3).
    async fn listxattr(&self, _fh: Bytes) -> Result<Vec<String>> {
        Err(NfsError::Unsupported(
            "Named attributes require NFSv4".to_string(),
        ))
    }

    /// List named attribute (xattr) names by path.
    async fn listxattr_path(&self, path: &str) -> Result<Vec<String>> {
        let res = self.lookup_path(path).await?;
        self.listxattr(res.fh).await
    }

    /// Remove a named attribute (xattr) (NFSv4 only; returns Unsupported on NFSv3).
    async fn removexattr(&self, _fh: Bytes, _name: &str) -> Result<()> {
        Err(NfsError::Unsupported(
            "Named attributes require NFSv4".to_string(),
        ))
    }

    /// Remove a named attribute (xattr) by path.
    async fn removexattr_path(&self, path: &str, name: &str) -> Result<()> {
        let res = self.lookup_path(path).await?;
        self.removexattr(res.fh, name).await
    }

    /// Procedure FSINFO retrieves non-volatile file system state information.
    ///
    /// # Example
    ///
    /// ```
    /// async fn get_max_filesize(mount: &dyn nfs_rs::Mount) -> nfs_rs::Result<u64> {
    ///     let info = mount.fsinfo().await?;
    ///     Ok(info.maxfilesize)
    /// }
    /// ```
    async fn fsinfo(&self) -> Result<FSInfo>;

    /// Procedure FSSTAT retrieves volatile file system state information.
    ///
    /// # Example
    ///
    /// ```
    /// async fn has_space_for_files(mount: &dyn nfs_rs::Mount, num_files: u64, num_bytes: u64) -> nfs_rs::Result<bool> {
    ///     let fsstats = mount.fsstat().await?;
    ///     Ok(fsstats.ffiles >= num_files && fsstats.fbytes >= num_bytes)
    /// }
    /// ```
    async fn fsstat(&self) -> Result<FSStat>;

    /// Procedure GETATTR retrieves the attributes for a specified file system object.
    ///
    /// # Example
    ///
    /// ```
    /// async fn has_changed_since(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes, time: &nfs_rs::Time) -> nfs_rs::Result<bool> {
    ///     let attr = mount.getattr(fh).await?;
    ///     Ok(attr.mtime.seconds != time.seconds || attr.mtime.nseconds != time.nseconds)
    /// }
    /// ```
    async fn getattr(&self, fh: Bytes) -> Result<Attr>;

    /// Same as [`Mount::getattr`] but instead of taking in a file handle, takes in a path for which file handle is
    /// obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn has_path_changed_since(mount: &dyn nfs_rs::Mount, path: &str, time: &nfs_rs::Time) -> nfs_rs::Result<bool> {
    ///     let attr = mount.getattr_path(path).await?;
    ///     Ok(attr.mtime.seconds != time.seconds || attr.mtime.nseconds != time.nseconds)
    /// }
    /// ```
    async fn getattr_path(&self, path: &str) -> Result<Attr> {
        let res = self.lookup_path(path).await?;
        self.getattr(res.fh).await
    }

    /// Procedure SETATTR changes one or more of the attributes of a file system object on the server.
    ///
    /// # Examples
    ///
    /// ```
    /// async fn chmod(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes, mode: u32, guard: Option<nfs_rs::Time>) -> nfs_rs::Result<()> {
    ///     mount.setattr(fh, guard, Some(mode), None, None, None, None, None).await
    /// }
    ///
    /// async fn chown(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes, uid: u32, gid: u32, guard: Option<nfs_rs::Time>) -> nfs_rs::Result<()> {
    ///     mount.setattr(fh, guard, None, Some(uid), Some(gid), None, None, None).await
    /// }
    ///
    /// async fn truncate(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes, size: u64, guard: Option<nfs_rs::Time>) -> nfs_rs::Result<()> {
    ///     mount.setattr(fh, guard, None, None, None, Some(size), None, None).await
    /// }
    ///
    /// async fn touch(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes, now: nfs_rs::Time, guard: Option<nfs_rs::Time>) -> nfs_rs::Result<()> {
    ///     let now_clone = now.clone();
    ///     mount.setattr(fh, guard, None, None, None, None, Some(now), Some(now_clone)).await
    /// }
    /// ```
    #[allow(clippy::too_many_arguments)]
    async fn setattr(
        &self,
        fh: Bytes,
        guard_ctime: Option<Time>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Time>,
        mtime: Option<Time>,
    ) -> Result<()>;

    /// Same as [`Mount::setattr`] but instead of taking in a file handle, takes in a path for which file handle is
    /// obtained by performing one or more LOOKUP procedures.  Also, instead of taking in optional guard ctime, takes
    /// in a boolean which determines whether to specify guard in SETATTR procedure or not, using ctime from LOOKUP.
    ///
    /// # Examples
    ///
    /// ```
    /// async fn chmod_path(mount: &dyn nfs_rs::Mount, path: &str, mode: u32, guard: bool) -> nfs_rs::Result<()> {
    ///     mount.setattr_path(path, guard, Some(mode), None, None, None, None, None).await
    /// }
    ///
    /// async fn chown_path(mount: &dyn nfs_rs::Mount, path: &str, uid: u32, gid: u32, guard: bool) -> nfs_rs::Result<()> {
    ///     mount.setattr_path(path, guard, None, Some(uid), Some(gid), None, None, None).await
    /// }
    ///
    /// async fn truncate_path(mount: &dyn nfs_rs::Mount, path: &str, size: u64, guard: bool) -> nfs_rs::Result<()> {
    ///     mount.setattr_path(path, guard, None, None, None, Some(size), None, None).await
    /// }
    ///
    /// async fn touch_path(mount: &dyn nfs_rs::Mount, path: &str, now: nfs_rs::Time, guard: bool) -> nfs_rs::Result<()> {
    ///     let now_clone = now.clone();
    ///     mount.setattr_path(path, guard, None, None, None, None, Some(now), Some(now_clone)).await
    /// }
    /// ```
    #[allow(clippy::too_many_arguments)]
    async fn setattr_path(
        &self,
        path: &str,
        specify_guard: bool,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Time>,
        mtime: Option<Time>,
    ) -> Result<()>;

    /// Procedure GETFH returns the current filehandle value.
    async fn getfh(&self) -> Bytes;

    /// Procedure LINK creates a hard link.
    ///
    /// # Example
    ///
    /// ```
    /// async fn create_link(mount: &dyn nfs_rs::Mount, src_fh: bytes::Bytes, dst_dir_fh: bytes::Bytes, dst_filename: &str) -> nfs_rs::Result<nfs_rs::Attr> {
    ///     mount.link(src_fh, dst_dir_fh, dst_filename).await
    /// }
    /// ```
    async fn link(&self, src_fh: Bytes, dst_dir_fh: Bytes, dst_filename: &str) -> Result<Attr>;

    /// Same as [`Mount::link`] but instead of taking in a source file handle, destination directory file handle,
    /// and destination filename, takes in a source path for which file handle is obtained by performing one or more
    /// LOOKUP procedures and destination path for which directory file handle is obtained by performing one or more
    /// LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn create_link_path(mount: &dyn nfs_rs::Mount, src_path: &str, dst_path: &str) -> nfs_rs::Result<nfs_rs::Attr> {
    ///     mount.link_path(src_path, dst_path).await
    /// }
    /// ```
    async fn link_path(&self, src_path: &str, dst_path: &str) -> Result<Attr>;

    /// Procedure SYMLINK creates a new symbolic link.
    ///
    /// # Example
    ///
    /// ```
    /// async fn create_symlink(mount: &dyn nfs_rs::Mount, src_path: &str, dst_dir_fh: bytes::Bytes, dst_filename: &str) -> nfs_rs::Result<nfs_rs::ObjRes> {
    ///     mount.symlink(src_path, dst_dir_fh, dst_filename).await
    /// }
    /// ```
    async fn symlink(
        &self,
        src_path: &str,
        dst_dir_fh: Bytes,
        dst_filename: &str,
    ) -> Result<ObjRes>;

    /// Same as [`Mount::symlink`] but instead of taking in a destination directory file handle and destination
    /// filename, takes in a  destination path for which directory file handle is obtained by performing one or more
    /// LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn create_symlink(mount: &dyn nfs_rs::Mount, src_path: &str, dst_path: &str) -> nfs_rs::Result<nfs_rs::ObjRes> {
    ///     mount.symlink_path(src_path, dst_path).await
    /// }
    /// ```
    async fn symlink_path(&self, src_path: &str, dst_path: &str) -> Result<ObjRes>;

    /// Symbolic link creation with ownership / timestamps applied in the same
    /// network round trip when the backend supports it.
    ///
    /// Behaves like [`Mount::symlink`] when all attribute arguments are `None`.
    /// When any of `uid` / `gid` / `atime` / `mtime` is `Some`, implementations
    /// SHOULD bundle a SETATTR into the same NFSv4 COMPOUND, saving one RPC
    /// vs. the explicit CREATE-then-SETATTR pattern. The default implementation
    /// preserves the old behavior (two RPCs) so backends that cannot merge
    /// work without modification.
    ///
    /// On success, the symlink exists with the requested attributes applied.
    /// If the server rejects in-compound SETATTR, the implementation is
    /// expected to fall back to a separate SETATTR transparently.
    #[allow(clippy::too_many_arguments)]
    async fn symlink_with_attrs(
        &self,
        src_path: &str,
        dst_dir_fh: Bytes,
        dst_filename: &str,
        uid: Option<u32>,
        gid: Option<u32>,
        atime: Option<Time>,
        mtime: Option<Time>,
    ) -> Result<ObjRes> {
        let obj = self.symlink(src_path, dst_dir_fh, dst_filename).await?;
        if uid.is_some() || gid.is_some() || atime.is_some() || mtime.is_some() {
            self.setattr(obj.fh.clone(), None, None, uid, gid, None, atime, mtime)
                .await?;
        }
        Ok(obj)
    }

    /// Procedure READLINK reads the data associated with a symbolic link.
    ///
    /// # Example
    ///
    /// ```
    /// async fn get_symlink_src_path(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes) -> nfs_rs::Result<String> {
    ///     mount.readlink(fh).await
    /// }
    /// ```
    async fn readlink(&self, fh: Bytes) -> Result<String>;

    /// Same as [`Mount::readlink`] but instead of taking in a file handle, takes in a path for which file handle is
    /// obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn get_symlink_src_path_for_path(mount: &dyn nfs_rs::Mount, path: &str) -> nfs_rs::Result<String> {
    ///     mount.readlink_path(path).await
    /// }
    /// ```
    async fn readlink_path(&self, path: &str) -> Result<String> {
        let res = self.lookup_path(path).await?;
        self.readlink(res.fh).await
    }

    /// Procedure LOOKUP searches a directory for a specific name and returns the file handle and attributes for the
    /// corresponding file system object.
    ///
    /// # Example
    ///
    /// ```
    /// async fn is_directory(mount: &dyn nfs_rs::Mount, dir_fh: bytes::Bytes, filename: &str) -> nfs_rs::Result<bool> {
    ///     const TYPE_DIR: u32 = 2;
    ///     let res = mount.lookup(dir_fh, filename).await?;
    ///     Ok(res.attr.map_or(false, |attr| attr.type_ == TYPE_DIR))
    /// }
    /// ```
    async fn lookup(&self, dir_fh: Bytes, filename: &str) -> Result<ObjRes>;

    /// Same as [`Mount::lookup`] but instead of taking in a directory file handle and filename, takes in a path for
    /// which directory file handle is obtained by performing one or more LOOKUP procedures for each directory in the
    /// path, in turn.
    ///
    /// # Example
    ///
    /// ```
    /// async fn is_path_directory(mount: &dyn nfs_rs::Mount, path: &str) -> nfs_rs::Result<bool> {
    ///     const TYPE_DIR: u32 = 2;
    ///     let res = mount.lookup_path(path).await?;
    ///     Ok(res.attr.map_or(false, |attr| attr.type_ == TYPE_DIR))
    /// }
    /// ```
    async fn lookup_path(&self, path: &str) -> Result<ObjRes>;

    /// Procedure PATHCONF retrieves the pathconf information for a file or directory.
    ///
    /// # Example
    ///
    /// ```
    /// async fn chown_allowed(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes) -> nfs_rs::Result<bool> {
    ///     let conf = mount.pathconf(fh).await?;
    ///     Ok(!conf.chown_restricted)
    /// }
    /// ```
    async fn pathconf(&self, fh: Bytes) -> Result<Pathconf>;

    /// Same as [`Mount::pathconf`] but instead of taking in a file handle, takes in a path for which file handle is
    /// obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn chown_allowed_for_path(mount: &dyn nfs_rs::Mount, path: &str) -> nfs_rs::Result<bool> {
    ///     let conf = mount.pathconf_path(path).await?;
    ///     Ok(!conf.chown_restricted)
    /// }
    /// ```
    async fn pathconf_path(&self, path: &str) -> Result<Pathconf> {
        let res = self.lookup_path(path).await?;
        self.pathconf(res.fh).await
    }

    /// Procedure READ reads data from a file.
    ///
    /// # Example
    ///
    /// ```
    /// async fn read_exact(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes, mut offset: u64, mut count: u32) -> nfs_rs::Result<bytes::Bytes> {
    ///     let mut ret = Vec::new();
    ///     loop {
    ///         let chunk = mount.read(fh.clone(), offset, count).await?;
    ///         let chunk_len = chunk.len();
    ///         count -= chunk_len as u32;
    ///         ret.extend_from_slice(&chunk);
    ///         if count == 0 {
    ///             return Ok(bytes::Bytes::from(ret));
    ///         }
    ///         offset += chunk_len as u64;
    ///     }
    /// }
    /// ```
    async fn read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes>;

    /// Same as [`Mount::read`] but instead of taking in a file handle, takes in a path for which file handle is
    /// obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn read_exact_from_path(mount: &dyn nfs_rs::Mount, path: &str, mut offset: u64, mut count: u32) -> nfs_rs::Result<bytes::Bytes> {
    ///     let mut ret = Vec::new();
    ///     loop {
    ///         let chunk = mount.read_path(path, offset, count).await?;
    ///         let chunk_len = chunk.len();
    ///         count -= chunk_len as u32;
    ///         ret.extend_from_slice(&chunk);
    ///         if count == 0 {
    ///             return Ok(bytes::Bytes::from(ret));
    ///         }
    ///         offset += chunk_len as u64;
    ///     }
    /// }
    /// ```
    async fn read_path(&self, path: &str, offset: u64, count: u32) -> Result<Bytes> {
        let res = self.lookup_path(path).await?;
        self.read(res.fh, offset, count).await
    }

    /// Procedure WRITE writes data to a file.
    ///
    /// # Example
    ///
    /// ```
    /// async fn write_all(mount: &dyn nfs_rs::Mount, fh: bytes::Bytes, mut offset: u64, data: bytes::Bytes) -> nfs_rs::Result<()> {
    ///     let mut remaining = data;
    ///     loop {
    ///         if remaining.is_empty() {
    ///             return Ok(());
    ///         }
    ///         let chunk_size = mount.get_max_write_size() as usize;
    ///         let chunk = if remaining.len() > chunk_size {
    ///             remaining.split_to(chunk_size)
    ///         } else {
    ///             let chunk = remaining;
    ///             remaining = bytes::Bytes::new();
    ///             chunk
    ///         };
    ///         let written_bytes = mount.write(fh.clone(), offset, chunk).await? as usize;
    ///         offset += written_bytes as u64;
    ///     }
    /// }
    /// ```
    async fn write(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<u32>;

    /// Same as [`Mount::write`] but instead of taking in a file handle, takes in a path for which file handle is
    /// obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn write_all_to_path(mount: &dyn nfs_rs::Mount, path: &str, mut offset: u64, data: bytes::Bytes) -> nfs_rs::Result<()> {
    ///     let mut remaining = data;
    ///     loop {
    ///         if remaining.is_empty() {
    ///             return Ok(());
    ///         }
    ///         let chunk_size = mount.get_max_write_size() as usize;
    ///         let chunk = if remaining.len() > chunk_size {
    ///             remaining.split_to(chunk_size)
    ///         } else {
    ///             let chunk = remaining;
    ///             remaining = bytes::Bytes::new();
    ///             chunk
    ///         };
    ///         let written_bytes = mount.write_path(path, offset, chunk).await? as usize;
    ///         offset += written_bytes as u64;
    ///     }
    /// }
    /// ```
    async fn write_path(&self, path: &str, offset: u64, data: Bytes) -> Result<u32> {
        let res = self.lookup_path(path).await?;
        self.write(res.fh, offset, data).await
    }

    /// Procedure READDIR retrieves a variable number of entries, in sequence, from a directory and returns the name
    /// and file identifier for each, with information to allow the client to request additional directory entries in
    /// a subsequent READDIR request.
    ///
    /// # Example
    ///
    /// ```
    /// use futures::TryStreamExt;
    /// async fn list_entries(mount: &dyn nfs_rs::Mount, dir_fh: bytes::Bytes) -> nfs_rs::Result<()> {
    ///     let mut stream = mount.readdir(dir_fh).await;
    ///     while let Some(entry) = stream.try_next().await? {
    ///         println!("{}", entry.file_name);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    async fn readdir(&self, dir_fh: Bytes) -> ReaddirStream<'_>;

    /// Same as [`Mount::readdir`] but instead of taking in a directory file handle, takes in a path for which
    /// directory file handle is obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// use futures::TryStreamExt;
    /// async fn list_entries_in_path(mount: &dyn nfs_rs::Mount, dir_path: &str) -> nfs_rs::Result<()> {
    ///     let mut stream = mount.readdir_path(dir_path).await?;
    ///     while let Some(entry) = stream.try_next().await? {
    ///         println!("{}", entry.file_name);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    async fn readdir_path(&self, dir_path: &str) -> Result<ReaddirStream<'_>> {
        let res = self.lookup_path(dir_path).await?;
        Ok(self.readdir(res.fh).await)
    }

    /// Procedure READDIRPLUS retrieves a variable number of entries from a file system directory and returns complete
    /// information about each along with information to allow the client to request additional directory entries in a
    /// subsequent READDIRPLUS.  READDIRPLUS differs from READDIR only in the amount of information returned for each
    /// entry.  In READDIR, each entry returns the filename and the fileid.  In READDIRPLUS, each entry returns the
    /// name, the fileid, attributes (including the fileid), and file handle.
    ///
    /// Returns a [`Stream`] of [`ReaddirplusEntry`] items, yielding entries lazily page by page.
    ///
    /// # Example
    ///
    /// ```
    /// use futures::TryStreamExt;
    /// async fn list_detailed_entries(mount: &dyn nfs_rs::Mount, dir_fh: bytes::Bytes) -> nfs_rs::Result<()> {
    ///     println!("{:>5} {:>25} {}", "mode", "size", "name");
    ///     let mut stream = mount.readdirplus(dir_fh).await;
    ///     while let Some(entry) = stream.try_next().await? {
    ///         let (mode, size) = entry.attr.map_or((String::new(), String::new()), |attr| {
    ///             (
    ///                 format!("{:o}", attr.file_mode),
    ///                 format!("{}", attr.filesize),
    ///             )
    ///         });
    ///         println!("{:>5} {:>25} {}", mode, size, entry.file_name);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    async fn readdirplus(&self, dir_fh: Bytes) -> ReaddirplusStream<'_>;

    /// Same as [`Mount::readdirplus`] but instead of taking in a directory file handle, takes in a path for which
    /// directory file handle is obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// use futures::TryStreamExt;
    /// async fn list_detailed_entries_in_path(mount: &dyn nfs_rs::Mount, dir_path: &str) -> nfs_rs::Result<()> {
    ///     println!("{:>5} {:>25} {}", "mode", "size", "name");
    ///     let mut stream = mount.readdirplus_path(dir_path).await?;
    ///     while let Some(entry) = stream.try_next().await? {
    ///         let (mode, size) = entry.attr.map_or((String::new(), String::new()), |attr| {
    ///             (
    ///                 format!("{:o}", attr.file_mode),
    ///                 format!("{}", attr.filesize),
    ///             )
    ///         });
    ///         println!("{:>5} {:>25} {}", mode, size, entry.file_name);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    async fn readdirplus_path(&self, dir_path: &str) -> Result<ReaddirplusStream<'_>> {
        let res = self.lookup_path(dir_path).await?;
        Ok(self.readdirplus(res.fh).await)
    }

    /// Procedure MKDIR creates a new subdirectory.
    ///
    /// # Example
    ///
    /// ```
    /// async fn ensure_directory_exists(mount: &dyn nfs_rs::Mount, dir_fh: bytes::Bytes, dirname: &str) -> nfs_rs::Result<()> {
    ///     let mode = 0o750;
    ///     let res = mount.mkdir(dir_fh, dirname, mode).await;
    ///     if res.is_err() {
    ///         let err = res.unwrap_err();
    ///         if err.to_string() != "file exists" {
    ///             return Err(err);
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    async fn mkdir(&self, dir_fh: Bytes, dirname: &str, mode: u32) -> Result<ObjRes>;

    /// Same as [`Mount::mkdir`] but instead of taking in directory file handle and dirname, takes in a path for which
    /// directory file handle is obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn ensure_directory_exists_for_path(mount: &dyn nfs_rs::Mount, path: &str) -> nfs_rs::Result<()> {
    ///     let mode = 0o750;
    ///     let res = mount.mkdir_path(path, mode).await;
    ///     if res.is_err() {
    ///         let err = res.unwrap_err();
    ///         if err.to_string() != "file exists" {
    ///             return Err(err);
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    async fn mkdir_path(&self, path: &str, mode: u32) -> Result<ObjRes>;

    /// Procedure REMOVE removes (deletes) an entry from a directory.
    ///
    /// # Example
    ///
    /// ```
    /// async fn delete_file(mount: &dyn nfs_rs::Mount, dir_fh: bytes::Bytes, filename: &str) -> nfs_rs::Result<()> {
    ///     mount.remove(dir_fh, filename).await
    /// }
    /// ```
    async fn remove(&self, dir_fh: Bytes, filename: &str) -> Result<()>;

    /// Same as [`Mount::remove`] but instead of taking in a directory file handle and filename, takes in a path for
    /// which directory file handle is obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn delete_file_path(mount: &dyn nfs_rs::Mount, path: &str) -> nfs_rs::Result<()> {
    ///     mount.remove_path(path).await
    /// }
    /// ```
    async fn remove_path(&self, path: &str) -> Result<()>;

    /// Procedure RMDIR removes (deletes) a subdirectory from a directory.
    ///
    /// # Example
    ///
    /// ```
    /// async fn delete_directory(mount: &dyn nfs_rs::Mount, dir_fh: bytes::Bytes, dirname: &str) -> nfs_rs::Result<()> {
    ///     mount.rmdir(dir_fh, dirname).await
    /// }
    /// ```
    async fn rmdir(&self, dir_fh: Bytes, dirname: &str) -> Result<()>;

    /// Same as [`Mount::rmdir`] but instead of taking in a directory file handle and directory name, takes in a path
    /// for which directory file handle is obtained by performing one or more LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn delete_directory_path(mount: &dyn nfs_rs::Mount, path: &str) -> nfs_rs::Result<()> {
    ///     mount.rmdir_path(path).await
    /// }
    /// ```
    async fn rmdir_path(&self, path: &str) -> Result<()>;

    // Procedure RENAME renames an entry.
    ///
    /// # Example
    ///
    /// ```
    /// async fn mv(
    ///     mount: &dyn nfs_rs::Mount,
    ///     from_dir_fh: bytes::Bytes,
    ///     from_filename: &str,
    ///     to_dir_fh: bytes::Bytes,
    ///     to_filename: &str,
    /// ) -> nfs_rs::Result<()> {
    ///     mount.rename(from_dir_fh, from_filename, to_dir_fh, to_filename).await
    /// }
    /// ```
    async fn rename(
        &self,
        from_dir_fh: Bytes,
        from_filename: &str,
        to_dir_fh: Bytes,
        to_filename: &str,
    ) -> Result<()>;

    /// Same as [`Mount::rename`] but instead of taking in a from directory file handle, from filename, to directory
    /// file handle, and to filename, takes in a from path for which directory file handle is obtained by performing
    /// one or more LOOKUP procedures and to path for which directory file handle is obtained by performing one or more
    /// LOOKUP procedures.
    ///
    /// # Example
    ///
    /// ```
    /// async fn mv_path(mount: &dyn nfs_rs::Mount, from_path: &str, to_path: &str) -> nfs_rs::Result<()> {
    ///     mount.rename_path(from_path, to_path).await
    /// }
    /// ```
    async fn rename_path(&self, from_path: &str, to_path: &str) -> Result<()>;

    /// Procedure UMOUNT unmounts the mount itself.
    ///
    /// # Example
    ///
    /// ```
    /// async fn unmount(mount: &dyn nfs_rs::Mount) -> nfs_rs::Result<()> {
    ///     mount.umount().await
    /// }
    /// ```
    async fn umount(&self) -> Result<()>;

    /// Return NFS version
    ///
    /// # Example
    ///
    /// ```
    /// fn list_unsupported_procedures(mount: &dyn nfs_rs::Mount) -> Option<Vec<String>> {
    ///     match mount.version() {
    ///         nfs_rs::NFSVersion::NFSv3 => Some(vec![
    ///             String::from("close"),
    ///             String::from("delegpurge"),
    ///             String::from("delegreturn"),
    ///             String::from("getfh"),
    ///         ]),
    ///         _ => None,
    ///     }
    /// }
    /// ```
    fn version(&self) -> NFSVersion;

    /// Blocking version of [`Mount::null`]
    fn sync_null(&self) -> Result<()> {
        block_on_compat(self.null())
    }

    /// Blocking version of [`Mount::access`]
    fn sync_access(&self, fh: Bytes, mode: u32) -> Result<u32> {
        block_on_compat(self.access(fh, mode))
    }

    /// Blocking version of [`Mount::access_path`]
    fn sync_access_path(&self, path: &str, mode: u32) -> Result<u32> {
        block_on_compat(self.access_path(path, mode))
    }

    /// Blocking version of [`Mount::open`]
    fn sync_open(&self, dir_fh: Bytes, filename: &str, access: u32) -> Result<ObjRes> {
        block_on_compat(self.open(dir_fh, filename, access))
    }

    /// Blocking version of [`Mount::open_path`]
    fn sync_open_path(&self, path: &str, access: u32) -> Result<ObjRes> {
        block_on_compat(self.open_path(path, access))
    }

    /// Blocking version of [`Mount::close`]
    fn sync_close(&self, fh: Bytes) -> Result<()> {
        block_on_compat(self.close(fh))
    }

    /// Blocking version of [`Mount::commit`]
    fn sync_commit(&self, fh: Bytes, offset: u64, count: u32) -> Result<()> {
        block_on_compat(self.commit(fh, offset, count))
    }

    /// Blocking version of [`Mount::commit_path`]
    fn sync_commit_path(&self, path: &str, offset: u64, count: u32) -> Result<()> {
        block_on_compat(self.commit_path(path, offset, count))
    }

    /// Blocking version of [`Mount::create`]
    fn sync_create(&self, dir_fh: Bytes, filename: &str, mode: Option<u32>) -> Result<ObjRes> {
        block_on_compat(self.create(dir_fh, filename, mode))
    }

    /// Blocking version of [`Mount::create_path`]
    fn sync_create_path(&self, path: &str, mode: Option<u32>) -> Result<ObjRes> {
        block_on_compat(self.create_path(path, mode))
    }

    /// Blocking version of [`Mount::delegpurge`]
    #[allow(deprecated)]
    fn sync_delegpurge(&self, clientid: u64) -> Result<()> {
        block_on_compat(self.delegpurge(clientid))
    }

    /// Blocking version of [`Mount::delegreturn`]
    #[allow(deprecated)]
    fn sync_delegreturn(&self, stateid: u64) -> Result<()> {
        block_on_compat(self.delegreturn(stateid))
    }

    /// Blocking version of [`Mount::fsinfo`]
    fn sync_fsinfo(&self) -> Result<FSInfo> {
        block_on_compat(self.fsinfo())
    }

    /// Blocking version of [`Mount::fsstat`]
    fn sync_fsstat(&self) -> Result<FSStat> {
        block_on_compat(self.fsstat())
    }

    /// Blocking version of [`Mount::getattr`]
    fn sync_getattr(&self, fh: Bytes) -> Result<Attr> {
        block_on_compat(self.getattr(fh))
    }

    /// Blocking version of [`Mount::getattr_path`]
    fn sync_getattr_path(&self, path: &str) -> Result<Attr> {
        block_on_compat(self.getattr_path(path))
    }

    /// Blocking version of [`Mount::setattr`]
    #[allow(clippy::too_many_arguments)]
    fn sync_setattr(
        &self,
        fh: Bytes,
        guard_ctime: Option<Time>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Time>,
        mtime: Option<Time>,
    ) -> Result<()> {
        block_on_compat(self.setattr(fh, guard_ctime, mode, uid, gid, size, atime, mtime))
    }

    /// Blocking version of [`Mount::setattr_path`]
    #[allow(clippy::too_many_arguments)]
    fn sync_setattr_path(
        &self,
        path: &str,
        specify_guard: bool,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Time>,
        mtime: Option<Time>,
    ) -> Result<()> {
        block_on_compat(self.setattr_path(path, specify_guard, mode, uid, gid, size, atime, mtime))
    }

    /// Blocking version of [`Mount::getfh`]
    fn sync_getfh(&self) -> Bytes {
        block_on_compat(self.getfh())
    }

    /// Blocking version of [`Mount::link`]
    fn sync_link(&self, src_fh: Bytes, dst_dir_fh: Bytes, dst_filename: &str) -> Result<Attr> {
        block_on_compat(self.link(src_fh, dst_dir_fh, dst_filename))
    }

    /// Blocking version of [`Mount::link_path`]
    fn sync_link_path(&self, src_path: &str, dst_path: &str) -> Result<Attr> {
        block_on_compat(self.link_path(src_path, dst_path))
    }

    /// Blocking version of [`Mount::symlink`]
    fn sync_symlink(
        &self,
        src_path: &str,
        dst_dir_fh: Bytes,
        dst_filename: &str,
    ) -> Result<ObjRes> {
        block_on_compat(self.symlink(src_path, dst_dir_fh, dst_filename))
    }

    /// Blocking version of [`Mount::symlink_path`]
    fn sync_symlink_path(&self, src_path: &str, dst_path: &str) -> Result<ObjRes> {
        block_on_compat(self.symlink_path(src_path, dst_path))
    }

    /// Blocking version of [`Mount::readlink`]
    fn sync_readlink(&self, fh: Bytes) -> Result<String> {
        block_on_compat(self.readlink(fh))
    }

    /// Blocking version of [`Mount::readlink_path`]
    fn sync_readlink_path(&self, path: &str) -> Result<String> {
        block_on_compat(self.readlink_path(path))
    }

    /// Blocking version of [`Mount::lookup`]
    fn sync_lookup(&self, dir_fh: Bytes, filename: &str) -> Result<ObjRes> {
        block_on_compat(self.lookup(dir_fh, filename))
    }

    /// Blocking version of [`Mount::lookup_path`]
    fn sync_lookup_path(&self, path: &str) -> Result<ObjRes> {
        block_on_compat(self.lookup_path(path))
    }

    /// Blocking version of [`Mount::pathconf`]
    fn sync_pathconf(&self, fh: Bytes) -> Result<Pathconf> {
        block_on_compat(self.pathconf(fh))
    }

    /// Blocking version of [`Mount::pathconf_path`]
    fn sync_pathconf_path(&self, path: &str) -> Result<Pathconf> {
        block_on_compat(self.pathconf_path(path))
    }

    /// Blocking version of [`Mount::read`]
    fn sync_read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
        block_on_compat(self.read(fh, offset, count))
    }

    /// Blocking version of [`Mount::read_path`]
    fn sync_read_path(&self, path: &str, offset: u64, count: u32) -> Result<Bytes> {
        block_on_compat(self.read_path(path, offset, count))
    }

    /// Blocking version of [`Mount::write`]
    fn sync_write(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<u32> {
        block_on_compat(self.write(fh, offset, data))
    }

    /// Blocking version of [`Mount::write_path`]
    fn sync_write_path(&self, path: &str, offset: u64, data: Bytes) -> Result<u32> {
        block_on_compat(self.write_path(path, offset, data))
    }

    /// Blocking version of [`Mount::readdir`]
    fn sync_readdir(&self, dir_fh: Bytes) -> Result<Vec<ReaddirEntry>> {
        block_on_compat(async { self.readdir(dir_fh).await.try_collect().await })
    }

    /// Blocking version of [`Mount::readdir_path`]
    fn sync_readdir_path(&self, dir_path: &str) -> Result<Vec<ReaddirEntry>> {
        block_on_compat(async { self.readdir_path(dir_path).await?.try_collect().await })
    }

    /// Blocking version of [`Mount::readdirplus`]
    fn sync_readdirplus(&self, dir_fh: Bytes) -> Result<Vec<ReaddirplusEntry>> {
        block_on_compat(async { self.readdirplus(dir_fh).await.try_collect().await })
    }

    /// Blocking version of [`Mount::readdirplus_path`]
    fn sync_readdirplus_path(&self, dir_path: &str) -> Result<Vec<ReaddirplusEntry>> {
        block_on_compat(async { self.readdirplus_path(dir_path).await?.try_collect().await })
    }

    /// Blocking version of [`Mount::mkdir`]
    fn sync_mkdir(&self, dir_fh: Bytes, dirname: &str, mode: u32) -> Result<ObjRes> {
        block_on_compat(self.mkdir(dir_fh, dirname, mode))
    }

    /// Blocking version of [`Mount::mkdir_path`]
    fn sync_mkdir_path(&self, path: &str, mode: u32) -> Result<ObjRes> {
        block_on_compat(self.mkdir_path(path, mode))
    }

    /// Blocking version of [`Mount::remove`]
    fn sync_remove(&self, dir_fh: Bytes, filename: &str) -> Result<()> {
        block_on_compat(self.remove(dir_fh, filename))
    }

    /// Blocking version of [`Mount::remove_path`]
    fn sync_remove_path(&self, path: &str) -> Result<()> {
        block_on_compat(self.remove_path(path))
    }

    /// Blocking version of [`Mount::rmdir`]
    fn sync_rmdir(&self, dir_fh: Bytes, dirname: &str) -> Result<()> {
        block_on_compat(self.rmdir(dir_fh, dirname))
    }

    /// Blocking version of [`Mount::rmdir_path`]
    fn sync_rmdir_path(&self, path: &str) -> Result<()> {
        block_on_compat(self.rmdir_path(path))
    }

    /// Blocking version of [`Mount::rename`]
    fn sync_rename(
        &self,
        from_dir_fh: Bytes,
        from_filename: &str,
        to_dir_fh: Bytes,
        to_filename: &str,
    ) -> Result<()> {
        block_on_compat(self.rename(from_dir_fh, from_filename, to_dir_fh, to_filename))
    }

    /// Blocking version of [`Mount::rename_path`]
    fn sync_rename_path(&self, from_path: &str, to_path: &str) -> Result<()> {
        block_on_compat(self.rename_path(from_path, to_path))
    }

    /// Blocking version of [`Mount::umount`]
    fn sync_umount(&self) -> Result<()> {
        block_on_compat(self.umount())
    }

    /// Procedure EXPORT returns the list of file systems exported by the server.
    ///
    /// Each entry contains the export path and the list of allowed client groups or hosts.
    /// An empty `groups` list means the export is accessible by any client (world-readable).
    /// Equivalent to `showmount -e`.
    ///
    /// # Example
    ///
    /// ```
    /// async fn print_exports(mount: &dyn nfs_rs::Mount) -> nfs_rs::Result<()> {
    ///     for entry in mount.exports().await? {
    ///         println!("{} {:?}", entry.path, entry.groups);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    async fn exports(&self) -> Result<Vec<ExportEntry>>;

    /// Blocking version of [`Mount::exports`]
    fn sync_exports(&self) -> Result<Vec<ExportEntry>> {
        block_on_compat(self.exports())
    }
}

/// A single entry returned by [`Mount::exports`]: an exported path and the list of
/// allowed client groups (empty = unrestricted / world-accessible).
#[derive(Debug, Default, PartialEq, Clone)]
pub struct ExportEntry {
    pub path: String,
    pub groups: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NFSVersion {
    Unknown,
    NFSv3,
    NFSv4p0,
    /// Compatibility spelling retained for callers of releases before 0.5.0.
    /// URL parsing intentionally does not map the ambiguous selector `4` here.
    #[deprecated(note = "use NFSVersion::NFSv4p0 and the exact URL selector version=4.0")]
    NFSv4,
    NFSv4p1,
    NFSv4p2,
}

impl From<&str> for NFSVersion {
    fn from(val: &str) -> Self {
        match val {
            "3" => NFSVersion::NFSv3,
            "4.0" => NFSVersion::NFSv4p0,
            "4.1" => NFSVersion::NFSv4p1,
            "4.2" => NFSVersion::NFSv4p2,
            _ => NFSVersion::Unknown,
        }
    }
}

/// Struct describing attributes for an NFS entry.
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Attr {
    pub type_: u32,
    pub file_mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub filesize: u64,
    pub used: u64,
    pub spec_data: [u32; 2],
    pub fsid: u64,
    pub fileid: u64,
    pub atime: Time,
    pub mtime: Time,
    pub ctime: Time,
    /// NFSv4 ACL decoded from FATTR4_ACL (attr #12). `None` for NFSv3 or if not requested.
    pub acl: Option<Acl>,
    /// NFSv4 raw owner string (attr #35), e.g. "root@localdomain" or "0".
    /// Empty string for NFSv3 or if not present.
    pub owner: String,
    /// NFSv4 raw owner_group string (attr #37 in NFSv4.1).
    pub owner_group: String,
    /// NFSv4.1 per-entry filehandle (FATTR4_FILEHANDLE, attr #19).
    /// Returned by READDIR when requested in the attr bitmap.
    /// Empty for NFSv3 or if not requested.
    pub filehandle: Bytes,
}

/// NFSv4 ACE type (RFC 7530 §6.2.1.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum AceType {
    AccessAllowed = 0,
    AccessDenied = 1,
    SystemAudit = 2,
    SystemAlarm = 3,
}

/// NFSv4 ACE flags bitfield (RFC 7530 §6.2.1.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AceFlags(pub u32);

impl AceFlags {
    pub const FILE_INHERIT: u32 = 0x0000_0001;
    pub const DIRECTORY_INHERIT: u32 = 0x0000_0002;
    pub const NO_PROPAGATE_INHERIT: u32 = 0x0000_0004;
    pub const INHERIT_ONLY: u32 = 0x0000_0008;
    pub const SUCCESSFUL_ACCESS: u32 = 0x0000_0010;
    pub const FAILED_ACCESS: u32 = 0x0000_0020;
    pub const IDENTIFIER_GROUP: u32 = 0x0000_0040;

    pub fn contains(self, flag: u32) -> bool {
        self.0 & flag != 0
    }
}

/// NFSv4 ACE access mask bitfield (RFC 7530 §6.2.1.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AceMask(pub u32);

impl AceMask {
    pub const READ_DATA: u32 = 0x0000_0001;
    pub const LIST_DIRECTORY: u32 = 0x0000_0001;
    pub const WRITE_DATA: u32 = 0x0000_0002;
    pub const ADD_FILE: u32 = 0x0000_0002;
    pub const APPEND_DATA: u32 = 0x0000_0004;
    pub const ADD_SUBDIRECTORY: u32 = 0x0000_0004;
    pub const READ_NAMED_ATTRS: u32 = 0x0000_0008;
    pub const WRITE_NAMED_ATTRS: u32 = 0x0000_0010;
    pub const EXECUTE: u32 = 0x0000_0020;
    pub const DELETE_CHILD: u32 = 0x0000_0040;
    pub const READ_ATTRIBUTES: u32 = 0x0000_0080;
    pub const WRITE_ATTRIBUTES: u32 = 0x0000_0100;
    pub const DELETE: u32 = 0x0001_0000;
    pub const READ_ACL: u32 = 0x0002_0000;
    pub const WRITE_ACL: u32 = 0x0004_0000;
    pub const WRITE_OWNER: u32 = 0x0008_0000;
    pub const SYNCHRONIZE: u32 = 0x0010_0000;

    pub fn contains(self, mask: u32) -> bool {
        self.0 & mask != 0
    }
}

/// A single NFSv4 Access Control Entry (RFC 7530 §6.2.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NfsAce {
    pub ace_type: AceType,
    pub flags: AceFlags,
    pub access_mask: AceMask,
    pub who: String,
}

/// NFSv4 ACL: ordered list of access control entries.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Acl {
    pub aces: Vec<NfsAce>,
}

/// Bitmask of ACE types supported by the server (FATTR4_ACLSUPPORT, attr #13).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AclSupport(pub u32);

impl AclSupport {
    pub const ALLOW: u32 = 0x0000_0001;
    pub const DENY: u32 = 0x0000_0002;
    pub const AUDIT: u32 = 0x0000_0004;
    pub const ALARM: u32 = 0x0000_0008;

    pub fn supports(self, ace_type: u32) -> bool {
        self.0 & ace_type != 0
    }
}

/// Struct describing non-volatile file system state information.
#[derive(Debug, Default, PartialEq)]
pub struct FSInfo {
    pub attr: Option<Attr>,
    pub rtmax: u32,
    pub rtpref: u32,
    pub rtmult: u32,
    pub wtmax: u32,
    pub wtpref: u32,
    pub wtmult: u32,
    pub dtpref: u32,
    pub maxfilesize: u64,
    pub time_delta: Time,
    pub properties: u32,
}

/// Struct describing volatile file system state information.
#[derive(Debug, Default, PartialEq)]
pub struct FSStat {
    pub attr: Option<Attr>,
    pub tbytes: u64,
    pub fbytes: u64,
    pub abytes: u64,
    pub tfiles: u64,
    pub ffiles: u64,
    pub afiles: u64,
    pub invarsec: u32,
}

/// Struct describing an NFS entry response as returned by various procedures.
#[derive(Debug, Default, PartialEq, Clone)]
pub struct ObjRes {
    pub fh: Bytes,
    pub attr: Option<Attr>,
}

/// Struct describing path configuration for an NFS entry as returned by [`Mount::pathconf`] and [`Mount::pathconf_path`].
#[derive(Debug, Default, PartialEq)]
pub struct Pathconf {
    pub attr: Option<Attr>,
    pub linkmax: u32,
    pub name_max: u32,
    pub no_trunc: bool,
    pub chown_restricted: bool,
    pub case_insensitive: bool,
    pub case_preserving: bool,
}

/// Struct describing a single NFS entry as returned by [`Mount::readdir`] and [`Mount::readdir_path`].
#[derive(Debug)]
pub struct ReaddirEntry {
    pub fileid: u64,
    pub file_name: String,
}

/// Struct describing a single NFS entry as returned by [`Mount::readdirplus`] and [`Mount::readdirplus_path`].
#[derive(Debug)]
pub struct ReaddirplusEntry {
    pub fileid: u64,
    pub file_name: String,
    pub attr: Option<Attr>,
    pub handle: Bytes,
}
