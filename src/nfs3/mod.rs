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

mod access;
mod commit;
mod create;
mod export;
mod fsinfo;
mod fsstat;
mod getattr;
mod link;
mod lookup;
mod mkdir;
mod mount;
mod null;
mod pathconf;
mod read;
mod readdir;
mod readdirplus;
mod readlink;
mod remove;
mod rename;
mod rmdir;
mod setattr;
mod symlink;
mod umount;
mod write;

pub(crate) use mount::{mount, query_exports};

use crate::{rpc, Auth, ObjRes, Time};
use crate::error::{NfsError, Result};
use bytes::Bytes;

/// Convert NFS byte-string to Rust String. Tries UTF-8 first;
/// falls back to lossy conversion with a warning for non-UTF-8 filenames.
pub(crate) fn bytes_to_string(raw: Bytes) -> String {
    match std::str::from_utf8(&raw) {
        Ok(s) => s.to_owned(),
        Err(_) => {
            tracing::warn!(raw_hex = %hex_preview(&raw), "NFS name contains invalid UTF-8, using lossy conversion");
            String::from_utf8_lossy(&raw).into_owned()
        }
    }
}

fn hex_preview(bytes: &[u8]) -> String {
    let limit = bytes.len().min(32);
    let hex: String = bytes[..limit].iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" ");
    if bytes.len() > 32 { format!("{}...", hex) } else { hex }
}

pub(crate) mod fastxdr;

// Re-export fastxdr response types used by procedure files.
// Note: filename3 and nfspath3 are NOT re-exported here because we have local
// request-encoding structs with those names. Callers use the local encoding types.
pub(crate) use fastxdr::{
    entry3, entryplus3, fattr3, mount_mountstat3, mountres3_ok, nfsstat3, nfstime3, post_op_attr,
    post_op_fh3, ACCESS3resok, COMMIT3resok, CREATE3resok, FSINFO3resok, FSSTAT3resok,
    GETATTR3resok, LINK3resok, LOOKUP3resok, MKDIR3resok, PATHCONF3resok, READ3resok,
    READDIR3resok, READDIRPLUS3resok, READLINK3resok, REMOVE3resok, RENAME3resok, RMDIR3resok,
    SETATTR3resok, SYMLINK3resok, WRITE3resok, stable_how,
};

/// Shared paging logic for `readdir` and `readdirplus` streams.
///
/// Generates a `try_unfold + try_flatten` stream that fetches directory pages
/// via `$fetch_page`, then yields entries directly from the XDR linked list
/// without an intermediate `Vec` — each entry node is converted by `$convert`.
///
/// The linked list is walked twice per page: once (read-only) to find the last
/// cookie and entry count, then once (destructive) via `from_fn` to yield entries.
macro_rules! paged_dir_stream {
    ($self:expr, $dir_fh:expr, $fetch_page:ident, $convert:expr, $label:literal) => {{
        use futures::stream::TryStreamExt as _;
        let this = $self;
        futures::stream::try_unfold(
            Some(($dir_fh, 0u64, [0u8; 8])),
            move |state| async move {
                let Some((fh, cookie, verifier)) = state else {
                    return Ok::<_, crate::error::NfsError>(None);
                };
                let res = this.$fetch_page(fh.clone(), cookie, verifier).await?;
                let new_verifier: [u8; 8] =
                    res.cookieverf.0.as_ref().try_into().unwrap_or([0u8; 8]);
                let eof = res.reply.eof;
                // Walk linked list (read-only) for last cookie and count.
                let (new_cookie, entry_count, entries_head) = match res.reply.entries {
                    Some(entry) => {
                        let mut count = 0usize;
                        let mut last_cookie = cookie;
                        let mut e = &*entry;
                        loop {
                            count += 1;
                            last_cookie = e.cookie.0;
                            match &e.nextentry {
                                Some(next) => e = next,
                                None => break,
                            }
                        }
                        (last_cookie, count, Some(entry))
                    }
                    None => (cookie, 0, None),
                };
                tracing::debug!(cookie = new_cookie, eof, entry_count, $label);
                let next = if eof || entry_count == 0 {
                    None
                } else {
                    Some((fh, new_cookie, new_verifier))
                };
                // Yield entries directly from the linked list — no intermediate Vec.
                let convert = $convert;
                let entry_iter = {
                    let mut current = entries_head;
                    std::iter::from_fn(move || {
                        let mut node = current.take()?;
                        current = node.nextentry.take();
                        Some(Ok(convert(node)))
                    })
                };
                Ok(Some((futures::stream::iter(entry_iter), next)))
            },
        )
        .try_flatten()
    }};
}

pub(crate) use paged_dir_stream;

#[allow(dead_code)]
enum MountProc3 {
    Null = 0,
    Mount = 1,
    Umount = 3,
    Export = 5,
}

enum NFSProc3 {
    Null = 0,
    GetAttr = 1,
    SetAttr = 2,
    Lookup = 3,
    Access = 4,
    Readlink = 5,
    Read = 6,
    Write = 7,
    Create = 8,
    Mkdir = 9,
    Symlink = 10,
    Remove = 12,
    Rmdir = 13,
    Rename = 14,
    Link = 15,
    Readdir = 16,
    Readdirplus = 17,
    FSStat = 18,
    FSInfo = 19,
    Pathconf = 20,
    Commit = 21,
}

// XDR encoding trait for request argument types.
trait XdrEncode {
    fn encode(&self, buf: &mut Vec<u8>);
}

// Helper encoding functions.
fn xdr_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn xdr_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn xdr_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// Write `data` followed by padding to the next 4-byte boundary.
fn xdr_fixed_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(data);
    let pad = (4 - data.len() % 4) % 4;
    for _ in 0..pad {
        buf.push(0);
    }
}

/// Write a variable-length XDR opaque: 4-byte length, then data padded to 4-byte boundary.
fn xdr_var_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    xdr_u32(buf, data.len() as u32);
    xdr_fixed_bytes(buf, data);
}

/// Write an XDR variable-length string (same encoding as opaque).
fn xdr_string(buf: &mut Vec<u8>, s: &str) {
    xdr_var_bytes(buf, s.as_bytes());
}

// Mount phase uses a small retry count for fast failure detection.
const MOUNT_RETRIES: usize = 2;
// Post-mount NFS operations use a higher retry count for resilience.
const NFS_RETRIES: usize = 10;
// Timeout for metadata operations (LOOKUP, GETATTR, READDIR, etc.).
const METADATA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
// Base timeout for data operations (READ, WRITE). Scaled up for large payloads.
const DATA_TIMEOUT_BASE_SECS: u64 = 10;
// Minimum bandwidth assumed for timeout scaling (10 Mbps = ~1.25 MB/s).
const MIN_BANDWIDTH_BYTES_PER_SEC: u64 = 1_250_000;

fn data_timeout(data_size: usize) -> std::time::Duration {
    let transfer_secs = data_size as u64 / MIN_BANDWIDTH_BYTES_PER_SEC;
    std::time::Duration::from_secs(DATA_TIMEOUT_BASE_SECS + transfer_secs)
}

// ─── nfs3_call! macro ────────────────────────────────────────────────────────
//
// Generates a private async method on Mount that:
//   1. Packs the RPC header + args into a Vec<u8>.
//   2. Sends via rpc::Client::call().
//   3. Decodes the nfsstat3 status from the first 4 bytes.
//   4. On NFS3_OK, decodes the *resok struct via TryFrom<&mut Bytes>.
//   5. On error, returns the nfsstat3 error code as an Err.

macro_rules! nfs3_call {
    ($name:ident, $proc:ident, $args:ty, $resok:ty) => {
        nfs3_call!($name, $proc, $args, $resok, warn);
    };
    ($name:ident, $proc:ident, $args:ty, $resok:ty, $err_level:ident) => {
        async fn $name(&self, args: $args) -> Result<$resok> {
            let mut buf = Vec::<u8>::new();
            self.pack_nfs3(NFSProc3::$proc, &args, &mut buf);
            tracing::debug!(proc = stringify!($proc), "NFS3 call");
            let mut bytes = self.rpc.call(buf, NFS_RETRIES, METADATA_TIMEOUT).await?;
            let status = nfsstat3::try_from(&mut bytes)
                .map_err(|e| NfsError::Xdr(e.to_string()))?;
            match status {
                nfsstat3::NFS3_OK => {
                    tracing::trace!(proc = stringify!($proc), "NFS3 call succeeded");
                    <$resok>::try_from(&mut bytes)
                        .map_err(|e| NfsError::Xdr(e.to_string()))
                }
                e => {
                    tracing::$err_level!(proc = stringify!($proc), status = ?e, "NFS3 call returned error status");
                    Err(NfsError::Nfs3(e))
                }
            }
        }
    };
}

// ─── Mount struct ────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Mount {
    pub(crate) rpc: rpc::Client,
    pub(crate) auth: Auth,
    pub(crate) fh: Bytes,
    pub(crate) dir: String,
    pub(crate) dircount: u32,
    pub(crate) maxcount: u32,
    pub(crate) rsize: u32,
    pub(crate) wsize: u32,
}

impl Mount {
    fn pack_nfs3(&self, proc: NFSProc3, args: &dyn XdrEncode, buf: &mut Vec<u8>) {
        rpc_header(rpc::NFS_PROG, rpc::NFS3_VERSION, proc as u32, &self.auth).encode(buf);
        args.encode(buf);
    }

    pub fn getfh(&self) -> Bytes {
        self.fh.clone()
    }

    // Special-cased: NULL returns no body to decode.
    async fn _null(&self, args: NULL3args) -> Result<()> {
        let mut buf = Vec::<u8>::new();
        self.pack_nfs3(NFSProc3::Null, &args, &mut buf);
        tracing::debug!(proc = "Null", "NFS3 call");
        let _ = self.rpc.call(buf, NFS_RETRIES, METADATA_TIMEOUT).await?;
        tracing::trace!(proc = "Null", "NFS3 call succeeded");
        Ok(())
    }

    nfs3_call!(_access, Access, ACCESS3args, ACCESS3resok);
    nfs3_call!(_commit, Commit, COMMIT3args, COMMIT3resok);
    nfs3_call!(_create, Create, CREATE3args, CREATE3resok);
    nfs3_call!(_fsinfo, FSInfo, FSINFO3args, FSINFO3resok);
    nfs3_call!(_fsstat, FSStat, FSSTAT3args, FSSTAT3resok);
    nfs3_call!(_getattr, GetAttr, GETATTR3args, GETATTR3resok);
    nfs3_call!(_link, Link, LINK3args, LINK3resok);
    nfs3_call!(_lookup, Lookup, LOOKUP3args, LOOKUP3resok);
    nfs3_call!(_mkdir, Mkdir, MKDIR3args, MKDIR3resok, info);
    nfs3_call!(_pathconf, Pathconf, PATHCONF3args, PATHCONF3resok);
    // _read is special-cased: timeout scales with requested read size.
    async fn _read(&self, args: READ3args) -> Result<READ3resok> {
        let timeout = data_timeout(args.count as usize);
        let mut buf = Vec::<u8>::new();
        self.pack_nfs3(NFSProc3::Read, &args, &mut buf);
        tracing::debug!(proc = "Read", "NFS3 call");
        let mut bytes = self.rpc.call(buf, NFS_RETRIES, timeout).await?;
        let status = nfsstat3::try_from(&mut bytes)
            .map_err(|e| NfsError::Xdr(e.to_string()))?;
        match status {
            nfsstat3::NFS3_OK => {
                tracing::trace!(proc = "Read", "NFS3 call succeeded");
                READ3resok::try_from(&mut bytes)
                    .map_err(|e| NfsError::Xdr(e.to_string()))
            }
            e => {
                tracing::warn!(proc = "Read", status = ?e, "NFS3 call returned error status");
                Err(NfsError::Nfs3(e))
            }
        }
    }
    nfs3_call!(_readdir, Readdir, READDIR3args, READDIR3resok);
    nfs3_call!(_readdirplus, Readdirplus, READDIRPLUS3args, READDIRPLUS3resok);
    nfs3_call!(_readlink, Readlink, READLINK3args, READLINK3resok);
    nfs3_call!(_remove, Remove, REMOVE3args, REMOVE3resok);
    nfs3_call!(_rename, Rename, RENAME3args, RENAME3resok);
    nfs3_call!(_rmdir, Rmdir, RMDIR3args, RMDIR3resok);
    nfs3_call!(_setattr, SetAttr, SETATTR3args, SETATTR3resok);
    nfs3_call!(_symlink, Symlink, SYMLINK3args, SYMLINK3resok);
    // _write is special-cased to avoid copying the write payload into the request buffer.
    // WRITE3args::encode writes only the XDR length prefix; the raw data is passed to
    // call_with_data which appends it directly to the TCP stream.
    async fn _write(&self, args: WRITE3args) -> Result<WRITE3resok> {
        let data = args.data.clone();
        let timeout = data_timeout(data.len());
        let mut buf = Vec::<u8>::new();
        self.pack_nfs3(NFSProc3::Write, &args, &mut buf);
        tracing::debug!(proc = "Write", data_len = data.len(), "NFS3 call");
        let mut bytes = self.rpc.call_with_data(buf, data, NFS_RETRIES, timeout).await?;
        let status = nfsstat3::try_from(&mut bytes)
            .map_err(|e| NfsError::Xdr(e.to_string()))?;
        match status {
            nfsstat3::NFS3_OK => {
                tracing::trace!(proc = "Write", "NFS3 call succeeded");
                WRITE3resok::try_from(&mut bytes)
                    .map_err(|e| NfsError::Xdr(e.to_string()))
            }
            e => {
                tracing::warn!(proc = "Write", status = ?e, "NFS3 call returned error status");
                Err(NfsError::Nfs3(e))
            }
        }
    }
}

pub(crate) fn rpc_header(prog: u32, vers: u32, proc: u32, cred: &Auth) -> rpc::Header {
    rpc::Header::new(rpc::RPC_VERSION, prog, vers, proc, cred, &Auth::new_null())
}

// ─── Request argument types ──────────────────────────────────────────────────
// These are encoding-only types used to build NFS3 wire requests.
// Response types come from the fastxdr module (TryFrom<Bytes>).

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) struct nfs_fh3 {
    pub(crate) data: Bytes,
}

impl XdrEncode for nfs_fh3 {
    fn encode(&self, buf: &mut Vec<u8>) {
        xdr_var_bytes(buf, &self.data);
    }
}

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) struct filename3(pub(crate) String);

impl XdrEncode for filename3 {
    fn encode(&self, buf: &mut Vec<u8>) {
        xdr_string(buf, &self.0);
    }
}

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) struct nfspath3(pub(crate) String);

impl XdrEncode for nfspath3 {
    fn encode(&self, buf: &mut Vec<u8>) {
        xdr_string(buf, &self.0);
    }
}

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) struct diropargs3 {
    pub(crate) dir: nfs_fh3,
    pub(crate) name: filename3,
}

impl XdrEncode for diropargs3 {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.dir.encode(buf);
        self.name.encode(buf);
    }
}

// sattr3 field types — match the old nfs3xdr.rs enums.
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) enum set_mode3 {
    TRUE(u32),
    default,
}

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) enum set_uid3 {
    TRUE(u32),
    default,
}

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) enum set_gid3 {
    TRUE(u32),
    default,
}

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) enum set_size3 {
    TRUE(u64),
    default,
}

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) enum set_atime {
    SET_TO_CLIENT_TIME(nfstime3_req),
    default,
}

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) enum set_mtime {
    SET_TO_CLIENT_TIME(nfstime3_req),
    default,
}

/// nfstime3 used for *encoding* in requests (distinct from the fastxdr decoding type).
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) struct nfstime3_req {
    pub(crate) seconds: u32,
    pub(crate) nseconds: u32,
}

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) struct sattr3 {
    pub(crate) mode: set_mode3,
    pub(crate) uid: set_uid3,
    pub(crate) gid: set_gid3,
    pub(crate) size: set_size3,
    pub(crate) atime: set_atime,
    pub(crate) mtime: set_mtime,
}

impl Default for sattr3 {
    fn default() -> Self {
        Self {
            mode: set_mode3::default,
            uid: set_uid3::default,
            gid: set_gid3::default,
            size: set_size3::default,
            atime: set_atime::default,
            mtime: set_mtime::default,
        }
    }
}

impl XdrEncode for sattr3 {
    fn encode(&self, buf: &mut Vec<u8>) {
        // mode
        match &self.mode {
            set_mode3::TRUE(v) => {
                xdr_i32(buf, 1);
                xdr_u32(buf, *v);
            }
            set_mode3::default => xdr_i32(buf, 0),
        }
        // uid
        match &self.uid {
            set_uid3::TRUE(v) => {
                xdr_i32(buf, 1);
                xdr_u32(buf, *v);
            }
            set_uid3::default => xdr_i32(buf, 0),
        }
        // gid
        match &self.gid {
            set_gid3::TRUE(v) => {
                xdr_i32(buf, 1);
                xdr_u32(buf, *v);
            }
            set_gid3::default => xdr_i32(buf, 0),
        }
        // size
        match &self.size {
            set_size3::TRUE(v) => {
                xdr_i32(buf, 1);
                xdr_u64(buf, *v);
            }
            set_size3::default => xdr_i32(buf, 0),
        }
        // atime
        match &self.atime {
            set_atime::SET_TO_CLIENT_TIME(t) => {
                xdr_i32(buf, 2); // SET_TO_CLIENT_TIME
                xdr_u32(buf, t.seconds);
                xdr_u32(buf, t.nseconds);
            }
            set_atime::default => xdr_i32(buf, 0),
        }
        // mtime
        match &self.mtime {
            set_mtime::SET_TO_CLIENT_TIME(t) => {
                xdr_i32(buf, 2); // SET_TO_CLIENT_TIME
                xdr_u32(buf, t.seconds);
                xdr_u32(buf, t.nseconds);
            }
            set_mtime::default => xdr_i32(buf, 0),
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) enum sattrguard3 {
    TRUE(nfstime3_req),
    FALSE,
}

impl XdrEncode for sattrguard3 {
    fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            sattrguard3::TRUE(t) => {
                xdr_i32(buf, 1);
                xdr_u32(buf, t.seconds);
                xdr_u32(buf, t.nseconds);
            }
            sattrguard3::FALSE => xdr_i32(buf, 0),
        }
    }
}


#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub(crate) struct symlinkdata3 {
    pub(crate) symlink_attributes: sattr3,
    pub(crate) symlink_data: nfspath3,
}

impl XdrEncode for symlinkdata3 {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.symlink_attributes.encode(buf);
        self.symlink_data.encode(buf);
    }
}

#[allow(non_camel_case_types, dead_code)]
#[derive(Debug, PartialEq)]
pub(crate) enum createhow3 {
    UNCHECKED(sattr3),
    GUARDED(sattr3),
    EXCLUSIVE([u8; 8]),
}

impl XdrEncode for createhow3 {
    fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            createhow3::UNCHECKED(a) => {
                xdr_i32(buf, 0); // UNCHECKED
                a.encode(buf);
            }
            createhow3::GUARDED(a) => {
                xdr_i32(buf, 1); // GUARDED
                a.encode(buf);
            }
            createhow3::EXCLUSIVE(v) => {
                xdr_i32(buf, 2); // EXCLUSIVE
                buf.extend_from_slice(v);
            }
        }
    }
}

// ─── NFS3 argument structs ───────────────────────────────────────────────────

#[derive(Debug, PartialEq)]
pub(crate) struct NULL3args {}

impl XdrEncode for NULL3args {
    fn encode(&self, _buf: &mut Vec<u8>) {}
}

#[derive(Debug, PartialEq)]
pub(crate) struct GETATTR3args {
    pub(crate) object: nfs_fh3,
}

impl XdrEncode for GETATTR3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.object.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct ACCESS3args {
    pub(crate) object: nfs_fh3,
    pub(crate) access: u32,
}

impl XdrEncode for ACCESS3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.object.encode(buf);
        xdr_u32(buf, self.access);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct READLINK3args {
    pub(crate) symlink: nfs_fh3,
}

impl XdrEncode for READLINK3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.symlink.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct READ3args {
    pub(crate) file: nfs_fh3,
    pub(crate) offset: u64,
    pub(crate) count: u32,
}

impl XdrEncode for READ3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.file.encode(buf);
        xdr_u64(buf, self.offset);
        xdr_u32(buf, self.count);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct WRITE3args {
    pub(crate) file: nfs_fh3,
    pub(crate) offset: u64,
    pub(crate) count: u32,
    pub(crate) stable: WriteStable,
    pub(crate) data: Bytes,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub(crate) enum WriteStable {
    Unstable = 0,
    DataSync = 1,
    FileSync = 2,
}

impl XdrEncode for WRITE3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.file.encode(buf);
        xdr_u64(buf, self.offset);
        xdr_u32(buf, self.count);
        xdr_i32(buf, self.stable.as_i32());
        // Write only the XDR opaque length prefix here. The raw payload bytes are
        // sent separately by call_with_data to avoid copying the user buffer.
        xdr_u32(buf, self.data.len() as u32);
    }
}

impl WriteStable {
    fn as_i32(&self) -> i32 {
        match self {
            WriteStable::Unstable => 0,
            WriteStable::DataSync => 1,
            WriteStable::FileSync => 2,
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct CREATE3args {
    pub(crate) where_: diropargs3,
    pub(crate) how: createhow3,
}

impl XdrEncode for CREATE3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.where_.encode(buf);
        self.how.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct MKDIR3args {
    pub(crate) where_: diropargs3,
    pub(crate) attrs: sattr3,
}

impl XdrEncode for MKDIR3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.where_.encode(buf);
        self.attrs.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct SYMLINK3args {
    pub(crate) where_: diropargs3,
    pub(crate) symlink: symlinkdata3,
}

impl XdrEncode for SYMLINK3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.where_.encode(buf);
        self.symlink.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct REMOVE3args {
    pub(crate) object: diropargs3,
}

impl XdrEncode for REMOVE3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.object.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct RMDIR3args {
    pub(crate) object: diropargs3,
}

impl XdrEncode for RMDIR3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.object.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct RENAME3args {
    pub(crate) from: diropargs3,
    pub(crate) to: diropargs3,
}

impl XdrEncode for RENAME3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.from.encode(buf);
        self.to.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct LINK3args {
    pub(crate) file: nfs_fh3,
    pub(crate) link: diropargs3,
}

impl XdrEncode for LINK3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.file.encode(buf);
        self.link.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct READDIR3args {
    pub(crate) dir: nfs_fh3,
    pub(crate) cookie: u64,
    pub(crate) cookieverf: [u8; 8],
    pub(crate) count: u32,
}

impl XdrEncode for READDIR3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.dir.encode(buf);
        xdr_u64(buf, self.cookie);
        buf.extend_from_slice(&self.cookieverf);
        xdr_u32(buf, self.count);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct READDIRPLUS3args {
    pub(crate) dir: nfs_fh3,
    pub(crate) cookie: u64,
    pub(crate) cookieverf: [u8; 8],
    pub(crate) dircount: u32,
    pub(crate) maxcount: u32,
}

impl XdrEncode for READDIRPLUS3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.dir.encode(buf);
        xdr_u64(buf, self.cookie);
        buf.extend_from_slice(&self.cookieverf);
        xdr_u32(buf, self.dircount);
        xdr_u32(buf, self.maxcount);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct FSSTAT3args {
    pub(crate) fsroot: nfs_fh3,
}

impl XdrEncode for FSSTAT3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.fsroot.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct FSINFO3args {
    pub(crate) fsroot: nfs_fh3,
}

impl XdrEncode for FSINFO3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.fsroot.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct PATHCONF3args {
    pub(crate) object: nfs_fh3,
}

impl XdrEncode for PATHCONF3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.object.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct COMMIT3args {
    pub(crate) file: nfs_fh3,
    pub(crate) offset: u64,
    pub(crate) count: u32,
}

impl XdrEncode for COMMIT3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.file.encode(buf);
        xdr_u64(buf, self.offset);
        xdr_u32(buf, self.count);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct SETATTR3args {
    pub(crate) object: nfs_fh3,
    pub(crate) new_attributes: sattr3,
    pub(crate) guard: sattrguard3,
}

impl XdrEncode for SETATTR3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.object.encode(buf);
        self.new_attributes.encode(buf);
        self.guard.encode(buf);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct LOOKUP3args {
    pub(crate) what: diropargs3,
}

impl XdrEncode for LOOKUP3args {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.what.encode(buf);
    }
}

// ─── MOUNT protocol argument encoding ────────────────────────────────────────

/// Encode a MOUNT dirpath argument (variable-length string).
pub(crate) fn encode_dirpath(buf: &mut Vec<u8>, path: &str) {
    xdr_string(buf, path);
}

// ─── From/Into conversions ────────────────────────────────────────────────────

impl From<nfstime3> for Time {
    fn from(time: nfstime3) -> Self {
        Self {
            seconds: time.seconds,
            nseconds: time.nseconds,
        }
    }
}

impl From<fattr3> for crate::mount::Attr {
    fn from(attr: fattr3) -> Self {
        Self {
            type_: attr.type_v as u32,
            file_mode: attr.mode.0,
            nlink: attr.nlink,
            uid: attr.uid.0,
            gid: attr.gid.0,
            filesize: attr.size.0,
            used: attr.used.0,
            spec_data: [attr.rdev.specdata1, attr.rdev.specdata2],
            fsid: attr.fsid,
            fileid: attr.fileid.0,
            atime: attr.atime.into(),
            mtime: attr.mtime.into(),
            ctime: attr.ctime.into(),
            acl: None,
            owner: String::new(),
            owner_group: String::new(),
            filehandle: Bytes::new(),
        }
    }
}

impl From<post_op_attr> for Option<crate::mount::Attr> {
    fn from(attr: post_op_attr) -> Self {
        match attr {
            post_op_attr::TRUE(a) => Some(a.into()),
            post_op_attr::FALSE => None,
        }
    }
}

impl From<FSINFO3resok> for crate::mount::FSInfo {
    fn from(ok: FSINFO3resok) -> Self {
        Self {
            attr: ok.obj_attributes.into(),
            rtmax: ok.rtmax,
            rtpref: ok.rtpref,
            rtmult: ok.rtmult,
            wtmax: ok.wtmax,
            wtpref: ok.wtpref,
            wtmult: ok.wtmult,
            dtpref: ok.dtpref,
            maxfilesize: ok.maxfilesize.0,
            time_delta: ok.time_delta.into(),
            properties: ok.properties,
        }
    }
}

impl From<FSSTAT3resok> for crate::mount::FSStat {
    fn from(ok: FSSTAT3resok) -> Self {
        Self {
            attr: ok.obj_attributes.into(),
            tbytes: ok.tbytes.0,
            fbytes: ok.fbytes.0,
            abytes: ok.abytes.0,
            tfiles: ok.tfiles.0,
            ffiles: ok.ffiles.0,
            afiles: ok.afiles.0,
            invarsec: ok.invarsec,
        }
    }
}

impl From<PATHCONF3resok> for crate::mount::Pathconf {
    fn from(ok: PATHCONF3resok) -> Self {
        Self {
            attr: ok.obj_attributes.into(),
            linkmax: ok.linkmax,
            name_max: ok.name_max,
            no_trunc: ok.no_trunc,
            chown_restricted: ok.chown_restricted,
            case_insensitive: ok.case_insensitive,
            case_preserving: ok.case_preserving,
        }
    }
}

/// Extract a Bytes file handle from a post_op_fh3.
pub(crate) fn from_post_op_fh3(pofh: post_op_fh3) -> Result<Bytes> {
    match pofh {
        post_op_fh3::TRUE(fh) => Ok(fh.0),
        post_op_fh3::FALSE => Err(NfsError::Rpc("bad file handle".to_string())),
    }
}

#[allow(unused)]
pub use fastxdr::nfsstat3 as ErrorCode;

impl std::error::Error for ErrorCode {}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::NFS3_OK => write!(f, "call completed successfully"),
            ErrorCode::NFS3ERR_PERM => write!(f, "permission denied"),
            ErrorCode::NFS3ERR_NOENT => write!(f, "no such file or directory"),
            ErrorCode::NFS3ERR_NXIO => write!(f, "i/o error - no such device or address"),
            ErrorCode::NFS3ERR_ACCES => write!(f, "access denied"),
            ErrorCode::NFS3ERR_EXIST => write!(f, "file exists"),
            ErrorCode::NFS3ERR_XDEV => write!(f, "cross-device hard link not allowed"),
            ErrorCode::NFS3ERR_NODEV => write!(f, "no such device"),
            ErrorCode::NFS3ERR_NOTDIR => write!(f, "not a directory"),
            ErrorCode::NFS3ERR_ISDIR => write!(f, "is a directory"),
            ErrorCode::NFS3ERR_INVAL => write!(f, "invalid or unsupported argument"),
            ErrorCode::NFS3ERR_FBIG => write!(f, "file too large"),
            ErrorCode::NFS3ERR_NOSPC => write!(f, "no space left on device"),
            ErrorCode::NFS3ERR_ROFS => write!(f, "read-only file system"),
            ErrorCode::NFS3ERR_MLINK => write!(f, "too many hard links"),
            ErrorCode::NFS3ERR_NAMETOOLONG => write!(f, "name is too long"),
            ErrorCode::NFS3ERR_NOTEMPTY => write!(f, "directory not empty"),
            ErrorCode::NFS3ERR_DQUOT => write!(f, "resource (quota) hard limit exceeded"),
            ErrorCode::NFS3ERR_STALE => write!(f, "invalid file handle"),
            ErrorCode::NFS3ERR_REMOTE => write!(f, "too many levels of remote in path"),
            ErrorCode::NFS3ERR_BADHANDLE => write!(f, "illegal NFS file handle"),
            ErrorCode::NFS3ERR_NOT_SYNC => write!(f, "update synchronization mismatch"),
            ErrorCode::NFS3ERR_BAD_COOKIE => write!(f, "cookie is stale"),
            ErrorCode::NFS3ERR_NOTSUPP => write!(f, "operation is not supported"),
            ErrorCode::NFS3ERR_TOOSMALL => write!(f, "buffer or request is too small"),
            ErrorCode::NFS3ERR_SERVERFAULT => write!(f, "internal server error"),
            ErrorCode::NFS3ERR_BADTYPE => write!(f, "type not supported by server"),
            ErrorCode::NFS3ERR_JUKEBOX => write!(f, "try again"),
            ErrorCode::NFS3ERR_IO => write!(
                f,
                "i/o error occurred while processing the requested operation"
            ),
        }
    }
}

#[allow(unused)]
pub use fastxdr::mount_mountstat3 as MountErrorCode;

impl std::error::Error for MountErrorCode {}

impl std::fmt::Display for MountErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MountErrorCode::MNT3_OK => write!(f, "call completed successfully"),
            MountErrorCode::MNT3ERR_PERM => write!(f, "permission denied"),
            MountErrorCode::MNT3ERR_NOENT => write!(f, "no such file or directory"),
            MountErrorCode::MNT3ERR_ACCES => write!(f, "access denied"),
            MountErrorCode::MNT3ERR_NOTDIR => write!(f, "not a directory"),
            MountErrorCode::MNT3ERR_INVAL => write!(f, "invalid or unsupported argument"),
            MountErrorCode::MNT3ERR_NAMETOOLONG => write!(f, "name is too long"),
            MountErrorCode::MNT3ERR_NOTSUPP => write!(f, "operation is not supported"),
            MountErrorCode::MNT3ERR_SERVERFAULT => write!(f, "internal server error"),
            MountErrorCode::MNT3ERR_IO => write!(
                f,
                "i/o error occurred while processing the requested operation"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_header_util() {
        let auth = crate::Auth::new_unix("machinist", 123, 987);
        let header = rpc_header(9, 8, 7, &auth);
        let expected =
            rpc::Header::new(rpc::RPC_VERSION, 9, 8, 7, &auth, &crate::Auth::new_null());
        assert_eq!(header, expected);
    }

    // ─── XDR encoding tests ──────────────────────────────────────────

    #[test]
    fn xdr_u32_encodes_big_endian() {
        let mut buf = Vec::new();
        xdr_u32(&mut buf, 0x01020304);
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn xdr_u64_encodes_big_endian() {
        let mut buf = Vec::new();
        xdr_u64(&mut buf, 0x0102030405060708);
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn xdr_i32_encodes_negative() {
        let mut buf = Vec::new();
        xdr_i32(&mut buf, -1);
        assert_eq!(buf, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn xdr_fixed_bytes_no_padding() {
        let mut buf = Vec::new();
        xdr_fixed_bytes(&mut buf, &[1, 2, 3, 4]); // 4 bytes, no padding needed
        assert_eq!(buf, [1, 2, 3, 4]);
    }

    #[test]
    fn xdr_fixed_bytes_with_padding() {
        let mut buf = Vec::new();
        xdr_fixed_bytes(&mut buf, &[1, 2, 3]); // 3 bytes → 1 byte padding
        assert_eq!(buf, [1, 2, 3, 0]);

        let mut buf = Vec::new();
        xdr_fixed_bytes(&mut buf, &[1, 2]); // 2 bytes → 2 bytes padding
        assert_eq!(buf, [1, 2, 0, 0]);

        let mut buf = Vec::new();
        xdr_fixed_bytes(&mut buf, &[1]); // 1 byte → 3 bytes padding
        assert_eq!(buf, [1, 0, 0, 0]);
    }

    #[test]
    fn xdr_fixed_bytes_empty() {
        let mut buf = Vec::new();
        xdr_fixed_bytes(&mut buf, &[]);
        assert!(buf.is_empty());
    }

    #[test]
    fn xdr_var_bytes_encodes_length_prefix_and_padding() {
        let mut buf = Vec::new();
        xdr_var_bytes(&mut buf, b"hello"); // 5 bytes → length(4) + data(5) + pad(3) = 12
        assert_eq!(buf.len(), 12);
        assert_eq!(&buf[0..4], &5u32.to_be_bytes()); // length = 5
        assert_eq!(&buf[4..9], b"hello");
        assert_eq!(&buf[9..12], &[0, 0, 0]); // padding
    }

    #[test]
    fn xdr_var_bytes_empty() {
        let mut buf = Vec::new();
        xdr_var_bytes(&mut buf, &[]);
        assert_eq!(buf, [0, 0, 0, 0]); // length = 0, no data, no padding
    }

    #[test]
    fn xdr_string_encodes_as_var_bytes() {
        let mut buf1 = Vec::new();
        xdr_string(&mut buf1, "test");
        let mut buf2 = Vec::new();
        xdr_var_bytes(&mut buf2, b"test");
        assert_eq!(buf1, buf2);
    }

    // ─── bytes_to_string tests ───────────────────────────────────────

    #[test]
    fn bytes_to_string_valid_utf8() {
        let b = Bytes::from("hello world");
        assert_eq!(bytes_to_string(b), "hello world");
    }

    #[test]
    fn bytes_to_string_valid_utf8_unicode() {
        let b = Bytes::from("日本語テスト");
        assert_eq!(bytes_to_string(b), "日本語テスト");
    }

    #[test]
    fn bytes_to_string_empty() {
        let b = Bytes::new();
        assert_eq!(bytes_to_string(b), "");
    }

    #[test]
    fn bytes_to_string_invalid_utf8_lossy() {
        let b = Bytes::from_static(&[0xFF, 0xFE, 0x68, 0x69]); // invalid + "hi"
        let result = bytes_to_string(b);
        assert!(result.contains("hi"));
        assert!(result.contains('\u{FFFD}')); // replacement character
    }

    // ─── hex_preview tests ───────────────────────────────────────────

    #[test]
    fn hex_preview_short() {
        assert_eq!(hex_preview(&[0xAB, 0xCD, 0xEF]), "ab cd ef");
    }

    #[test]
    fn hex_preview_truncates_at_32() {
        let data: Vec<u8> = (0..64).collect();
        let result = hex_preview(&data);
        assert!(result.ends_with("..."));
        // 32 bytes * 3 chars ("xx ") - 1 trailing space + 3 "..." = ~98 chars
        assert!(!result.contains("20")); // byte 0x20 = 32, should be truncated
    }

    #[test]
    fn hex_preview_empty() {
        assert_eq!(hex_preview(&[]), "");
    }
}
