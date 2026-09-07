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

use super::{METADATA_TIMEOUT, MOUNT_REPLAY, MOUNT_RETRIES};
use bytes::Bytes;
use futures::TryStreamExt;
use tracing::{debug, info, warn};

use async_trait::async_trait;

use super::{
    Mount, MountProc3, ObjRes, Time, encode_dirpath, export::decode_exports, mount_mountstat3,
    mountres3_ok, rpc_header,
};
use crate::error::{NfsError, Result};
use crate::mount::{WriteOutcome, WriteStability, finish_stable_write};
use crate::{NFSVersion, SocketAddr, ToSocketAddrs, nfs3, rpc};

#[derive(Debug)]
struct Mount3 {
    m: Mount,
    io_options: crate::IoOptions,
}

#[async_trait]
impl crate::Mount for Mount3 {
    fn get_max_read_size(&self) -> u32 {
        self.m.rsize
    }

    fn get_max_write_size(&self) -> u32 {
        self.m.wsize
    }

    fn io_options(&self) -> crate::IoOptions {
        self.io_options
    }

    async fn null(&self) -> Result<()> {
        self.m.null().await
    }

    async fn access(&self, fh: Bytes, mode: u32) -> Result<u32> {
        self.m.access(fh, mode).await
    }

    async fn commit(&self, fh: Bytes, offset: u64, count: u32) -> Result<()> {
        self.m.commit(fh, offset, count).await
    }

    async fn create(&self, dir_fh: Bytes, filename: &str, mode: Option<u32>) -> Result<ObjRes> {
        self.m.create(dir_fh, filename, mode).await
    }

    async fn create_path(&self, path: &str, mode: Option<u32>) -> Result<ObjRes> {
        self.m.create_path(path, mode).await
    }

    async fn fsinfo(&self) -> Result<crate::mount::FSInfo> {
        self.m.fsinfo().await
    }

    async fn fsstat(&self) -> Result<crate::mount::FSStat> {
        self.m.fsstat().await
    }

    async fn getattr(&self, fh: Bytes) -> Result<crate::mount::Attr> {
        self.m.getattr(fh).await
    }

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
    ) -> Result<()> {
        self.m
            .setattr(fh, guard_ctime, mode, uid, gid, size, atime, mtime)
            .await
    }

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
    ) -> Result<()> {
        self.m
            .setattr_path(path, specify_guard, mode, uid, gid, size, atime, mtime)
            .await
    }

    async fn getfh(&self) -> Bytes {
        self.m.getfh()
    }

    async fn link(
        &self,
        src_fh: Bytes,
        dst_dir_fh: Bytes,
        dst_filename: &str,
    ) -> Result<crate::mount::Attr> {
        self.m.link(src_fh, dst_dir_fh, dst_filename).await
    }

    async fn link_path(&self, src_path: &str, dst_path: &str) -> Result<crate::mount::Attr> {
        self.m.link_path(src_path, dst_path).await
    }

    async fn symlink(
        &self,
        src_path: &str,
        dst_dir_fh: Bytes,
        dst_filename: &str,
    ) -> Result<ObjRes> {
        self.m.symlink(src_path, dst_dir_fh, dst_filename).await
    }

    async fn symlink_path(&self, src_path: &str, dst_path: &str) -> Result<ObjRes> {
        self.m.symlink_path(src_path, dst_path).await
    }

    async fn readlink(&self, fh: Bytes) -> Result<String> {
        self.m.readlink(fh).await
    }

    async fn lookup(&self, dir_fh: Bytes, filename: &str) -> Result<ObjRes> {
        self.m.lookup(dir_fh, filename).await
    }

    async fn lookup_path(&self, path: &str) -> Result<ObjRes> {
        self.m.lookup_path(path).await
    }

    async fn pathconf(&self, fh: Bytes) -> Result<crate::mount::Pathconf> {
        self.m.pathconf(fh).await
    }

    async fn read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
        self.m.read(fh, offset, count).await
    }

    async fn write(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<WriteOutcome> {
        self.m
            .write_how(fh, offset, data, WriteStability::Unstable)
            .await
    }

    async fn write_stable(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<u32> {
        let outcome = self
            .m
            .write_how(fh.clone(), offset, data, WriteStability::FileSync)
            .await?;
        finish_stable_write(self, fh, offset, outcome).await
    }

    async fn commit_with_verifier(
        &self,
        fh: Bytes,
        offset: u64,
        count: u32,
    ) -> Result<Option<[u8; 8]>> {
        self.m.commit_with_verifier(fh, offset, count).await
    }

    async fn readdir(&self, dir_fh: Bytes) -> crate::mount::ReaddirStream<'_> {
        Box::pin(self.m.readdir(dir_fh).await.map_ok(Into::into))
    }

    async fn readdirplus(&self, dir_fh: Bytes) -> crate::mount::ReaddirplusStream<'_> {
        Box::pin(self.m.readdirplus(dir_fh).await.map_ok(Into::into))
    }

    async fn mkdir(&self, dir_fh: Bytes, dirname: &str, mode: u32) -> Result<ObjRes> {
        self.m.mkdir(dir_fh, dirname, mode).await
    }

    async fn mkdir_path(&self, path: &str, mode: u32) -> Result<ObjRes> {
        self.m.mkdir_path(path, mode).await
    }

    async fn remove(&self, dir_fh: Bytes, filename: &str) -> Result<()> {
        self.m.remove(dir_fh, filename).await
    }

    async fn remove_path(&self, path: &str) -> Result<()> {
        self.m.remove_path(path).await
    }

    async fn rmdir(&self, dir_fh: Bytes, dirname: &str) -> Result<()> {
        self.m.rmdir(dir_fh, dirname).await
    }

    async fn rmdir_path(&self, path: &str) -> Result<()> {
        self.m.rmdir_path(path).await
    }

    async fn rename(
        &self,
        from_dir_fh: Bytes,
        from_filename: &str,
        to_dir_fh: Bytes,
        to_filename: &str,
    ) -> Result<()> {
        self.m
            .rename(from_dir_fh, from_filename, to_dir_fh, to_filename)
            .await
    }

    async fn rename_path(&self, from_path: &str, to_path: &str) -> Result<()> {
        self.m.rename_path(from_path, to_path).await
    }

    async fn umount(&self) -> Result<()> {
        self.m.umount().await
    }

    async fn exports(&self) -> Result<Vec<crate::mount::ExportEntry>> {
        self.m.export().await
    }

    fn version(&self) -> NFSVersion {
        NFSVersion::NFSv3
    }
}

async fn ensure_port(
    addrs: &Vec<SocketAddr>,
    port: u16,
    prog: u32,
    vers: u32,
    auth: &crate::Auth,
    max_retries: usize,
    noresvport: bool,
) -> Result<u16> {
    if port != 0 {
        return Ok(port);
    }
    rpc::portmap(addrs, prog, vers, auth, max_retries, noresvport).await
}

pub(crate) async fn mount(args: &crate::MountArgs) -> Result<Box<dyn crate::Mount>> {
    // start by resolving host address and assigning portmapper port to each resolved address
    let addrs: Vec<SocketAddr> = (args.host.as_str(), rpc::PORTMAP_PORT)
        .to_socket_addrs()?
        .collect();
    debug!(host = %args.host, addr_count = addrs.len(), "resolved NFS server addresses");
    let auth = crate::Auth::new_unix("nfs-rs", args.uid, args.gid);
    // Run both portmapper queries concurrently — they are independent TCP connections.
    let (nfsport, mountport) = tokio::try_join!(
        ensure_port(
            &addrs,
            args.nfsport,
            rpc::NFS_PROG,
            rpc::NFS3_VERSION,
            &auth,
            MOUNT_RETRIES,
            args.noresvport
        ),
        ensure_port(
            &addrs,
            args.mountport,
            rpc::MOUNT_PROG,
            rpc::MOUNT3_VERSION,
            &auth,
            MOUNT_RETRIES,
            args.noresvport
        ),
    )?;
    info!(nfsport, mountport, "ports resolved");
    let mut last_error = None;
    for mut addr in addrs {
        addr.set_port(nfsport); // replace portmapper port with NFS port obtained above
        match mount_on_addr(&addr, args, &auth, mountport).await {
            Ok(mount) => return Ok(mount),
            Err(e) => {
                warn!(addr = %addr, error = %e, "mount attempt failed on address");
                last_error = Some(e);
                continue;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| NfsError::Rpc("no valid socket address".to_string())))
}

async fn mount_on_addr(
    addr: &SocketAddr,
    args: &crate::MountArgs,
    auth: &crate::Auth,
    mountport: u16,
) -> Result<Box<dyn crate::Mount>> {
    info!(addr = %addr, dirpath = %args.dirpath, "connecting to NFS server for mount");
    let nfs_mux = rpc::StreamMux::connect(*addr, args.noresvport).await?;
    let dir: String = args.dirpath.to_owned();
    let (dircount, maxcount) = (args.dircount, args.maxcount);
    let (rsize, wsize) = (args.rsize, args.wsize);
    let (rsize_max, wsize_max) = (4194304, 4194304); // XXX: according to libnfs, maximum read/write size is 4 MiB
    let (rsize_min, wsize_min) = (8192, 8192); // XXX: according to libnfs, minimum read/write size is 8 KiB
    let mount_mux = if mountport != addr.port() {
        let mut mount_addr = *addr;
        mount_addr.set_port(mountport);
        Some(rpc::StreamMux::connect(mount_addr, args.noresvport).await?)
    } else {
        None
    };
    let client = rpc::Client::new(nfs_mux, mount_mux);

    // MOUNT MNT — encode dirpath, decode mountres3.
    // MOUNT NULL is skipped: RFC 1813 does not require it before MNT.
    let mut buf = Vec::with_capacity(128);
    rpc_header(
        rpc::MOUNT_PROG,
        rpc::MOUNT3_VERSION,
        MountProc3::Mount as u32,
        auth,
    )
    .encode(&mut buf);
    encode_dirpath(&mut buf, dir.trim_end_matches('/'));
    // Mount phase: use small retry count for fast failure detection.
    let mut bytes = client.call(buf, MOUNT_REPLAY, METADATA_TIMEOUT).await?;
    let status =
        mount_mountstat3::try_from(&mut bytes).map_err(|e| NfsError::Xdr(e.to_string()))?;
    match status {
        mount_mountstat3::MNT3_OK => {}
        e => return Err(NfsError::Mount(e)),
    }
    let ok = mountres3_ok::try_from(&mut bytes).map_err(|e| NfsError::Xdr(e.to_string()))?;
    let fh = ok.fhandle.0;
    info!(addr = %addr, dirpath = %args.dirpath, fh_len = fh.len(), "MOUNT MNT succeeded, got root file handle");

    let mut m = Mount {
        rpc: client,
        auth: auth.clone(),
        fh,
        dir,
        dircount,
        maxcount,
        rsize,
        wsize,
    };
    // NFS NULL is skipped: the FSINFO call below already validates the connection.
    let fsinfo_ok = m
        ._fsinfo(nfs3::FSINFO3args {
            fsroot: nfs3::nfs_fh3 { data: m.getfh() },
        })
        .await?;
    let fsinfo = crate::mount::FSInfo::from(fsinfo_ok);
    m.rsize = fsinfo.rtmax.min(m.rsize).min(rsize_max).max(rsize_min);
    m.wsize = fsinfo.wtmax.min(m.wsize).min(wsize_max).max(wsize_min);
    info!(
        rsize = m.rsize,
        wsize = m.wsize,
        rtmax = fsinfo.rtmax,
        wtmax = fsinfo.wtmax,
        "NFS mount complete, negotiated transfer sizes"
    );

    Ok(Box::new(Mount3 {
        m,
        io_options: args.io_options,
    }))
}

/// Query the MOUNT service and return all exported file systems — the `showmount -e` equivalent.
///
/// Resolves the MOUNT port via portmapper (unless `args.mountport` is non-zero), then calls
/// the MOUNT EXPORT procedure (5) and decodes the XDR linked-list response.
pub(crate) async fn query_exports(
    args: &crate::MountArgs,
) -> Result<Vec<crate::mount::ExportEntry>> {
    let addrs: Vec<SocketAddr> = (args.host.as_str(), rpc::PORTMAP_PORT)
        .to_socket_addrs()?
        .collect();
    let auth = crate::Auth::new_unix("nfs-rs", args.uid, args.gid);
    let mountport = ensure_port(
        &addrs,
        args.mountport,
        rpc::MOUNT_PROG,
        rpc::MOUNT3_VERSION,
        &auth,
        MOUNT_RETRIES,
        args.noresvport,
    )
    .await?;
    for mut addr in addrs {
        addr.set_port(mountport);
        let res = query_exports_on_addr(&addr, &auth, MOUNT_RETRIES, args.noresvport).await;
        if res.is_ok() {
            return res;
        }
    }
    Err(NfsError::Rpc("no valid socket address".to_string()))
}

async fn query_exports_on_addr(
    addr: &SocketAddr,
    auth: &crate::Auth,
    max_retries: usize,
    noresvport: bool,
) -> Result<Vec<crate::mount::ExportEntry>> {
    let mux = rpc::StreamMux::connect(*addr, noresvport).await?;
    let client = rpc::Client::new(mux, None);
    let mut buf = Vec::with_capacity(128);
    rpc_header(
        rpc::MOUNT_PROG,
        rpc::MOUNT3_VERSION,
        MountProc3::Export as u32,
        auth,
    )
    .encode(&mut buf);
    let mut bytes = client
        .call(
            buf,
            crate::rpc::ReplayPolicy::byte_identical(max_retries),
            METADATA_TIMEOUT,
        )
        .await?;
    decode_exports(&mut bytes)
}
