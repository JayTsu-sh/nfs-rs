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
use crate::dialer::{Dialer, Endpoint};
use crate::error::{NfsError, Result};
use crate::mount::{WriteOutcome, WriteStability};
use crate::{NFSVersion, nfs3, rpc};
use std::sync::Arc;

#[derive(Debug)]
struct Mount3 {
    m: Mount,
    posix_acl: Option<super::acl::Client>,
}

#[async_trait]
impl crate::Mount for Mount3 {
    fn capabilities(&self) -> crate::MountCapabilities {
        crate::MountCapabilities {
            posix_acl: self.posix_acl.is_some(),
            ..Default::default()
        }
    }

    async fn get_posix_acl(&self, fh: Bytes) -> Result<crate::PosixAcl> {
        self.posix_acl
            .as_ref()
            .ok_or_else(|| NfsError::Unsupported("NFSACL v3 extension is unavailable".into()))?
            .get(fh)
            .await
    }

    async fn set_posix_acl(&self, fh: Bytes, acl: &crate::PosixAcl) -> Result<()> {
        self.posix_acl
            .as_ref()
            .ok_or_else(|| NfsError::Unsupported("NFSACL v3 extension is unavailable".into()))?
            .set(fh, acl)
            .await
    }
    fn get_max_read_size(&self) -> u32 {
        self.m.rsize
    }

    fn get_max_write_size(&self) -> u32 {
        self.m.wsize
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

    async fn open_path_with_options(
        &self,
        path: &str,
        options: crate::OpenOptions,
    ) -> Result<crate::OpenFile> {
        crate::mount::open_path_guarded(self, path, options, |parent, name| async move {
            self.m
                .create_guarded(parent, &name, options.create_mode)
                .await
                .map(crate::OpenFile::from_object)
        })
        .await
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

    async fn readdirplus_page(
        &self,
        dir_fh: Bytes,
        position: crate::DirectoryCookie,
    ) -> Result<crate::ReaddirplusPage> {
        self.m.readdirplus_page(dir_fh, position).await
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
    dialer: &Arc<dyn Dialer>,
    addrs: &[Endpoint],
    port: u16,
    prog: u32,
    vers: u32,
    auth: &crate::Auth,
    max_retries: usize,
) -> Result<u16> {
    if port != 0 {
        return Ok(port);
    }
    rpc::portmap(dialer, addrs, prog, vers, auth, max_retries).await
}

pub(crate) async fn mount(args: &crate::MountArgs) -> Result<Box<dyn crate::Mount>> {
    // start by resolving host address and assigning portmapper port to each resolved address
    let dialer = args.dialer();
    let addrs = dialer.resolve(&args.host, rpc::PORTMAP_PORT).await?;
    debug!(host = %args.host, addr_count = addrs.len(), "resolved NFS server addresses");
    let auth = crate::Auth::new_unix("nfs-rs", args.uid, args.gid);
    // Run both portmapper queries concurrently — they are independent connections.
    let (nfsport, mountport) = tokio::try_join!(
        ensure_port(
            &dialer,
            &addrs,
            args.nfsport,
            rpc::NFS_PROG,
            rpc::NFS3_VERSION,
            &auth,
            MOUNT_RETRIES,
        ),
        ensure_port(
            &dialer,
            &addrs,
            args.mountport,
            rpc::MOUNT_PROG,
            rpc::MOUNT3_VERSION,
            &auth,
            MOUNT_RETRIES,
        ),
    )?;
    info!(nfsport, mountport, "ports resolved");
    let mut last_error = None;
    for addr in addrs {
        // replace portmapper port with NFS port obtained above
        let addr = addr.with_port(nfsport);
        match mount_on_addr(&dialer, &addr, args, &auth, mountport).await {
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
    dialer: &Arc<dyn Dialer>,
    addr: &Endpoint,
    args: &crate::MountArgs,
    auth: &crate::Auth,
    mountport: u16,
) -> Result<Box<dyn crate::Mount>> {
    info!(addr = %addr, dirpath = %args.dirpath, "connecting to NFS server for mount");
    let nfs_mux = rpc::StreamMux::dial(Arc::clone(dialer), addr.clone()).await?;
    let dir: String = args.dirpath.to_owned();
    let mount_mux = if mountport != addr.port() {
        Some(rpc::StreamMux::dial(Arc::clone(dialer), addr.with_port(mountport)).await?)
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
        dircount: crate::mount::DEFAULT_READDIR_BUFFER,
        maxcount: crate::mount::DEFAULT_READDIR_BUFFER,
        rsize: 0,
        wsize: 0,
    };
    // NFS NULL is skipped: the FSINFO call below already validates the connection.
    let fsinfo_ok = m
        ._fsinfo(nfs3::FSINFO3args {
            fsroot: nfs3::nfs_fh3 { data: m.getfh() },
        })
        .await?;
    let fsinfo = crate::mount::FSInfo::from(fsinfo_ok);
    m.rsize = crate::mount::negotiated_io_size(u64::from(fsinfo.rtmax))?;
    m.wsize = crate::mount::negotiated_io_size(u64::from(fsinfo.wtmax))?;
    (m.dircount, m.maxcount) =
        crate::mount::directory_reply_limits(args.readdir_buffer, Some(m.rsize));
    info!(
        rsize = m.rsize,
        wsize = m.wsize,
        rtmax = fsinfo.rtmax,
        wtmax = fsinfo.wtmax,
        dircount = m.dircount,
        maxcount = m.maxcount,
        "NFS mount complete, negotiated transfer sizes"
    );

    let posix_acl = super::acl::Client::discover(dialer, addr, &m.rpc, auth).await;
    Ok(Box::new(Mount3 { m, posix_acl }))
}

/// Query the MOUNT service and return all exported file systems — the `showmount -e` equivalent.
///
/// Resolves the MOUNT port via portmapper (unless `args.mountport` is non-zero), then calls
/// the MOUNT EXPORT procedure (5) and decodes the XDR linked-list response.
pub(crate) async fn query_exports(
    args: &crate::MountArgs,
) -> Result<Vec<crate::mount::ExportEntry>> {
    let dialer = args.dialer();
    let addrs = dialer.resolve(&args.host, rpc::PORTMAP_PORT).await?;
    let auth = crate::Auth::new_unix("nfs-rs", args.uid, args.gid);
    let mountport = ensure_port(
        &dialer,
        &addrs,
        args.mountport,
        rpc::MOUNT_PROG,
        rpc::MOUNT3_VERSION,
        &auth,
        MOUNT_RETRIES,
    )
    .await?;
    for addr in addrs {
        let addr = addr.with_port(mountport);
        let res = query_exports_on_addr(&dialer, &addr, &auth, MOUNT_RETRIES).await;
        if res.is_ok() {
            return res;
        }
    }
    Err(NfsError::Rpc("no valid socket address".to_string()))
}

async fn query_exports_on_addr(
    dialer: &Arc<dyn Dialer>,
    addr: &Endpoint,
    auth: &crate::Auth,
    max_retries: usize,
) -> Result<Vec<crate::mount::ExportEntry>> {
    let mux = rpc::StreamMux::dial(Arc::clone(dialer), addr.clone()).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialer::ScriptedDialer;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn guarded_open_options_reuse_parent_and_preserve_raced_file_attributes() {
        use crate::Mount as _;
        use crate::nfs4::compound::{take_opaque, xdr_opaque};
        use bytes::Buf;
        for raced in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let rpc = rpc::Client::new(
                rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
                    .await
                    .unwrap(),
                None,
            );
            let mount = Mount3 {
                m: Mount {
                    rpc,
                    auth: crate::Auth::new_null(),
                    fh: Bytes::from_static(b"root"),
                    dir: "/".into(),
                    dircount: 8192,
                    maxcount: 8192,
                    rsize: 4096,
                    wsize: 4096,
                },
                posix_acl: None,
            };
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let expected = if raced {
                    vec![3, 3, 8, 3, 2]
                } else {
                    vec![3, 3, 8]
                };
                for (index, procedure) in expected.iter().enumerate() {
                    let request = read_record(&mut stream).await.unwrap();
                    assert_eq!(
                        u32::from_be_bytes(request[20..24].try_into().unwrap()),
                        *procedure
                    );
                    let mut args = Bytes::copy_from_slice(&request[40..]); // AUTH_NULL RPC header
                    let fh = take_opaque(&mut args, "fh").unwrap();
                    assert_eq!(
                        fh.as_ref(),
                        if index == 0 {
                            b"root".as_slice()
                        } else if *procedure == 2 {
                            b"file".as_slice()
                        } else {
                            b"parent".as_slice()
                        }
                    );
                    let mut payload = vec![];
                    if *procedure == 3 {
                        let name = take_opaque(&mut args, "name").unwrap();
                        assert_eq!(
                            name.as_ref(),
                            if index == 0 {
                                b"parent".as_slice()
                            } else {
                                b"new"
                            }
                        );
                        if index == 1 {
                            payload.extend_from_slice(&2u32.to_be_bytes());
                            payload.extend_from_slice(&0u32.to_be_bytes());
                        } else {
                            payload.extend_from_slice(&0u32.to_be_bytes());
                            xdr_opaque(&mut payload, if index == 0 { b"parent" } else { b"file" });
                            payload.extend_from_slice(&[0; 8]); // object + directory attributes absent
                        }
                    } else if *procedure == 8 {
                        assert_eq!(take_opaque(&mut args, "name").unwrap().as_ref(), b"new");
                        assert_eq!(args.get_u32(), 1, "RFC 1813 GUARDED creation");
                        assert_eq!(args.get_u32(), 1);
                        assert_eq!(args.get_u32(), 0o666);
                        assert_eq!(args.as_ref(), &[0; 20]); // uid/gid/size/atime/mtime unset
                        if raced {
                            payload.extend_from_slice(&17u32.to_be_bytes());
                            payload.extend_from_slice(&[0; 8]);
                        } else {
                            payload.extend_from_slice(&[0; 4]);
                            payload.extend_from_slice(&1u32.to_be_bytes());
                            xdr_opaque(&mut payload, b"file");
                            payload.extend_from_slice(&[0; 12]); // attributes + wcc absent
                        }
                    } else {
                        // Only SIZE=0, never reset the concurrent creator's mode.
                        assert_eq!(args.split_to(12).as_ref(), &[0; 12]);
                        assert_eq!(args.get_u32(), 1);
                        assert_eq!(args.get_u64(), 0);
                        assert_eq!(args.as_ref(), &[0; 12]);
                        payload.extend_from_slice(&[0; 12]);
                    }
                    reply(&mut stream, &request, &payload).await.unwrap();
                }
                // Keep the fixture connected through the caller's local CLOSE.
                let _ = read_record(&mut stream).await;
            });
            let file = mount
                .open_path_with_options(
                    "parent/new",
                    crate::OpenOptions {
                        access: crate::OPEN_WRITE,
                        create_mode: Some(0o666),
                        truncate: true,
                    },
                )
                .await
                .unwrap();
            mount.close_stateful(file).await.unwrap();
            server.abort();
        }
    }

    async fn read_record<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Vec<u8>> {
        let marker = stream.read_u32().await?;
        let mut record = vec![0; (marker & 0x7fff_ffff) as usize];
        stream.read_exact(&mut record).await?;
        Ok(record)
    }

    /// An accepted RPC reply (AUTH_NONE verifier, SUCCESS) to `request` carrying `payload`.
    async fn reply<S: AsyncWrite + Unpin>(
        stream: &mut S,
        request: &[u8],
        payload: &[u8],
    ) -> std::io::Result<()> {
        let mut response = request[0..4].to_vec();
        for word in [1u32, 0, 0, 0, 0] {
            response.extend_from_slice(&word.to_be_bytes());
        }
        response.extend_from_slice(payload);
        stream
            .write_u32(0x8000_0000 | response.len() as u32)
            .await?;
        stream.write_all(&response).await
    }

    /// MNT: status OK, a root handle and no auth flavors.
    fn mnt_ok() -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&4u32.to_be_bytes());
        payload.extend_from_slice(b"root");
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload
    }

    /// FSINFO: status OK, no attributes, `rtmax` and a 64 KiB `wtmax`.
    fn fsinfo_ok(rtmax: u32) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0u32.to_be_bytes()); // NFS3_OK
        payload.extend_from_slice(&0u32.to_be_bytes()); // no attributes
        for value in [rtmax, rtmax, 4096, 65536, 65536, 4096, 4096] {
            payload.extend_from_slice(&value.to_be_bytes()); // rt*, wt*, dtpref
        }
        payload.extend_from_slice(&u64::MAX.to_be_bytes()); // maxfilesize
        payload.extend_from_slice(&[0; 8]); // time_delta
        payload.extend_from_slice(&0u32.to_be_bytes()); // properties
        payload
    }

    /// Serves a mount (MNT, FSINFO) and one READDIRPLUS, which it refuses; returns the request's
    /// `dircount` and `maxcount`, the last two words of READDIRPLUS3args (RFC 1813 §3.3.17).
    async fn serve(listener: TcpListener, rtmax: u32) -> std::io::Result<(u32, u32)> {
        let (mut stream, _) = listener.accept().await?;
        let mnt = read_record(&mut stream).await?;
        reply(&mut stream, &mnt, &mnt_ok()).await?;
        let fsinfo = read_record(&mut stream).await?;
        reply(&mut stream, &fsinfo, &fsinfo_ok(rtmax)).await?;
        // Mount now probes the optional NFSACL extension before directory I/O.
        // Refuse that program explicitly so the fixture still tests the next
        // actual READDIRPLUS request, not the capability probe's arguments.
        let probe = read_record(&mut stream).await?;
        assert_eq!(
            u32::from_be_bytes(probe[12..16].try_into().unwrap()),
            rpc::NFSACL_PROG
        );
        assert_eq!(u32::from_be_bytes(probe[16..20].try_into().unwrap()), 3);
        assert_eq!(u32::from_be_bytes(probe[20..24].try_into().unwrap()), 0);
        let mut unsupported = probe[..4].to_vec();
        for word in [1u32, 0, 0, 0, 1] {
            // Accepted RPC reply, PROG_UNAVAIL.
            unsupported.extend_from_slice(&word.to_be_bytes());
        }
        stream
            .write_u32(0x8000_0000 | unsupported.len() as u32)
            .await?;
        stream.write_all(&unsupported).await?;
        let readdirplus = read_record(&mut stream).await?;
        let word = |from_end: usize| {
            let at = readdirplus.len() - from_end;
            u32::from_be_bytes(readdirplus[at..at + 4].try_into().unwrap())
        };
        let sizes = (word(8), word(4));
        let mut refused = 13u32.to_be_bytes().to_vec(); // NFS3ERR_ACCES
        refused.extend_from_slice(&0u32.to_be_bytes()); // no directory attributes
        reply(&mut stream, &readdirplus, &refused).await?;
        Ok(sizes)
    }

    /// A mount through a custom dialer: the server name reaches the dialer
    /// unresolved, and every connection, the optional NFSACL portmapper
    /// lookup included, is opened by it.
    #[tokio::test]
    async fn mount_with_dials_every_connection_through_the_dialer() {
        let dialer = Arc::new(ScriptedDialer::default());
        let mut stream = dialer.accept();
        let server = tokio::spawn(async move {
            let mnt = read_record(&mut stream).await?;
            reply(&mut stream, &mnt, &mnt_ok()).await?;
            let fsinfo = read_record(&mut stream).await?;
            reply(&mut stream, &fsinfo, &fsinfo_ok(65536)).await?;
            let probe = read_record(&mut stream).await?;
            let mut unsupported = probe[..4].to_vec();
            for word in [1u32, 0, 0, 0, 1] {
                // Accepted RPC reply, PROG_UNAVAIL.
                unsupported.extend_from_slice(&word.to_be_bytes());
            }
            stream
                .write_u32(0x8000_0000 | unsupported.len() as u32)
                .await?;
            stream.write_all(&unsupported).await?;
            Ok::<_, std::io::Error>(stream)
        });
        let options = crate::MountOptions::from_url(
            "nfs://nfs.example.internal/export?version=3&nfsport=2049&mountport=2049",
        )
        .unwrap();
        let mount = crate::mount_with(options, dialer.clone()).await.unwrap();
        assert_eq!(mount.get_max_read_size(), 65536);
        let _stream = server.await.unwrap().unwrap();
        assert_eq!(
            dialer.dialed(),
            vec![
                Endpoint::new("nfs.example.internal", 2049),
                // NFSACL is not on the NFS port; its portmapper is asked
                // through the same dialer (refused here, so ACLs stay off).
                Endpoint::new("nfs.example.internal", rpc::PORTMAP_PORT),
            ]
        );
    }

    #[tokio::test]
    async fn list_exports_with_asks_the_portmapper_through_the_dialer() {
        let dialer = Arc::new(ScriptedDialer::default());
        let error = crate::list_exports_with("nfs.example.internal", dialer.clone())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("refused"), "{error}");
        assert_eq!(
            dialer.dialed(),
            vec![Endpoint::new("nfs.example.internal", rpc::PORTMAP_PORT)]
        );
    }

    #[tokio::test]
    async fn mount_with_refuses_retained_delegations() {
        let dialer = Arc::new(ScriptedDialer::default());
        let options = crate::MountOptions::from_url(
            "nfs://nfs.example.internal/export?version=4.0&retain-delegations=true",
        )
        .unwrap();
        let error = crate::mount_with(options, dialer.clone())
            .await
            .unwrap_err();
        assert!(matches!(error, NfsError::InvalidInput(_)), "{error:?}");
        assert!(dialer.dialed().is_empty());
    }

    /// Without `readdir-buffer`, an NFSv3 mount sizes READDIRPLUS `dircount` and `maxcount` from
    /// the FSINFO `rtmax` it negotiated for READ (bounded by the client ceiling); an explicit
    /// `readdir-buffer` is sent as given. Checked on the READDIRPLUS request itself.
    #[tokio::test]
    async fn directory_replies_are_sized_from_fsinfo_rtmax() {
        let ceiling = crate::mount::MAX_IO_SIZE;
        for (readdir_buffer, rtmax, expected) in [
            (None, 1 << 20, (1 << 20, 1 << 20)),
            (None, 32768, (32768, 32768)),
            (None, 64 << 20, (ceiling, ceiling)),
            (Some((2048, 4096)), 1 << 20, (2048, 4096)),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(serve(listener, rtmax));
            let args = crate::MountArgs {
                versions: vec![NFSVersion::NFSv3],
                host: "127.0.0.1".to_string(),
                dirpath: "/export".to_string(),
                mountport: addr.port(),
                nfsport: addr.port(),
                uid: 0,
                gid: 0,
                readdir_buffer,
                noresvport: true,
                retain_delegations: false,
                dialer: None,
            };
            let auth = crate::Auth::new_null();
            let mount = mount_on_addr(&args.dialer(), &addr.into(), &args, &auth, addr.port())
                .await
                .unwrap();
            assert_eq!(mount.get_max_read_size(), rtmax.min(ceiling));
            let refused = mount
                .readdirplus_page(
                    Bytes::from_static(b"root"),
                    crate::DirectoryCookie::default(),
                )
                .await;
            assert!(
                matches!(refused, Err(NfsError::Nfs3(nfs3::ErrorCode::NFS3ERR_ACCES))),
                "{refused:?}"
            );
            assert_eq!(server.await.unwrap().unwrap(), expected, "rtmax {rtmax}");
        }
    }
}
