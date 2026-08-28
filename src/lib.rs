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

//! An asynchronous, pure Rust client library for NFSv3 and NFSv4.1.
//!
//! Start with [`parse_url_and_mount`] to connect to an NFS export. The returned
//! [`Mount`] trait object provides file, directory, metadata, link, and
//! filesystem operations shared across the supported protocol versions.
//!
//! ```no_run
//! use bytes::Bytes;
//! use nfs_rs::{OPEN_READ, Result, parse_url_and_mount};
//!
//! # async fn example() -> Result<()> {
//! let mount = parse_url_and_mount(
//!     "nfs://127.0.0.1/some/export?version=4.1&noresvport=true",
//! )
//! .await?;
//!
//! let created = mount.create_path("hello.txt", Some(0o644)).await?;
//! mount
//!     .write(created.fh.clone(), 0, Bytes::from_static(b"hello NFS"))
//!     .await?;
//! mount.commit(created.fh.clone(), 0, 9).await?;
//! mount.close(created.fh).await?;
//!
//! let opened = mount.open_path("hello.txt", OPEN_READ).await?;
//! let contents = mount.read(opened.fh.clone(), 0, 9).await?;
//! mount.close(opened.fh).await?;
//! assert_eq!(&contents[..], b"hello NFS");
//!
//! mount.umount().await?;
//! # Ok(())
//! # }
//! ```
//!
//! By default the client binds a privileged source port. Add
//! `noresvport=true` when the server export accepts non-privileged source
//! ports.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use tokio::net::{TcpSocket, TcpStream};

fn privileged_port_candidates(ports: &[u16], start: usize) -> Vec<u16> {
    (0..ports.len())
        .map(|offset| ports[(start + offset) % ports.len()])
        .collect()
}

fn is_privileged_port_collision(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::AddrInUse | std::io::ErrorKind::AddrNotAvailable
    )
}

pub(crate) async fn connect_to_target(addr: &SocketAddr, noresvport: bool) -> Result<TcpStream> {
    // Bind to a random privileged port (< 1024), required by some NFS servers.
    // Exclude well-known service ports to avoid conflicts.
    const WELL_KNOWN_PORTS: &[u16] = &[
        1,   // tcpmux
        7,   // echo
        9,   // discard
        11,  // systat
        13,  // daytime
        15,  // netstat
        20,  // ftp-data
        21,  // ftp
        22,  // ssh
        23,  // telnet
        25,  // smtp
        37,  // time
        42,  // nameserver
        43,  // whois
        49,  // tacacs
        53,  // dns
        67,  // dhcp server
        68,  // dhcp client
        69,  // tftp
        70,  // gopher
        79,  // finger
        80,  // http
        88,  // kerberos
        102, // iso-tsap
        110, // pop3
        111, // rpcbind/portmapper
        119, // nntp
        123, // ntp
        135, // msrpc
        137, // netbios-ns
        138, // netbios-dgm
        139, // netbios-ssn
        143, // imap
        161, // snmp
        162, // snmp-trap
        179, // bgp
        389, // ldap
        427, // svrloc
        443, // https
        445, // microsoft-ds
        464, // kpasswd
        465, // smtps
        514, // syslog
        515, // printer
        520, // rip
        530, // rpc
        543, // klogin
        544, // kshell
        546, // dhcpv6-client
        547, // dhcpv6-server
        548, // afp
        554, // rtsp
        587, // submission
        593, // http-rpc-epmap
        631, // ipp
        636, // ldaps
        873, // rsync
        990, // ftps
        993, // imaps
        995, // pop3s
    ];
    let local_addr_base = if addr.is_ipv4() {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    } else {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
    };
    // noresvport=true: 让 OS 选临时端口（Windows ~49152-65535, Linux ~32768-60999）。
    // 避开 ~960 个特权端口的耗尽问题，要求 server 端启用 insecure（NFSv3 export 选项）。
    if noresvport {
        let socket = if addr.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        socket.set_reuseaddr(true)?;
        socket.bind(local_addr_base)?;
        let stream = socket.connect(*addr).await?;
        stream.set_nodelay(true)?;
        const KEEPALIVE_TIME_SECS: u64 = 30;
        const KEEPALIVE_INTERVAL_SECS: u64 = 5;
        #[cfg(target_os = "linux")]
        const KEEPALIVE_RETRIES: u32 = 3;
        let sock_ref = socket2::SockRef::from(&stream);
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(std::time::Duration::from_secs(KEEPALIVE_TIME_SECS))
            .with_interval(std::time::Duration::from_secs(KEEPALIVE_INTERVAL_SECS));
        #[cfg(target_os = "linux")]
        let keepalive = keepalive.with_retries(KEEPALIVE_RETRIES);
        sock_ref.set_tcp_keepalive(&keepalive)?;
        info!(
            addr = %addr,
            local_port = stream.local_addr().map(|a| a.port()).unwrap_or(0),
            "TCP connection established (ephemeral source port, noresvport)"
        );
        return Ok(stream);
    }
    // 预计算可用端口列表（1-1023 排除已知端口），约 960 个可用
    let available_ports: Vec<u16> = (1..1024u16)
        .filter(|p| !WELL_KNOWN_PORTS.contains(p))
        .collect();
    // Start at a random point, then visit every candidate exactly once. With
    // SO_REUSEADDR, bind may succeed for a port already used by another
    // outbound socket; Linux reports the duplicate four-tuple from connect as
    // EADDRNOTAVAIL, while Windows may report EADDRINUSE.
    let start = rand::random_range(0..available_ports.len());
    let candidates = privileged_port_candidates(&available_ports, start);
    let mut last_collision = None;
    for (attempt, local_port) in candidates.iter().copied().enumerate() {
        let socket = if addr.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        // 允许 bind 到处于 TIME_WAIT 状态的端口，避免重复运行时端口耗尽
        socket.set_reuseaddr(true)?;
        let mut local_addr = local_addr_base;
        local_addr.set_port(local_port);
        match socket.bind(local_addr) {
            Ok(_) => {}
            Err(e) if is_privileged_port_collision(&e) => {
                trace!(
                    local_port,
                    error = %e,
                    "source port bind collision, trying another"
                );
                last_collision = Some(e);
                continue;
            }
            Err(e) => return Err(e.into()),
        }
        debug!(local_port = local_addr.port(), addr = %addr, "bound to local port, connecting");
        match socket.connect(*addr).await {
            Ok(stream) => {
                stream.set_nodelay(true)?;
                const KEEPALIVE_TIME_SECS: u64 = 30;
                const KEEPALIVE_INTERVAL_SECS: u64 = 5;
                #[cfg(target_os = "linux")]
                const KEEPALIVE_RETRIES: u32 = 3;

                let sock_ref = socket2::SockRef::from(&stream);
                let keepalive = socket2::TcpKeepalive::new()
                    .with_time(std::time::Duration::from_secs(KEEPALIVE_TIME_SECS))
                    .with_interval(std::time::Duration::from_secs(KEEPALIVE_INTERVAL_SECS));
                #[cfg(target_os = "linux")]
                let keepalive = keepalive.with_retries(KEEPALIVE_RETRIES);
                sock_ref.set_tcp_keepalive(&keepalive)?;
                info!(addr = %addr, local_port = local_addr.port(), "TCP connection established");
                return Ok(stream);
            }
            Err(e) if is_privileged_port_collision(&e) => {
                debug!(
                    local_port,
                    addr = %addr,
                    attempt,
                    error = %e,
                    "connect found a privileged-port tuple collision, trying another port"
                );
                last_collision = Some(e);
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }
    let attempted = candidates.len();
    let last_collision = last_collision.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "no privileged source-port candidate was available",
        )
    });
    warn!(addr = %addr, attempted, error = %last_collision, "all privileged source ports failed");
    Err(NfsError::Io(std::io::Error::new(
        last_collision.kind(),
        format!(
            "failed to connect to {addr} after trying {attempted} privileged source ports; last error: {last_collision}"
        ),
    )))
}

pub mod error;
mod mount;
mod nfs3;
mod nfs4;
mod nfs40;
mod nfs41;
mod rpc;
mod shared;

pub use error::{
    NfsError, OperationClass, OperationOutcome, OperationOutcomeError, RecoveryAction,
    RequestContext, RequestId, RequestTransmission, Result,
};
pub use mount::{
    AceFlags, AceMask, AceType, Acl, AclSupport, Attr, CallbackStats, ExportEntry, FSInfo, FSStat,
    LockToken, Mount, MountCapabilities, MountHealth, MountLifecycleState, NFSVersion,
    Nfs41CallbackStats, Nfs41ChannelLimits, NfsAce, OPEN_BOTH, OPEN_READ, OPEN_WRITE, ObjRes,
    OpenFile, Pathconf, PathconfSupport, ReaddirEntry, ReaddirStream, ReaddirplusEntry,
    ReaddirplusStream, SupportedPathconf,
};
pub use shared::Time;
// 公开 NFS 错误码类型，供外部 crate 进行错误匹配
pub use nfs3::ErrorCode as Nfs3ErrorCode;
pub use nfs3::MountErrorCode as Nfs3MountErrorCode;
pub use nfs4::Nfs4ErrorCode;
// Decoder wrappers exposed solely for `benches/`. Not part of the stable API;
// `#[doc(hidden)]` keeps it out of rustdoc. Internal XDR types stay private —
// only `bytes::Bytes` and the public `NfsError` cross the boundary.
#[doc(hidden)]
pub mod __bench {
    use crate::error::{NfsError, Result};
    use crate::nfs3::fastxdr::{READ3resok, READDIRPLUS3resok, fattr3, post_op_attr};
    use bytes::Bytes;

    fn map_xdr_err<E: std::fmt::Display>(e: E) -> NfsError {
        NfsError::Xdr(e.to_string())
    }

    /// Decode a single `fattr3` (84 wire bytes).
    pub fn decode_fattr3(mut buf: Bytes) -> Result<()> {
        fattr3::try_from(&mut buf).map(|_| ()).map_err(map_xdr_err)
    }

    /// Decode a `post_op_attr` (4-byte discriminant + optional `fattr3`).
    pub fn decode_post_op_attr(mut buf: Bytes) -> Result<()> {
        post_op_attr::try_from(&mut buf)
            .map(|_| ())
            .map_err(map_xdr_err)
    }

    /// Decode a `READ3resok` (post_op_attr + count + eof + variable data slice).
    pub fn decode_read3resok(mut buf: Bytes) -> Result<()> {
        READ3resok::try_from(&mut buf)
            .map(|_| ())
            .map_err(map_xdr_err)
    }

    /// Decode a `READDIRPLUS3resok` page (variable number of entries).
    pub fn decode_readdirplus3resok(mut buf: Bytes) -> Result<()> {
        READDIRPLUS3resok::try_from(&mut buf)
            .map(|_| ())
            .map_err(map_xdr_err)
    }
}

use rpc::auth::Auth;
use tracing::{debug, info, trace, warn};
use url::Url;

#[derive(Debug)]
struct MountArgs {
    versions: Vec<NFSVersion>,
    host: String,
    dirpath: String,
    mountport: u16,
    nfsport: u16,
    uid: u32,
    gid: u32,
    dircount: u32,
    maxcount: u32,
    rsize: u32,
    wsize: u32,
    noresvport: bool,
    retain_delegations: bool,
}

/// Parses the specified URL and attempts to mount the relevant NFS export
///
/// # Example
///
/// ```no_run
/// async fn example() {
///     let mount = nfs_rs::parse_url_and_mount("nfs://127.0.0.1/some/export?version=4.1,4.0,3").await.unwrap();
///     if let Some(res) = mount.create_path("nfs-rs.txt", Some(0o664)).await.ok() {
///         let contents = "hello rust".as_bytes().to_vec();
///         let contents_len = contents.len();
///         let num_bytes_written = mount.write(res.fh.clone(), 0, contents.clone().into()).await.unwrap_or_default();
///         assert_eq!(num_bytes_written as usize, contents_len);
///         let bytes_read = mount.read(res.fh, 0, 16).await.unwrap_or_default();
///         assert_eq!(&bytes_read, "hello rust".as_bytes());
///     }
/// }
/// ```
pub async fn parse_url_and_mount(url: &str) -> Result<Box<dyn Mount>> {
    mount(parse_url(url)?).await
}

/// Queries the MOUNT service on the given host and returns all exported file systems.
///
/// Equivalent to running `showmount -e HOST`. Does not require an existing NFS mount.
///
/// `host` may be a bare hostname or IP address (e.g. `"192.168.1.1"`) or a full
/// `nfs://` URL (query params `uid`, `gid`, and `mountport` are honoured; the path
/// and `version` are ignored).
///
/// # Example
///
/// ```no_run
/// async fn example() {
///     let exports = nfs_rs::list_exports("192.168.1.1").await.unwrap();
///     for e in exports {
///         println!("{} {:?}", e.path, e.groups);
///     }
/// }
/// ```
pub async fn list_exports(host: &str) -> Result<Vec<ExportEntry>> {
    let url = if host.starts_with("nfs://") {
        host.to_string()
    } else {
        format!("nfs://{}/.", host)
    };
    nfs3::query_exports(&parse_url(&url)?).await
}

fn get_uid_gid() -> (u32, u32) {
    #[cfg(not(unix))]
    let uid_gid = || (65534, 65534);
    #[cfg(unix)]
    // SAFETY: getuid() and getgid() are always-safe POSIX calls with no preconditions.
    let uid_gid = || unsafe { (nix::libc::getuid(), nix::libc::getgid()) };
    uid_gid()
}

fn parse_url(url: &str) -> Result<MountArgs> {
    let mut parsed_url =
        Url::parse_with_params(url, &[("version", "3"), ("readdir-buffer", "8192,8192")])
            .map_err(|e| NfsError::InvalidInput(e.to_string()))?;
    if parsed_url.scheme() != "nfs" {
        return Err(NfsError::InvalidInput(
            "specified URL does not have scheme nfs".to_string(),
        ));
    }
    if !parsed_url.has_host() {
        return Err(NfsError::InvalidInput(
            "specified URL does not contain a host".to_string(),
        ));
    }
    let addr_port = parsed_url.port();
    parsed_url
        .set_port(None)
        .map_err(|_| NfsError::InvalidInput("cannot clear port on URL".to_string()))?;
    let version_str = parsed_url
        .query_pairs()
        .find(|(name, _)| name == "version")
        .ok_or_else(|| NfsError::InvalidInput("missing version parameter".to_string()))?
        .1;
    let mut versions = Vec::new();
    for v in version_str.split(',') {
        let version: NFSVersion = v.into();
        match version {
            NFSVersion::Unknown => {
                return Err(NfsError::InvalidInput(
                    "specified URL contains bad NFS version".to_string(),
                ));
            }
            _ => versions.push(version),
        }
    }
    if versions.is_empty() {
        versions.push(NFSVersion::NFSv4p1);
        versions.push(NFSVersion::NFSv3);
    }
    let (uid_def, gid_def) = get_uid_gid();
    let uid = get_url_query_param(
        &parsed_url,
        "uid",
        uid_def,
        "specified URL contains bad UID",
    )?;
    let gid = get_url_query_param(
        &parsed_url,
        "gid",
        gid_def,
        "specified URL contains bad GID",
    )?;
    let readdir_buffer_str = parsed_url
        .query_pairs()
        .find(|(name, _)| name == "readdir-buffer")
        .ok_or_else(|| NfsError::InvalidInput("missing readdir-buffer parameter".to_string()))?
        .1;
    let (dircount, maxcount): (u32, u32) = parse_readdir_buffer_query_param(&readdir_buffer_str)?;
    let nfsport = get_url_query_param(
        &parsed_url,
        "nfsport",
        addr_port.unwrap_or_default(),
        "specified URL contains bad NFS port",
    )?;
    let mountport = get_url_query_param(
        &parsed_url,
        "mountport",
        Default::default(),
        "specified URL contains bad mount port",
    )?;
    let txsize_def: u32 = 1048576; // mimic libnfs default of 1 MiB
    let rsize = get_url_query_param(
        &parsed_url,
        "rsize",
        txsize_def,
        "specified URL contains bad max read size value",
    )?;
    let wsize = get_url_query_param(
        &parsed_url,
        "wsize",
        txsize_def,
        "specified URL contains bad max write size value",
    )?;
    let noresvport = get_url_query_param(
        &parsed_url,
        "noresvport",
        false,
        "specified URL contains bad noresvport value",
    )?;
    let retain_delegations = get_url_query_param(
        &parsed_url,
        "retain-delegations",
        false,
        "specified URL contains bad retain-delegations value",
    )?;
    let host = parsed_url.host_str().unwrap_or_default().to_string();
    Ok(MountArgs {
        versions,
        host,
        mountport,
        nfsport,
        dirpath: parsed_url.path().to_string(),
        uid,
        gid,
        dircount,
        maxcount,
        rsize,
        wsize,
        noresvport,
        retain_delegations,
    })
}

fn get_url_query_param<T: std::str::FromStr>(
    url: &url::Url,
    name: &str,
    def: T,
    err_msg: &str,
) -> Result<T> {
    match url.query_pairs().find(|(n, _)| n == name) {
        Some((_, val)) => val
            .parse()
            .map_err(|_| NfsError::InvalidInput(err_msg.to_string())),
        None => Ok(def),
    }
}

fn parse_readdir_buffer_query_param(param: &str) -> Result<(u32, u32)> {
    if let Some((dircount_str, maxcount_str)) = param.split_once(',') {
        let dircount: u32 = dircount_str.parse().map_err(|_| {
            NfsError::InvalidInput("specified URL contains bad readdir-buffer value".to_string())
        })?;
        let maxcount: u32 = maxcount_str.parse().map_err(|_| {
            NfsError::InvalidInput("specified URL contains bad readdir-buffer value".to_string())
        })?;
        Ok((dircount, maxcount))
    } else {
        let count: u32 = param.parse().map_err(|_| {
            NfsError::InvalidInput("specified URL contains bad readdir-buffer value".to_string())
        })?;
        Ok((count, count))
    }
}

async fn mount(args: MountArgs) -> Result<Box<dyn Mount>> {
    let mut errs: Vec<NfsError> = Vec::new();
    for version in &args.versions {
        info!(version = ?version, host = %args.host, dirpath = %args.dirpath, "attempting NFS mount");
        let res: Result<Box<dyn Mount>> = match version {
            NFSVersion::NFSv3 => nfs3::mount(&args).await,
            NFSVersion::NFSv4p1 => nfs41::mount::mount(&args).await,
            NFSVersion::NFSv4p0 => nfs40::mount(&args).await,
            #[allow(deprecated)]
            NFSVersion::NFSv4 => Err(NfsError::Unsupported(
                "NFSv4.0 is not supported".to_string(),
            )),
            NFSVersion::NFSv4p2 => Err(NfsError::Unsupported(
                "NFSv4.2 is not supported".to_string(),
            )),
            _ => unreachable!(),
        };
        match res {
            Ok(_) => return res,
            Err(err) => {
                warn!(version = ?version, error = %err, "mount attempt failed for version");
                errs.push(err);
            }
        }
    }
    Err(squash_mount_errors(errs))
}

/// Extract the inner message from an NfsError without the variant prefix.
fn nfs_error_msg(err: &NfsError) -> String {
    match err {
        NfsError::Io(e) => e.to_string(),
        NfsError::Nfs3(c) => c.to_string(),
        NfsError::Nfs4(c) => c.to_string(),
        NfsError::LockDenied { .. } => err.to_string(),
        NfsError::Mount(c) => c.to_string(),
        NfsError::Rpc(s)
        | NfsError::Xdr(s)
        | NfsError::Unsupported(s)
        | NfsError::InvalidInput(s) => s.clone(),
        NfsError::RdattrError(code) => format!("rdattr_error: nfsstat4 {}", code),
        NfsError::OperationOutcome(error) => error.to_string(),
    }
}

fn squash_mount_errors(errs: Vec<NfsError>) -> NfsError {
    let mut unsupported_err = "".to_string();
    let mut errs: Vec<NfsError> = errs
        .into_iter()
        .filter_map(|err| {
            if matches!(&err, NfsError::Unsupported(_)) {
                let msg = nfs_error_msg(&err);
                if unsupported_err.is_empty() {
                    unsupported_err = msg;
                } else if unsupported_err != msg {
                    unsupported_err = "NFSv4.0 and NFSv4.2 are not supported".to_string();
                }
                None
            } else {
                Some(err)
            }
        })
        .collect();
    if errs.is_empty() {
        return NfsError::Unsupported(unsupported_err);
    }
    if errs.len() == 1 && unsupported_err.is_empty() {
        return errs.remove(0);
    }
    let mut msg = nfs_error_msg(&errs[0]);
    for err in &errs[1..] {
        msg = format!("{} - {}", msg, nfs_error_msg(err));
    }
    if !unsupported_err.is_empty() {
        msg = format!("{} - {}", msg, unsupported_err);
    }
    NfsError::Rpc(msg)
}

fn split_path(path: &str) -> Result<(String, String)> {
    let cleaned = path_clean::clean(format!("/=/{}", path));
    if !cleaned.starts_with("/=/") {
        return Err(NfsError::InvalidInput("invalid path specified".to_string()));
    }
    if cleaned.eq(std::path::Path::new("/=/")) {
        return Ok(("/".to_string(), "".to_string()));
    }
    let dir = cleaned
        .parent()
        .map(|x| {
            let path_str = x.to_string_lossy();
            let trimmed = &path_str[2..];
            #[cfg(windows)]
            {
                // Windows platform: replace backslashes with forward slashes
                trimmed.replace('\\', "/")
            }
            #[cfg(not(windows))]
            {
                // Non-Windows platform: keep original separators
                trimmed.to_string()
            }
        })
        .ok_or_else(|| NfsError::InvalidInput("invalid path specified".to_string()))?;

    let name = cleaned
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    Ok((dir, name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_url_bad_scheme() {
        for scheme in ["ftp", "scp", "ssh"] {
            let res = parse_url(&format!("{}://localhost/some/export/path", scheme));
            assert!(res.is_err());
            let err = res.unwrap_err();
            assert!(
                matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL does not have scheme nfs")
            );
        }
    }

    #[test]
    fn parse_url_missing_host() {
        let res = parse_url("nfs:///some/export/path");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL does not contain a host")
        );
    }

    #[test]
    fn parse_url_with_bad_version() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?version=5");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad NFS version")
        );
    }

    #[test]
    fn parse_url_requires_exact_nfsv40_minor_version() {
        let exact = parse_url("nfs://127.0.0.1/export?version=4.0").unwrap();
        assert_eq!(exact.versions, vec![NFSVersion::NFSv4p0]);

        let ambiguous = parse_url("nfs://127.0.0.1/export?version=4").unwrap_err();
        assert!(matches!(ambiguous, NfsError::InvalidInput(_)));
    }

    #[test]
    fn parse_url_preserves_explicit_version_fallback_order() {
        let args = parse_url("nfs://127.0.0.1/export?version=4.1,4.0,3").unwrap();
        assert_eq!(
            args.versions,
            vec![NFSVersion::NFSv4p1, NFSVersion::NFSv4p0, NFSVersion::NFSv3]
        );
    }

    #[test]
    fn parse_url_with_bad_uid() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?uid=nobody");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad UID")
        );
    }

    #[test]
    fn parse_url_with_bad_gid() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?gid=wheel");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad GID")
        );
    }

    #[test]
    fn parse_url_with_bad_nfsport() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?nfsport=default");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad NFS port")
        );
    }

    #[test]
    fn parse_url_with_bad_mountport() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?mountport=nfsport");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad mount port")
        );
    }

    #[test]
    fn parse_url_with_bad_readdir_buffer_single_value() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?readdir-buffer=unlimited");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad readdir-buffer value")
        );
    }

    #[test]
    fn parse_url_with_bad_readdir_buffer_pair_first_value() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?readdir-buffer=unlimited,4096");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad readdir-buffer value")
        );
    }

    #[test]
    fn parse_url_with_bad_readdir_buffer_pair_second_value() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?readdir-buffer=4096,unlimited");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad readdir-buffer value")
        );
    }

    #[test]
    fn parse_url_with_bad_readdir_buffer_triple_value() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?readdir-buffer=2048,4096,8192");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad readdir-buffer value")
        );
    }

    #[test]
    fn parse_url_with_bad_rsize() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?rsize=sizable");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad max read size value")
        );
    }

    #[test]
    fn parse_url_with_bad_wsize() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?wsize=4mib");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad max write size value")
        );
    }

    #[test]
    fn parse_url_without_uid_and_gid() {
        let res = parse_url("nfs://127.0.0.1/some/export/path");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(args.versions, vec![NFSVersion::NFSv3]);
        assert_eq!(args.host, "127.0.0.1".to_string());
        assert_eq!(args.nfsport, 0);
        assert_eq!(args.mountport, 0);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), get_uid_gid());
        assert_eq!((args.dircount, args.maxcount), (8192, 8192));
        assert_eq!((args.rsize, args.wsize), (1048576, 1048576));
    }

    #[test]
    fn parse_url_with_uid_and_gid_and_multi_version() {
        let res = parse_url("nfs://localhost/some/export/path?version=4.1,4.0,3&uid=616&gid=666");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(
            args.versions,
            vec![NFSVersion::NFSv4p1, NFSVersion::NFSv4p0, NFSVersion::NFSv3]
        );
        assert_eq!(args.host, "localhost".to_string());
        assert_eq!(args.nfsport, 0);
        assert_eq!(args.mountport, 0);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), (616, 666));
        assert_eq!((args.dircount, args.maxcount), (8192, 8192));
        assert_eq!((args.rsize, args.wsize), (1048576, 1048576));
    }

    #[test]
    fn parse_url_with_port() {
        let res = parse_url("nfs://localhost:20490/some/export/path");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(args.versions, vec![NFSVersion::NFSv3]);
        assert_eq!(args.host, "localhost".to_string());
        assert_eq!(args.nfsport, 20490);
        assert_eq!(args.mountport, 0);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), get_uid_gid());
        assert_eq!((args.dircount, args.maxcount), (8192, 8192));
        assert_eq!((args.rsize, args.wsize), (1048576, 1048576));
    }

    #[test]
    fn parse_url_with_nfsport() {
        let res = parse_url("nfs://localhost/some/export/path?nfsport=20490");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(args.versions, vec![NFSVersion::NFSv3]);
        assert_eq!(args.host, "localhost".to_string());
        assert_eq!(args.nfsport, 20490);
        assert_eq!(args.mountport, 0);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), get_uid_gid());
        assert_eq!((args.dircount, args.maxcount), (8192, 8192));
        assert_eq!((args.rsize, args.wsize), (1048576, 1048576));
    }

    #[test]
    fn parse_url_with_mountport() {
        let res = parse_url("nfs://localhost/some/export/path?mountport=20490");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(args.versions, vec![NFSVersion::NFSv3]);
        assert_eq!(args.host, "localhost".to_string());
        assert_eq!(args.nfsport, 0);
        assert_eq!(args.mountport, 20490);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), get_uid_gid());
        assert_eq!((args.dircount, args.maxcount), (8192, 8192));
        assert_eq!((args.rsize, args.wsize), (1048576, 1048576));
    }

    #[test]
    fn parse_url_with_port_and_mountport() {
        let res = parse_url("nfs://localhost:20389/some/export/path?mountport=20490");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(args.versions, vec![NFSVersion::NFSv3]);
        assert_eq!(args.host, "localhost".to_string());
        assert_eq!(args.nfsport, 20389);
        assert_eq!(args.mountport, 20490);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), get_uid_gid());
        assert_eq!((args.dircount, args.maxcount), (8192, 8192));
        assert_eq!((args.rsize, args.wsize), (1048576, 1048576));
    }

    #[test]
    fn parse_url_with_nfsport_and_mountport() {
        let res = parse_url("nfs://localhost/some/export/path?nfsport=20389&mountport=20490");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(args.versions, vec![NFSVersion::NFSv3]);
        assert_eq!(args.host, "localhost".to_string());
        assert_eq!(args.nfsport, 20389);
        assert_eq!(args.mountport, 20490);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), get_uid_gid());
        assert_eq!((args.dircount, args.maxcount), (8192, 8192));
        assert_eq!((args.rsize, args.wsize), (1048576, 1048576));
    }

    #[test]
    fn parse_url_with_port_nfsport_and_mountport() {
        let res = parse_url("nfs://localhost:20388/some/export/path?nfsport=20389&mountport=20490");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(args.versions, vec![NFSVersion::NFSv3]);
        assert_eq!(args.host, "localhost".to_string());
        assert_eq!(args.nfsport, 20389);
        assert_eq!(args.mountport, 20490);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), get_uid_gid());
        assert_eq!((args.dircount, args.maxcount), (8192, 8192));
        assert_eq!((args.rsize, args.wsize), (1048576, 1048576));
    }

    #[test]
    fn parse_url_with_rsize() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?rsize=16384");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(args.versions, vec![NFSVersion::NFSv3]);
        assert_eq!(args.host, "127.0.0.1".to_string());
        assert_eq!(args.nfsport, 0);
        assert_eq!(args.mountport, 0);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), get_uid_gid());
        assert_eq!((args.dircount, args.maxcount), (8192, 8192));
        assert_eq!((args.rsize, args.wsize), (16384, 1048576));
    }

    #[test]
    fn parse_url_with_wsize() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?wsize=16384");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(args.versions, vec![NFSVersion::NFSv3]);
        assert_eq!(args.host, "127.0.0.1".to_string());
        assert_eq!(args.nfsport, 0);
        assert_eq!(args.mountport, 0);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), get_uid_gid());
        assert_eq!((args.dircount, args.maxcount), (8192, 8192));
        assert_eq!((args.rsize, args.wsize), (1048576, 16384));
    }

    #[test]
    fn parse_url_with_readdir_buffer_single_value() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?readdir-buffer=4096");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(args.versions, vec![NFSVersion::NFSv3]);
        assert_eq!(args.host, "127.0.0.1".to_string());
        assert_eq!(args.nfsport, 0);
        assert_eq!(args.mountport, 0);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), get_uid_gid());
        assert_eq!((args.dircount, args.maxcount), (4096, 4096));
        assert_eq!((args.rsize, args.wsize), (1048576, 1048576));
    }

    #[test]
    fn parse_url_with_readdir_buffer_pair_value() {
        let res = parse_url("nfs://127.0.0.1/some/export/path?readdir-buffer=2048,4096");
        assert!(res.is_ok(), "err = {}", res.unwrap_err());
        let args = res.unwrap();
        assert_eq!(args.versions, vec![NFSVersion::NFSv3]);
        assert_eq!(args.host, "127.0.0.1".to_string());
        assert_eq!(args.nfsport, 0);
        assert_eq!(args.mountport, 0);
        assert_eq!(args.dirpath, "/some/export/path".to_string());
        assert_eq!((args.uid, args.gid), get_uid_gid());
        assert_eq!((args.dircount, args.maxcount), (2048, 4096));
        assert_eq!((args.rsize, args.wsize), (1048576, 1048576));
    }

    #[tokio::test]
    async fn mount_with_only_v4_0_attempts_the_protocol_engine() {
        let args = MountArgs {
            versions: vec![NFSVersion::NFSv4p0],
            host: Default::default(),
            mountport: Default::default(),
            nfsport: Default::default(),
            dirpath: Default::default(),
            gid: Default::default(),
            uid: Default::default(),
            dircount: Default::default(),
            maxcount: Default::default(),
            rsize: Default::default(),
            wsize: Default::default(),
            noresvport: Default::default(),
            retain_delegations: Default::default(),
        };
        let res = mount(args).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(!matches!(&err, NfsError::Unsupported(_)));
    }

    #[tokio::test]
    async fn mount_with_only_v4_2() {
        let args = MountArgs {
            versions: vec![NFSVersion::NFSv4p2],
            host: Default::default(),
            mountport: Default::default(),
            nfsport: Default::default(),
            dirpath: Default::default(),
            gid: Default::default(),
            uid: Default::default(),
            dircount: Default::default(),
            maxcount: Default::default(),
            rsize: Default::default(),
            wsize: Default::default(),
            noresvport: Default::default(),
            retain_delegations: Default::default(),
        };
        let res = mount(args).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(matches!(&err, NfsError::Unsupported(msg) if msg == "NFSv4.2 is not supported"));
    }

    #[tokio::test]
    async fn mount_with_v4_0_then_v4_2_attempts_v4_0_before_unsupported_fallback() {
        let args = MountArgs {
            versions: vec![NFSVersion::NFSv4p0, NFSVersion::NFSv4p2],
            host: Default::default(),
            mountport: Default::default(),
            nfsport: Default::default(),
            dirpath: Default::default(),
            gid: Default::default(),
            uid: Default::default(),
            dircount: Default::default(),
            maxcount: Default::default(),
            rsize: Default::default(),
            wsize: Default::default(),
            noresvport: Default::default(),
            retain_delegations: Default::default(),
        };
        let res = mount(args).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(matches!(&err, NfsError::Rpc(msg) if msg.contains("NFSv4.2 is not supported")));
    }

    #[test]
    fn squash_mount_errors_with_only_non_unsupported_err() {
        let errs = vec![NfsError::Rpc("some error".to_string())];
        let err = squash_mount_errors(errs);
        assert!(matches!(&err, NfsError::Rpc(msg) if msg == "some error"));
    }

    #[test]
    fn squash_mount_errors_with_only_non_unsupported_errs() {
        let errs = vec![
            NfsError::Rpc("some error".to_string()),
            NfsError::InvalidInput("some other error".to_string()),
            NfsError::InvalidInput("some final error".to_string()),
        ];
        let err = squash_mount_errors(errs);
        assert!(
            matches!(&err, NfsError::Rpc(msg) if msg == "some error - some other error - some final error")
        );
    }

    #[test]
    fn squash_mount_errors_with_only_unsupported_err() {
        let errs = vec![
            NfsError::Unsupported("NFSv4.2 is not supported".to_string()),
            NfsError::Unsupported("NFSv4.2 is not supported".to_string()), // XXX: same NFS version ignored
        ];
        let err = squash_mount_errors(errs);
        assert!(matches!(&err, NfsError::Unsupported(msg) if msg == "NFSv4.2 is not supported"));
    }

    #[test]
    fn squash_mount_errors_with_only_unsupported_errs() {
        let errs = vec![
            NfsError::Unsupported("NFSv4.2 is not supported".to_string()),
            NfsError::Unsupported("NFSv4 is not supported".to_string()),
        ];
        let err = squash_mount_errors(errs);
        assert!(
            matches!(&err, NfsError::Unsupported(msg) if msg == "NFSv4.0 and NFSv4.2 are not supported")
        );
    }

    #[test]
    fn squash_mount_errors_with_unsupported_err_and_non_unsupported_err() {
        let errs = vec![
            NfsError::Unsupported("NFSv4.2 is not supported".to_string()),
            NfsError::Rpc("some error".to_string()),
        ];
        let err = squash_mount_errors(errs);
        assert!(
            matches!(&err, NfsError::Rpc(msg) if msg == "some error - NFSv4.2 is not supported")
        );
    }

    #[test]
    fn squash_mount_errors_with_unsupported_errs_and_non_unsupported_err() {
        let errs = vec![
            NfsError::Unsupported("NFSv4.2 is not supported".to_string()),
            NfsError::Rpc("some error".to_string()),
            NfsError::Unsupported("NFSv4 is not supported".to_string()),
        ];
        let err = squash_mount_errors(errs);
        assert!(
            matches!(&err, NfsError::Rpc(msg) if msg == "some error - NFSv4.0 and NFSv4.2 are not supported")
        );
    }

    #[test]
    fn squash_mount_errors_with_unsupported_err_and_non_unsupported_errs() {
        let errs = vec![
            NfsError::Rpc("some error".to_string()),
            NfsError::Unsupported("NFSv4 is not supported".to_string()),
            NfsError::InvalidInput("some other error".to_string()),
        ];
        let err = squash_mount_errors(errs);
        assert!(
            matches!(&err, NfsError::Rpc(msg) if msg == "some error - some other error - NFSv4 is not supported")
        );
    }

    #[test]
    fn squash_mount_errors_with_unsupported_errs_and_non_unsupported_errs() {
        let errs = vec![
            NfsError::Rpc("some error".to_string()),
            NfsError::Unsupported("NFSv4 is not supported".to_string()),
            NfsError::InvalidInput("some other error".to_string()),
            NfsError::Unsupported("NFSv4.2 is not supported".to_string()),
        ];
        let err = squash_mount_errors(errs);
        assert!(
            matches!(&err, NfsError::Rpc(msg) if msg == "some error - some other error - NFSv4.0 and NFSv4.2 are not supported")
        );
    }

    #[test]
    fn split_path_empty_path() {
        let path = "";
        let res = split_path(path);
        assert!(res.is_ok());
        let (dir, name) = res.unwrap();
        assert_eq!(dir, "/".to_string());
        assert_eq!(name, "".to_string());
    }

    #[test]
    fn split_path_root_path() {
        let path = "/";
        let res = split_path(path);
        assert!(res.is_ok());
        let (dir, name) = res.unwrap();
        assert_eq!(dir, "/".to_string());
        assert_eq!(name, "".to_string());
    }

    #[test]
    fn split_path_sneaky_one() {
        let path = "..";
        let res = split_path(path);
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(matches!(&err, NfsError::InvalidInput(msg) if msg == "invalid path specified"));
    }

    #[test]
    fn split_path_sneaky_two() {
        let path = "/first/../..";
        let res = split_path(path);
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(matches!(&err, NfsError::InvalidInput(msg) if msg == "invalid path specified"));
    }

    #[test]
    fn split_path_path_depth_one() {
        let path = "/first/place/";
        let res = split_path(path);
        assert!(res.is_ok());
        let (dir, name) = res.unwrap();
        assert_eq!(dir, "/first".to_string());
        assert_eq!(name, "place".to_string());
    }

    #[test]
    fn split_path_path_depth_two() {
        let path = "/first/place/1999.txt";
        let res = split_path(path);
        assert!(res.is_ok());
        let (dir, name) = res.unwrap();
        assert_eq!(dir, "/first/place".to_string());
        assert_eq!(name, "1999.txt".to_string());
    }

    #[test]
    fn parse_url_noresvport_true() {
        let args = parse_url("nfs://127.0.0.1/some/export?noresvport=true").unwrap();
        assert!(args.noresvport, "noresvport=true should parse to true");
    }

    #[test]
    fn parse_url_noresvport_default_false() {
        let args = parse_url("nfs://127.0.0.1/some/export").unwrap();
        assert!(
            !args.noresvport,
            "default should be false (preserve legacy privileged-port behavior)"
        );
    }

    #[test]
    fn parse_url_retain_delegations_is_explicit_and_default_off() {
        let default = parse_url("nfs://127.0.0.1/export?version=4.1").unwrap();
        let enabled =
            parse_url("nfs://127.0.0.1/export?version=4.1&retain-delegations=true").unwrap();
        assert!(!default.retain_delegations);
        assert!(enabled.retain_delegations);
        assert!(parse_url("nfs://127.0.0.1/export?version=4.1&retain-delegations=maybe").is_err());
    }

    #[test]
    fn parse_url_noresvport_explicit_false() {
        let args = parse_url("nfs://127.0.0.1/some/export?noresvport=false").unwrap();
        assert!(!args.noresvport, "noresvport=false should parse to false");
    }

    #[test]
    fn parse_url_with_bad_noresvport() {
        let res = parse_url("nfs://127.0.0.1/some/export?noresvport=yes");
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(
            matches!(&err, NfsError::InvalidInput(msg) if msg == "specified URL contains bad noresvport value")
        );
    }

    #[tokio::test]
    async fn connect_to_target_ephemeral_when_noresvport_true() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let accept_handle = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            (stream, peer)
        });
        let stream = connect_to_target(&listen_addr, true).await.unwrap();
        let local_port = stream.local_addr().unwrap().port();
        assert!(
            local_port >= 1024,
            "with noresvport=true, source port {} must be ephemeral (>=1024)",
            local_port
        );
        let _ = accept_handle.await.unwrap();
    }

    #[tokio::test]
    async fn connect_to_target_privileged_when_noresvport_false() {
        // 探测：本机是否允许绑特权端口（Unix root / Windows admin）。
        // 没有权限时直接跳过，避免在无特权 CI 上误报 false negative。
        let probe = match tokio::net::TcpSocket::new_v4() {
            Ok(s) => s.bind("127.0.0.1:1".parse().unwrap()).is_ok(),
            _ => false,
        };
        if !probe {
            eprintln!("skipping: insufficient privilege to bind <1024");
            return;
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let accept_handle = tokio::spawn(async move { listener.accept().await.unwrap() });
        let stream = connect_to_target(&listen_addr, false).await.unwrap();
        let local_port = stream.local_addr().unwrap().port();
        assert!(
            (1..1024).contains(&local_port),
            "with noresvport=false, source port {} must be privileged (1-1023)",
            local_port
        );
        let _ = accept_handle.await.unwrap();
    }

    #[test]
    fn privileged_port_candidates_visit_every_port_once_from_random_start() {
        let ports = vec![2, 3, 4, 5];
        assert_eq!(privileged_port_candidates(&ports, 2), vec![4, 5, 2, 3]);
    }

    #[test]
    fn bind_and_connect_tuple_collisions_try_another_privileged_port() {
        for kind in [
            std::io::ErrorKind::AddrInUse,
            std::io::ErrorKind::AddrNotAvailable,
        ] {
            assert!(is_privileged_port_collision(&std::io::Error::from(kind)));
        }
        assert!(!is_privileged_port_collision(&std::io::Error::from(
            std::io::ErrorKind::ConnectionRefused,
        )));
    }

    #[tokio::test]
    async fn nfs3_mount_preserves_the_last_address_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let error = parse_url_and_mount(&format!(
            "nfs://127.0.0.1/export?version=3&nfsport={port}&mountport={port}&noresvport=true"
        ))
        .await
        .unwrap_err();

        assert!(
            matches!(error, NfsError::Io(ref error) if error.kind() == std::io::ErrorKind::ConnectionRefused),
            "last endpoint error was hidden: {error:?}"
        );
    }
}
