use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::time::Duration;

use super::compound::{
    CallbackAddress, CompoundBuilder, SetClientIdArgs, decode_confirm_response,
    decode_setclientid_response,
};
use crate::error::{NfsError, Result};
use crate::mount::{self, NFSVersion};
use crate::nfs4::compound::decode_navigation_response;
use crate::rpc::auth::Auth;
use crate::rpc::{self, ReplayPolicy};
use crate::{Mount, MountArgs};

const NFS4_DEFAULT_PORT: u16 = 2049;
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const SAFE_REPLAY: ReplayPolicy = ReplayPolicy::byte_identical(2);

struct Mount40 {
    rpc: rpc::Client,
    auth: Auth,
    root_fh: Bytes,
}

impl Debug for Mount40 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Mount40")
            .field("root_fh_len", &self.root_fh.len())
            .field("client_identity", &"confirmed")
            .finish()
    }
}

pub(crate) async fn mount(args: &MountArgs) -> Result<Box<dyn Mount>> {
    let port = if args.nfsport == 0 {
        NFS4_DEFAULT_PORT
    } else {
        args.nfsport
    };
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host(format!("{}:{port}", args.host))
        .await
        .map_err(NfsError::Io)?
        .collect();
    let auth = Auth::new_unix("nfs-rs", args.uid, args.gid);
    let mut last_error = None;
    for addr in addrs {
        match mount_on_addr(addr, args, auth.clone()).await {
            Ok(mount) => return Ok(Box::new(mount)),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| NfsError::Rpc("NFSv4.0 resolved no addresses".to_string())))
}

async fn mount_on_addr(addr: SocketAddr, args: &MountArgs, auth: Auth) -> Result<Mount40> {
    let mux = rpc::StreamMux::connect(addr, args.noresvport).await?;
    let rpc = rpc::Client::new(mux, None);
    let identity_rpc = rpc.clone();
    let identity_auth = auth.clone();
    let identity =
        tokio::spawn(async move { establish_identity(&identity_rpc, &identity_auth).await });
    identity
        .await
        .map_err(|error| NfsError::Rpc(format!("NFSv4.0 identity task failed: {error}")))??;

    let components: Vec<&str> = args
        .dirpath
        .split('/')
        .filter(|component| !component.is_empty())
        .collect();
    let mut navigation = CompoundBuilder::new("navigate").putrootfh();
    for component in &components {
        navigation = navigation.lookup(component);
    }
    let response = rpc
        .call(
            navigation.getfh().encode_with_header(&auth),
            SAFE_REPLAY,
            METADATA_TIMEOUT,
        )
        .await?;
    let root_fh = decode_navigation_response(response, components.len())?;

    Ok(Mount40 { rpc, auth, root_fh })
}

async fn establish_identity(rpc: &rpc::Client, auth: &Auth) -> Result<()> {
    let verifier: [u8; 8] = rand::random();
    let owner = format!("nfs-rs-v4.0-{:016x}", rand::random::<u64>());
    let identity_request = CompoundBuilder::new("identity")
        .setclientid(SetClientIdArgs {
            verifier,
            owner: owner.as_bytes(),
            callback: CallbackAddress::DISABLED,
        })
        .encode_with_header(auth);
    let identity_response = rpc
        .call(
            identity_request,
            ReplayPolicy::ONE_ATTEMPT,
            METADATA_TIMEOUT,
        )
        .await?;
    let (client_id, confirm_verifier) = decode_setclientid_response(identity_response)?;

    let confirm_request = CompoundBuilder::new("confirm")
        .setclientid_confirm(client_id, confirm_verifier)
        .encode_with_header(auth);
    let confirm_response = rpc
        .call(confirm_request, ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
        .await?;
    decode_confirm_response(confirm_response)?;
    Ok(())
}

fn unsupported<T>(operation: &str) -> Result<T> {
    Err(NfsError::Unsupported(format!(
        "NFSv4.0 {operation} is not implemented in the minimal mount slice"
    )))
}

#[async_trait]
impl Mount for Mount40 {
    fn get_max_read_size(&self) -> u32 {
        1_048_576
    }
    fn get_max_write_size(&self) -> u32 {
        1_048_576
    }
    fn version(&self) -> NFSVersion {
        NFSVersion::NFSv4p0
    }
    async fn getfh(&self) -> Bytes {
        self.root_fh.clone()
    }

    async fn null(&self) -> Result<()> {
        let mut request = Vec::new();
        crate::nfs3::rpc_header(100003, 4, 0, &self.auth).encode(&mut request);
        self.rpc
            .call(request, SAFE_REPLAY, METADATA_TIMEOUT)
            .await?;
        Ok(())
    }

    async fn umount(&self) -> Result<()> {
        self.rpc.shutdown().await;
        Ok(())
    }

    async fn access(&self, _fh: Bytes, _mode: u32) -> Result<u32> {
        unsupported("ACCESS")
    }
    async fn commit(&self, _fh: Bytes, _offset: u64, _count: u32) -> Result<()> {
        unsupported("COMMIT")
    }
    async fn create(
        &self,
        _dir_fh: Bytes,
        _filename: &str,
        _mode: Option<u32>,
    ) -> Result<mount::ObjRes> {
        unsupported("CREATE")
    }
    async fn create_path(&self, _path: &str, _mode: Option<u32>) -> Result<mount::ObjRes> {
        unsupported("CREATE")
    }
    async fn fsinfo(&self) -> Result<mount::FSInfo> {
        unsupported("FSINFO")
    }
    async fn fsstat(&self) -> Result<mount::FSStat> {
        unsupported("FSSTAT")
    }
    async fn getattr(&self, _fh: Bytes) -> Result<mount::Attr> {
        unsupported("GETATTR")
    }
    async fn setattr(
        &self,
        _fh: Bytes,
        _guard_ctime: Option<crate::Time>,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<crate::Time>,
        _mtime: Option<crate::Time>,
    ) -> Result<()> {
        unsupported("SETATTR")
    }
    async fn setattr_path(
        &self,
        _path: &str,
        _specify_guard: bool,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<crate::Time>,
        _mtime: Option<crate::Time>,
    ) -> Result<()> {
        unsupported("SETATTR")
    }
    async fn link(
        &self,
        _src_fh: Bytes,
        _dst_dir_fh: Bytes,
        _dst_filename: &str,
    ) -> Result<mount::Attr> {
        unsupported("LINK")
    }
    async fn link_path(&self, _src_path: &str, _dst_path: &str) -> Result<mount::Attr> {
        unsupported("LINK")
    }
    async fn symlink_path(&self, _src_path: &str, _dst_path: &str) -> Result<mount::ObjRes> {
        unsupported("SYMLINK")
    }
    async fn symlink(
        &self,
        _src_path: &str,
        _dst_dir_fh: Bytes,
        _dst_filename: &str,
    ) -> Result<mount::ObjRes> {
        unsupported("SYMLINK")
    }
    async fn readlink(&self, _fh: Bytes) -> Result<String> {
        unsupported("READLINK")
    }
    async fn lookup(&self, _dir_fh: Bytes, _filename: &str) -> Result<mount::ObjRes> {
        unsupported("LOOKUP")
    }
    async fn lookup_path(&self, _path: &str) -> Result<mount::ObjRes> {
        unsupported("LOOKUP")
    }
    async fn pathconf(&self, _fh: Bytes) -> Result<mount::Pathconf> {
        unsupported("PATHCONF")
    }
    async fn read(&self, _fh: Bytes, _offset: u64, _count: u32) -> Result<Bytes> {
        unsupported("READ")
    }
    async fn write(&self, _fh: Bytes, _offset: u64, _data: Bytes) -> Result<u32> {
        unsupported("WRITE")
    }
    async fn readdir(&self, _dir_fh: Bytes) -> mount::ReaddirStream<'_> {
        Box::pin(stream::once(async { unsupported("READDIR") }))
    }
    async fn readdirplus(&self, _dir_fh: Bytes) -> mount::ReaddirplusStream<'_> {
        Box::pin(stream::once(async { unsupported("READDIRPLUS") }))
    }
    async fn mkdir(&self, _dir_fh: Bytes, _dirname: &str, _mode: u32) -> Result<mount::ObjRes> {
        unsupported("MKDIR")
    }
    async fn mkdir_path(&self, _path: &str, _mode: u32) -> Result<mount::ObjRes> {
        unsupported("MKDIR")
    }
    async fn remove(&self, _dir_fh: Bytes, _filename: &str) -> Result<()> {
        unsupported("REMOVE")
    }
    async fn remove_path(&self, _path: &str) -> Result<()> {
        unsupported("REMOVE")
    }
    async fn rmdir(&self, _dir_fh: Bytes, _dirname: &str) -> Result<()> {
        unsupported("RMDIR")
    }
    async fn rmdir_path(&self, _path: &str) -> Result<()> {
        unsupported("RMDIR")
    }
    async fn rename_path(&self, _from_path: &str, _to_path: &str) -> Result<()> {
        unsupported("RENAME")
    }
    async fn rename(
        &self,
        _from_dir_fh: Bytes,
        _from_filename: &str,
        _to_dir_fh: Bytes,
        _to_filename: &str,
    ) -> Result<()> {
        unsupported("RENAME")
    }
    async fn exports(&self) -> Result<Vec<mount::ExportEntry>> {
        unsupported("EXPORTS")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs4::compound::xdr_opaque;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{Notify, oneshot};

    async fn read_record(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let marker = stream.read_u32().await?;
        let mut record = vec![0; (marker & 0x7fff_ffff) as usize];
        stream.read_exact(&mut record).await?;
        Ok(record)
    }

    fn compound_result(tag: &str, ops: &[(u32, &[u8])]) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&0u32.to_be_bytes());
        xdr_opaque(&mut result, tag.as_bytes());
        result.extend_from_slice(&(ops.len() as u32).to_be_bytes());
        for (opcode, data) in ops {
            result.extend_from_slice(&opcode.to_be_bytes());
            result.extend_from_slice(&0u32.to_be_bytes());
            result.extend_from_slice(data);
        }
        result
    }

    async fn reply(stream: &mut TcpStream, request: &[u8], payload: &[u8]) -> std::io::Result<()> {
        let xid = u32::from_be_bytes(request[0..4].try_into().unwrap());
        let mut response = Vec::new();
        response.extend_from_slice(&xid.to_be_bytes());
        response.extend_from_slice(&1u32.to_be_bytes());
        response.extend_from_slice(&0u32.to_be_bytes());
        response.extend_from_slice(&0u32.to_be_bytes());
        response.extend_from_slice(&0u32.to_be_bytes());
        response.extend_from_slice(&0u32.to_be_bytes());
        response.extend_from_slice(payload);
        stream
            .write_u32(0x8000_0000 | response.len() as u32)
            .await?;
        stream.write_all(&response).await
    }

    #[tokio::test]
    async fn scripted_mount_reconnect_preserves_confirmed_identity() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mounted = Arc::new(Notify::new());
        let mounted_for_server = Arc::clone(&mounted);
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await?;

            let identity = read_record(&mut first).await?;
            assert!(
                identity
                    .windows(4)
                    .any(|value| value == 35u32.to_be_bytes())
            );
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x77; 8]);
            reply(
                &mut first,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;

            let confirm = read_record(&mut first).await?;
            let identity_tuple = [
                0x0102_0304_0506_0708u64.to_be_bytes().as_slice(),
                [0x77; 8].as_slice(),
            ]
            .concat();
            assert!(confirm.windows(16).any(|value| value == identity_tuple));
            reply(
                &mut first,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;

            let navigation = read_record(&mut first).await?;
            let minor_and_ops = [0u32.to_be_bytes(), 3u32.to_be_bytes()].concat();
            assert!(navigation.windows(8).any(|value| value == minor_and_ops));
            let mut fh = Vec::new();
            xdr_opaque(&mut fh, b"scripted-fh");
            reply(
                &mut first,
                &navigation,
                &compound_result("navigate", &[(24, &[]), (15, &[]), (10, &fh)]),
            )
            .await?;
            mounted_for_server.notified().await;
            socket2::SockRef::from(&first).set_linger(Some(Duration::ZERO))?;
            drop(first);

            let (mut second, _) = listener.accept().await?;
            let null = read_record(&mut second).await?;
            reply(&mut second, &null, &[]).await?;
            Ok::<(), std::io::Error>(())
        });

        let args = crate::MountArgs {
            versions: vec![NFSVersion::NFSv4p0],
            host: "127.0.0.1".to_string(),
            dirpath: "/export".to_string(),
            mountport: 0,
            nfsport: addr.port(),
            uid: 0,
            gid: 0,
            dircount: 32 * 1024,
            maxcount: 32 * 1024,
            rsize: 0,
            wsize: 0,
            noresvport: true,
            retain_delegations: false,
        };
        let mount = mount_on_addr(addr, &args, Auth::new_null()).await.unwrap();
        assert_eq!(mount.root_fh, Bytes::from_static(b"scripted-fh"));
        mounted.notify_one();
        tokio::time::sleep(Duration::from_millis(10)).await;
        mount.null().await.unwrap();
        server.await.unwrap().unwrap();
        mount.umount().await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_after_setclientid_detaches_while_confirm_settles() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (identity_seen_tx, identity_seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let identity = read_record(&mut stream).await?;
            assert!(
                identity
                    .windows(4)
                    .any(|value| value == 35u32.to_be_bytes())
            );
            let _ = identity_seen_tx.send(());
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x77; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;
            Ok::<bool, std::io::Error>(confirm.windows(4).any(|value| value == 36u32.to_be_bytes()))
        });
        let args = crate::MountArgs {
            versions: vec![NFSVersion::NFSv4p0],
            host: "127.0.0.1".to_string(),
            dirpath: "/export".to_string(),
            mountport: 0,
            nfsport: addr.port(),
            uid: 0,
            gid: 0,
            dircount: 32 * 1024,
            maxcount: 32 * 1024,
            rsize: 0,
            wsize: 0,
            noresvport: true,
            retain_delegations: false,
        };
        let task = tokio::spawn(async move { mount_on_addr(addr, &args, Auth::new_null()).await });
        identity_seen_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        assert!(
            server.await.unwrap().unwrap(),
            "sent SETCLIENTID was not settled with confirm"
        );
    }
}
