#![cfg(test)]

use super::*;
use crate::{Auth, NfsError, rpc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time::{Duration, timeout},
};

#[derive(Debug, PartialEq)]
struct LookupCall {
    parent: Vec<u8>,
    name: Vec<u8>,
}

fn take_opaque<'a>(request: &mut &'a [u8]) -> &'a [u8] {
    let length = u32::from_be_bytes(request[..4].try_into().unwrap()) as usize;
    let value = &request[4..4 + length];
    *request = &request[4 + length.next_multiple_of(4)..];
    value
}

fn push_opaque(reply: &mut Vec<u8>, value: &[u8]) {
    reply.extend((value.len() as u32).to_be_bytes());
    reply.extend(value);
    reply.resize(reply.len().next_multiple_of(4), 0);
}

// Use the real Mount/RPC encoder, as in readlink and retry tests. No mount or
// portmapper exchange is needed for this isolated LOOKUP name/FH regression.
async fn resolve(path: &[u8], fail_first: bool) -> (Result<ObjRes>, Vec<LookupCall>) {
    resolve_with_raw(path, fail_first, None).await
}

async fn resolve_with_raw(
    path: &[u8],
    fail_first: bool,
    raw_name: Option<Bytes>,
) -> (Result<ObjRes>, Vec<LookupCall>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, mut stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut calls = Vec::new();
        loop {
            let marker = tokio::select! {
                _ = &mut stopped => break,
                marker = stream.read_u32() => marker.unwrap(),
            };
            assert_ne!(marker & 0x80000000, 0);
            let length = (marker & 0x7fffffff) as usize;
            assert!(length <= 4096);
            let mut request = vec![0; length];
            stream.read_exact(&mut request).await.unwrap();
            // RPC CALL, rpcvers=2, NFS program/version and LOOKUP procedure.
            let words: Vec<_> = request[4..40]
                .chunks_exact(4)
                .map(|word| u32::from_be_bytes(word.try_into().unwrap()))
                .collect();
            assert_eq!(words, [0, 2, 100003, 3, 3, 0, 0, 0, 0]);
            let mut args = &request[40..];
            let parent = take_opaque(&mut args).to_vec();
            let name = take_opaque(&mut args).to_vec();
            assert!(args.is_empty());
            calls.push(LookupCall { parent, name });

            let failed = fail_first && calls.len() == 1;
            let mut reply = request[..4].to_vec();
            // RPC accepted SUCCESS followed by the NFS status.
            for word in [1u32, 0, 0, 0, 0, if failed { 2 } else { 0 }] {
                reply.extend(word.to_be_bytes());
            }
            if !failed {
                push_opaque(&mut reply, &[calls.len() as u8]);
                reply.extend(0u32.to_be_bytes()); // absent object attributes
            }
            reply.extend(0u32.to_be_bytes()); // absent directory attributes
            stream
                .write_u32(0x80000000 | reply.len() as u32)
                .await
                .unwrap();
            stream.write_all(&reply).await.unwrap();
        }
        calls
    });
    let mux = rpc::StreamMux::connect(address, true).await.unwrap();
    let mount = Mount {
        rpc: rpc::Client::new(mux, None),
        auth: Auth::new_null(),
        fh: Bytes::from_static(&[0]),
        dir: "/".into(),
        dircount: 4096,
        maxcount: 4096,
        rsize: 4096,
        wsize: 4096,
    };
    let result = timeout(Duration::from_secs(2), async {
        if let Some(raw_name) = raw_name {
            mount.lookup(Bytes::from_static(&[0]), &raw_name).await
        } else {
            mount.lookup_path(path).await
        }
    })
    .await
    .expect("local LOOKUP fixture timed out");
    stop.send(()).unwrap();
    let calls = server.await.unwrap();
    (result, calls)
}

#[tokio::test]
async fn lookup_raw_distinguishes_non_utf8_from_literal_v3() {
    for (name, expected) in [
        (Bytes::from_static(b"\xff"), b"\xff".as_slice()),
        (
            Bytes::from_static(b"~nfs-rs-raw:FF"),
            b"~nfs-rs-raw:FF".as_slice(),
        ),
    ] {
        let (result, calls) = resolve_with_raw(b"", false, Some(name)).await;
        assert!(result.is_ok());
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, expected);
    }
}

async fn assert_resolved(path: &[u8], names: &[&str]) {
    let (result, calls) = resolve(path, false).await;
    assert_eq!(
        calls,
        names
            .iter()
            .enumerate()
            .map(|(index, name)| LookupCall {
                parent: vec![index as u8],
                name: name.as_bytes().to_vec(),
            })
            .collect::<Vec<_>>(),
        "LOOKUP name and parent filehandle for {path:?}"
    );
    assert_eq!(result.unwrap().fh, vec![names.len() as u8]);
}

#[cfg(unix)]
#[tokio::test]
async fn lookup_path_preserves_literal_backslash_v3() {
    // RFC 1813 §§3.2/3.3.3: LOOKUP selects the named entry. On Unix a
    // backslash is a Normal path component, not a root separator to skip.
    assert_resolved(b"\\", &["\\"]).await;
}

#[cfg(unix)]
#[tokio::test]
async fn lookup_path_preserves_nested_backslash_v3() {
    for (path, names) in [
        ("/\\/leaf", vec!["\\", "leaf"]),
        ("folder/\\/leaf", vec!["folder", "\\", "leaf"]),
    ] {
        assert_resolved((path).as_bytes(), &names).await;
    }
}

#[cfg(windows)]
#[tokio::test]
async fn lookup_path_skips_windows_root_v3() {
    assert_resolved("\\", &[]).await;
}

#[tokio::test]
async fn lookup_path_keeps_existing_normalization_v3() {
    for path in ["", "/", ".", "./", "/./", "folder/.."] {
        assert_resolved((path).as_bytes(), &[]).await;
    }
    for (path, names) in [
        ("folder/leaf", vec!["folder", "leaf"]),
        ("/folder//./unused/../leaf/", vec!["folder", "leaf"]),
        ("../leaf", vec!["..", "leaf"]),
        ("folder/../../leaf", vec!["..", "leaf"]),
    ] {
        assert_resolved((path).as_bytes(), &names).await;
    }
}

#[tokio::test]
async fn lookup_path_preserves_first_error_v3() {
    let (result, calls) = resolve(b"missing/leaf", true).await;
    assert!(matches!(
        result,
        Err(NfsError::Nfs3(super::super::nfsstat3::NFS3ERR_NOENT))
    ));
    assert_eq!(
        calls,
        vec![LookupCall {
            parent: vec![0],
            name: b"missing".to_vec(),
        }]
    );
}

#[tokio::test]
async fn byte_path_lookup_preserves_each_component_without_text_conversion() {
    let path = b"\xff/\xfe\\line\n";
    let (result, calls) = resolve(path, false).await;
    assert!(result.is_ok());
    assert_eq!(
        calls,
        vec![
            LookupCall {
                parent: vec![0],
                name: b"\xff".to_vec()
            },
            LookupCall {
                parent: vec![1],
                name: b"\xfe\\line\n".to_vec()
            },
        ]
    );
}
