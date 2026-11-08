#![cfg(test)]
//! 普通属性读取不隐式请求 ACL；显式 ACL 权限失败仍保留原始服务端原因。
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes};

use super::super::MinorVersion;
use super::super::test_support::{Reply, ok, opaque, putfh, serve_observed};
use crate::{Nfs4ErrorCode, NfsError};

#[tokio::test]
async fn ordinary_attributes_remain_available_when_acl_reads_are_denied() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let (mount, server) = serve_observed(
        |tag, mut args| {
            assert!(matches!(tag, "getattr" | "getacl"));
            putfh(&mut args);
            assert_eq!(args.get_u32(), 9);
            let words = args.get_u32();
            assert!(words > 0);
            let word0 = args.get_u32();
            if word0 & (1 << 12) != 0 {
                return vec![
                    ok(22, vec![]),
                    Reply {
                        opcode: 9,
                        status: 13,
                        data: vec![],
                    },
                ];
            }
            // RFC 8881 §18.7.3: unrequested ACL failure must not hide basic attributes.
            let mut values = 1u32.to_be_bytes().to_vec();
            values.extend_from_slice(&5u64.to_be_bytes());
            values.extend_from_slice(&0o644u32.to_be_bytes());
            let mut data = 2u32.to_be_bytes().to_vec();
            data.extend_from_slice(&((1u32 << 1) | (1 << 4)).to_be_bytes());
            data.extend_from_slice(&(1u32 << 1).to_be_bytes());
            data.extend_from_slice(&opaque(&values));
            vec![ok(22, vec![]), ok(9, data)]
        },
        MinorVersion::V1,
        observed.clone(),
    )
    .await;
    let attr = mount
        .getattr(Bytes::from_static(b"file"))
        .await
        .expect("ordinary observation must not acquire ACL permissions");
    assert_eq!(attr.type_, 1);
    assert_eq!(attr.filesize, 5);
    assert_eq!(attr.file_mode, 0o644);
    assert!(attr.acl.is_none());
    assert!(matches!(
        mount.getacl(Bytes::from_static(b"file")).await,
        Err(NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_ACCESS))
    ));
    assert_eq!(observed.lock().unwrap().len(), 2);
    server.abort();
}
