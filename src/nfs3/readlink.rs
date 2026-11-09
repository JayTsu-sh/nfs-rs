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

use super::{Mount, READLINK3args, Result, nfs_fh3};
use crate::NfsError;
use bytes::Bytes;

#[allow(unused)]
impl Mount {
    pub async fn readlink_path(&self, path: &str) -> Result<String> {
        self.readlink(self.lookup_path(path).await?.fh).await
    }

    pub async fn readlink(&self, fh: Bytes) -> Result<String> {
        let args = READLINK3args {
            symlink: nfs_fh3 { data: fh },
        };
        let ok = self._readlink(args).await?;
        // RFC 1813 §3.3.5 leaves interpretation to the client. This String
        // interface must not replace bytes and silently select another target.
        std::str::from_utf8(&ok.data.0)
            .map(str::to_owned)
            .map_err(|error| NfsError::Xdr(format!("READLINK target is not UTF-8: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Auth, NfsError, rpc};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn readlink_preserves_utf8_identity_v3() {
        // RFC 1813 §3.3.5 returns uninterpreted target bytes. A String API
        // must reject unrepresentable bytes rather than resolve another name.
        for target in [
            b"../target".as_slice(),
            "目录/target".as_bytes(),
            "�".as_bytes(),
            b"\xff",
            b"\xc0\xaf",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let wire_target = target.to_vec();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let marker = stream.read_u32().await.unwrap();
                assert_ne!(marker & 0x80000000, 0);
                let mut request = vec![0; (marker & 0x7fffffff) as usize];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(u32::from_be_bytes(request[20..24].try_into().unwrap()), 5);
                let mut reply = request[..4].to_vec();
                // RPC accepted SUCCESS, NFS3_OK, absent post-op attributes.
                for word in [1u32, 0, 0, 0, 0, 0, 0] {
                    reply.extend(word.to_be_bytes());
                }
                reply.extend((wire_target.len() as u32).to_be_bytes());
                reply.extend(&wire_target);
                reply.resize(reply.len().next_multiple_of(4), 0);
                stream
                    .write_u32(0x80000000 | reply.len() as u32)
                    .await
                    .unwrap();
                stream.write_all(&reply).await.unwrap();
            });
            let mux = rpc::StreamMux::connect(address, true).await.unwrap();
            let mount = Mount {
                rpc: rpc::Client::new(mux, None),
                auth: Auth::new_null(),
                fh: Bytes::from_static(b"link"),
                dir: "/".into(),
                dircount: 4096,
                maxcount: 4096,
                rsize: 4096,
                wsize: 4096,
            };
            let result = mount.readlink(mount.getfh()).await;
            match std::str::from_utf8(target) {
                Ok(expected) => assert_eq!(result.unwrap(), expected),
                Err(_) => assert!(matches!(result, Err(NfsError::Xdr(_))), "{result:?}"),
            }
            server.await.unwrap();
        }
    }
}
