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
use bytes::Bytes;

#[allow(unused)]
impl Mount {
    pub async fn readlink_path(&self, path: &[u8]) -> Result<Bytes> {
        self.readlink(self.lookup_path(path).await?.fh).await
    }

    pub async fn readlink(&self, fh: Bytes) -> Result<Bytes> {
        let args = READLINK3args {
            symlink: nfs_fh3 { data: fh },
        };
        let ok = self._readlink(args).await?;
        Ok(ok.data.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Auth, rpc};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn readlink_preserves_byte_identity_v3() {
        // RFC 1813 §3.3.5 returns uninterpreted target bytes. A String API
        // preserves every byte instead of resolving another name.
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
            assert_eq!(result.unwrap().as_ref(), target);
            server.await.unwrap();
        }
    }
}
