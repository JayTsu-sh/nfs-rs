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

use super::{Mount, WRITE3args, WriteStable, nfs_fh3, stable_how};
use crate::error::{NfsError, Result};
use bytes::Bytes;

#[allow(unused)]
impl Mount {
    pub async fn write_with_outcome(
        &self,
        fh: Bytes,
        offset: u64,
        data: Bytes,
    ) -> Result<crate::mount::WriteOutcome> {
        if data.len() > u32::MAX as usize {
            return Err(NfsError::InvalidInput(
                "data length exceeds maximum".to_string(),
            ));
        }
        let count = data.len() as u32;
        let ok = self
            ._write(WRITE3args {
                file: nfs_fh3 { data: fh },
                stable: WriteStable::FileSync,
                count,
                data,
                offset,
            })
            .await?;
        let verifier = ok
            .verf
            .0
            .as_ref()
            .try_into()
            .map_err(|_| NfsError::Xdr("WRITE verifier must be 8 bytes".to_string()))?;
        Ok(crate::mount::WriteOutcome {
            count: ok.count.0,
            stable: ok.committed == stable_how::FILE_SYNC,
            verifier: Some(verifier),
        })
    }

    pub async fn write_path(&self, path: &str, offset: u64, data: Bytes) -> Result<u32> {
        self.write(self.lookup_path(path).await?.fh, offset, data)
            .await
    }

    pub async fn write(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<u32> {
        if data.len() > u32::MAX as usize {
            return Err(NfsError::InvalidInput(
                "data length exceeds maximum".to_string(),
            ));
        }
        let count = data.len() as u32;
        let args = WRITE3args {
            file: nfs_fh3 { data: fh.clone() },
            stable: WriteStable::FileSync,
            count,
            data,
            offset,
        };
        let ok = self._write(args).await?;
        // RFC 1813 §3.3.7: server may downgrade stability. If we requested
        // FILE_SYNC but the server returned UNSTABLE or DATA_SYNC, issue
        // a COMMIT to ensure data reaches stable storage.
        if ok.committed != stable_how::FILE_SYNC {
            tracing::debug!(
                committed = ?ok.committed,
                offset,
                count = ok.count.0,
                "server downgraded write stability, issuing COMMIT"
            );
            self.commit(fh, offset, ok.count.0).await?;
        }
        Ok(ok.count.0)
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))] // usize and u32 are the same for wasm32, so below test won't compile due to overflow in (u32::MAX as usize) + 1
mod tests {
    use super::*;

    #[tokio::test]
    async fn mount_write_fh_data_exceeding_max_length() {
        let mount = Mount {
            rpc: crate::rpc::Client::new_dummy().await,
            auth: crate::rpc::auth::Auth::new_null(),
            dir: "/".to_string(),
            fh: Bytes::new(),
            dircount: 512,
            maxcount: 4096,
            rsize: 8192,
            wsize: 16384,
        };
        let data = vec![0u8; (u32::MAX as usize) + 1];
        let res = mount.write(Bytes::new(), 0, Bytes::from(data)).await;
        assert!(matches!(res, Err(NfsError::InvalidInput(_))));
    }
}
