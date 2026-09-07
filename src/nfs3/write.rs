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
use crate::mount::{WriteOutcome, WriteStability};
use bytes::Bytes;

impl Mount {
    /// WRITE with the requested stability level; no COMMIT is issued here.
    pub async fn write_with(
        &self,
        fh: Bytes,
        offset: u64,
        data: Bytes,
        stability: WriteStability,
    ) -> Result<WriteOutcome> {
        if data.len() > u32::MAX as usize {
            return Err(NfsError::InvalidInput(
                "data length exceeds maximum".to_string(),
            ));
        }
        let count = data.len() as u32;
        let stable = match stability {
            WriteStability::Unstable => WriteStable::Unstable,
            WriteStability::DataSync => WriteStable::DataSync,
            WriteStability::FileSync => WriteStable::FileSync,
        };
        let ok = self
            ._write(WRITE3args {
                file: nfs_fh3 { data: fh },
                stable,
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
        Ok(WriteOutcome {
            count: ok.count.0,
            stable: ok.committed == stable_how::FILE_SYNC,
            verifier: Some(verifier),
        })
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
        let res = mount
            .write_with(Bytes::new(), 0, Bytes::from(data), WriteStability::FileSync)
            .await;
        assert!(matches!(res, Err(NfsError::InvalidInput(_))));
    }
}
