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

use super::{Mount, READ3args, Result, nfs_fh3};
use bytes::Bytes;

#[allow(unused)]
impl Mount {
    pub async fn read_path(&self, path: &str, offset: u64, count: u32) -> Result<Bytes> {
        self.read(self.lookup_path(path).await?.fh, offset, count)
            .await
    }

    pub async fn read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
        let args = READ3args {
            file: nfs_fh3 { data: fh },
            offset,
            count: count.min(self.rsize),
        };
        let ok = self._read(args).await?;
        Ok(ok.data)
    }
}
