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

use super::{nfs_fh3, PATHCONF3args, Mount, Result};
use bytes::Bytes;

#[allow(unused)]
impl Mount {
    pub async fn pathconf_path(&self, path: &str) -> Result<crate::mount::Pathconf> {
        self.pathconf(self.lookup_path(path).await?.fh).await
    }

    pub async fn pathconf(&self, fh: Bytes) -> Result<crate::mount::Pathconf> {
        let args = PATHCONF3args {
            object: nfs_fh3 { data: fh },
        };
        Ok(self._pathconf(args).await?.into())
    }
}
