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

use super::{LINK3args, Mount, diropargs3, filename3, nfs_fh3};
use crate::error::{NfsError, Result};
use crate::split_path;
use bytes::Bytes;

impl Mount {
    pub async fn link_path(&self, src_path: &str, dst_path: &str) -> Result<crate::mount::Attr> {
        let (dst_dir, dst_filename) = split_path(dst_path)?;
        let src_fh = self.lookup_path(src_path).await?.fh;
        let dst_dir_fh = self.lookup_path(&dst_dir).await?.fh;
        self.link(src_fh, dst_dir_fh, &dst_filename).await
    }

    pub async fn link(
        &self,
        src_fh: Bytes,
        dst_dir_fh: Bytes,
        dst_filename: &str,
    ) -> Result<crate::mount::Attr> {
        let args = LINK3args {
            file: nfs_fh3 { data: src_fh },
            link: diropargs3 {
                dir: nfs_fh3 { data: dst_dir_fh },
                name: filename3(dst_filename.to_string()),
            },
        };
        let ok = self._link(args).await?;
        Into::<Option<crate::mount::Attr>>::into(ok.file_attributes)
            .ok_or(NfsError::Rpc("linking failed".to_string()))
    }
}
