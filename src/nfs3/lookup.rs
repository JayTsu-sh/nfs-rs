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

use super::{diropargs3, filename3, nfs_fh3, LOOKUP3args, LOOKUP3resok, Mount, ObjRes, Result};
use bytes::Bytes;

impl Mount {
    pub async fn lookup_path(&self, path: &str) -> Result<ObjRes> {
        tracing::debug!(path, "resolving path");
        let mut res = Ok(ObjRes {
            fh: self.fh.clone(),
            attr: None,
        });
        for n in &path_clean::clean(path) {
            if let Ok(ref r) = res {
                if !n.is_empty() && n != "/" && n != "." && n != "\\" {
                    tracing::trace!(component = %n.to_string_lossy(), "resolving path component");
                    res = self
                        .lookup(r.fh.clone(), &n.to_string_lossy())
                        .await;
                }
            }
        }
        res
    }

    pub async fn lookup(&self, dir_fh: Bytes, filename: &str) -> Result<ObjRes> {
        let args = LOOKUP3args {
            what: diropargs3 {
                dir: nfs_fh3 {
                    data: dir_fh,
                },
                name: filename3(filename.to_string()),
            },
        };
        self._lookup(args).await?.into()
    }
}

impl From<LOOKUP3resok> for Result<ObjRes> {
    fn from(value: LOOKUP3resok) -> Self {
        Ok(ObjRes {
            fh: value.object.0,
            attr: value.obj_attributes.into(),
        })
    }
}
