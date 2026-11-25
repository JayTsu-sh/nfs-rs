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

use super::{LOOKUP3args, LOOKUP3resok, Mount, ObjRes, Result, nfs_fh3};
use crate::path_resolution::{path_components, validate_name};
use bytes::Bytes;

impl Mount {
    pub async fn lookup_path(&self, path: &[u8]) -> Result<ObjRes> {
        let mut object = ObjRes {
            fh: self.fh.clone(),
            attr: None,
        };
        for component in path_components(path)? {
            object = self.lookup(object.fh, component).await?;
        }
        Ok(object)
    }

    pub async fn lookup(&self, dir_fh: Bytes, filename: &[u8]) -> Result<ObjRes> {
        validate_name(filename)?;
        let args = LOOKUP3args {
            dir: nfs_fh3 { data: dir_fh },
            name: filename,
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

#[cfg(test)]
#[path = "lookup_tests.rs"]
mod tests;
