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

use super::{bytes_to_string, entryplus3, nfs_fh3, post_op_fh3, paged_dir_stream, Mount, READDIRPLUS3args, READDIRPLUS3resok, Result};
use bytes::Bytes;
use futures::stream::Stream;

#[derive(Debug)]
pub struct ReaddirplusEntry {
    pub fileid: u64,
    pub file_name: String,
    pub attr: Option<crate::mount::Attr>,
    pub handle: Bytes,
}

impl From<ReaddirplusEntry> for crate::mount::ReaddirplusEntry {
    fn from(entry: ReaddirplusEntry) -> Self {
        Self {
            fileid: entry.fileid,
            file_name: entry.file_name,
            attr: entry.attr,
            handle: entry.handle,
        }
    }
}

#[allow(unused)]
impl Mount {
    pub async fn readdirplus_path(
        &self,
        dir_path: &str,
    ) -> Result<impl Stream<Item = Result<ReaddirplusEntry>> + '_> {
        let fh = self.lookup_path(dir_path).await?.fh;
        Ok(self.readdirplus(fh).await)
    }

    pub async fn readdirplus(&self, dir_fh: Bytes) -> impl Stream<Item = Result<ReaddirplusEntry>> + '_ {
        paged_dir_stream!(self, dir_fh, readdirplus_at, |entry: Box<entryplus3>| ReaddirplusEntry {
            fileid: entry.fileid.0,
            file_name: bytes_to_string(entry.name.0),
            attr: entry.name_attributes.into(),
            handle: match entry.name_handle {
                post_op_fh3::TRUE(h) => h.0,
                _ => Bytes::new(),
            },
        }, "readdirplus page received")
    }

    pub async fn readdirplus_at(
        &self,
        dir_fh: Bytes,
        cookie: u64,
        cookieverf: [u8; 8],
    ) -> Result<READDIRPLUS3resok> {
        let args = READDIRPLUS3args {
            dir: nfs_fh3 { data: dir_fh },
            cookie,
            cookieverf,
            dircount: self.dircount,
            maxcount: self.maxcount,
        };
        self._readdirplus(args).await
    }

}
