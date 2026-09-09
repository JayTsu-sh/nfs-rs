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

use super::{
    Mount, READDIR3args, READDIR3resok, Result, bytes_to_string, entry3, nfs_fh3, paged_dir_stream,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use futures::stream::Stream;

#[derive(Debug)]
pub struct ReaddirEntry {
    pub fileid: u64,
    pub file_name: String,
}

impl From<ReaddirEntry> for crate::mount::ReaddirEntry {
    fn from(entry: ReaddirEntry) -> Self {
        Self {
            fileid: entry.fileid,
            file_name: entry.file_name,
        }
    }
}

#[allow(unused)]
impl Mount {
    pub async fn readdir_path(
        &self,
        dir_path: &str,
    ) -> Result<impl Stream<Item = Result<ReaddirEntry>> + '_ + use<'_>> {
        let fh = self.lookup_path(dir_path).await?.fh;
        Ok(self.readdir(fh).await)
    }

    pub async fn readdir(&self, dir_fh: Bytes) -> impl Stream<Item = Result<ReaddirEntry>> + '_ {
        paged_dir_stream!(
            self,
            dir_fh,
            readdir_at,
            |entry: Box<entry3>| ReaddirEntry {
                fileid: entry.fileid.0,
                file_name: bytes_to_string(entry.name.0),
            },
            "readdir page received"
        )
    }

    pub async fn readdir_at(
        &self,
        dir_fh: Bytes,
        cookie: u64,
        cookieverf: [u8; 8],
    ) -> Result<READDIR3resok> {
        let args = READDIR3args {
            dir: nfs_fh3 { data: dir_fh },
            cookie,
            cookieverf,
            count: self.maxcount,
        };
        self._readdir(args).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs3::{READDIR3resok, READDIRPLUS3resok};
    use crate::nfs4::compound::{xdr_opaque, xdr_u32};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Pages {
        cookies: Vec<Option<u64>>,
        eof: bool,
        next: AtomicUsize,
    }
    impl Pages {
        fn body(&self, cookie: u64, verifier: [u8; 8], plus: bool) -> Bytes {
            let index = self.next.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                cookie,
                if index == 0 {
                    0
                } else {
                    self.cookies[index - 1].unwrap_or(0)
                }
            );
            assert_eq!(verifier, [if index == 0 { 0 } else { 7 }; 8]);
            let mut data = vec![0; 4]; // no directory post-op attrs
            data.extend([7; 8]);
            let entry = self.cookies[index];
            xdr_u32(&mut data, u32::from(entry.is_some()));
            if let Some(cookie) = entry {
                data.extend(42u64.to_be_bytes());
                xdr_opaque(&mut data, b"entry");
                data.extend(cookie.to_be_bytes());
                if plus {
                    data.extend([0; 8]);
                } // no attrs / fh
                xdr_u32(&mut data, 0);
            }
            xdr_u32(
                &mut data,
                u32::from(self.eof && index + 1 == self.cookies.len()),
            );
            Bytes::from(data)
        }
        async fn read(&self, _: Bytes, cookie: u64, verifier: [u8; 8]) -> Result<READDIR3resok> {
            Ok(READDIR3resok::try_from(&mut self.body(cookie, verifier, false)).unwrap())
        }
        async fn plus(
            &self,
            _: Bytes,
            cookie: u64,
            verifier: [u8; 8],
        ) -> Result<READDIRPLUS3resok> {
            Ok(READDIRPLUS3resok::try_from(&mut self.body(cookie, verifier, true)).unwrap())
        }
    }

    #[tokio::test]
    async fn directory_paging_rejects_no_progress_and_preserves_verifiers() {
        for (cookies, eof, valid) in [
            (vec![None], false, false),
            (vec![Some(5), Some(5)], false, false),
            (vec![Some(5), Some(8), Some(5)], false, false),
            (vec![Some(5), None], true, true),
            (vec![None], true, true),
        ] {
            let pages = Pages {
                cookies,
                eof,
                next: AtomicUsize::new(0),
            };
            let entries = paged_dir_stream!(
                &pages,
                Bytes::new(),
                read,
                |e: Box<crate::nfs3::entry3>| e.cookie.0,
                "test page"
            );
            let result: Result<Vec<_>> = entries.try_collect().await;
            assert_eq!(result.is_ok(), valid);
            pages.next.store(0, Ordering::SeqCst);
            let entries = paged_dir_stream!(
                &pages,
                Bytes::new(),
                plus,
                |e: Box<crate::nfs3::entryplus3>| e.cookie.0,
                "test plus page"
            );
            let result: Result<Vec<_>> = entries.try_collect().await;
            assert_eq!(result.is_ok(), valid);
        }
    }
}
