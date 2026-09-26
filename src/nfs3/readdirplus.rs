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
    Mount, READDIRPLUS3args, READDIRPLUS3resok, Result, bytes_to_string, entryplus3, nfs_fh3,
    paged_dir_stream, post_op_fh3,
};
use crate::mount::{DirectoryCookie, DirectoryCursor, ReaddirplusPage};
use bytes::Bytes;
use futures::TryStreamExt as _;
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
    ) -> Result<impl Stream<Item = Result<ReaddirplusEntry>> + '_ + use<'_>> {
        let fh = self.lookup_path(dir_path).await?.fh;
        Ok(self.readdirplus(fh).await)
    }

    pub async fn readdirplus(
        &self,
        dir_fh: Bytes,
    ) -> impl Stream<Item = Result<ReaddirplusEntry>> + '_ {
        paged_dir_stream!(
            self,
            dir_fh,
            readdirplus_at,
            |entry: Box<entryplus3>| convert_entry(*entry),
            "readdirplus page received"
        )
    }

    /// One READDIRPLUS page from a caller-held position (RFC 1813 §3.3.17).
    pub async fn readdirplus_page(
        &self,
        dir_fh: Bytes,
        position: DirectoryCookie,
    ) -> Result<ReaddirplusPage> {
        let res = self
            .readdirplus_at(dir_fh, position.cookie, position.verifier)
            .await?;
        into_page(res, position)
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

fn convert_entry(entry: entryplus3) -> ReaddirplusEntry {
    ReaddirplusEntry {
        fileid: entry.fileid.0,
        file_name: bytes_to_string(entry.name.0),
        attr: entry.name_attributes.into(),
        handle: match entry.name_handle {
            post_op_fh3::TRUE(h) => h.0,
            _ => Bytes::new(),
        },
    }
}

/// Converts one reply into a public page. `.` and `..` count toward progress
/// but are omitted, as in the stream.
fn into_page(res: READDIRPLUS3resok, position: DirectoryCookie) -> Result<ReaddirplusPage> {
    let verifier: [u8; 8] = res.cookieverf.0.as_ref().try_into().unwrap_or([0u8; 8]);
    let mut last_cookie = position.cookie;
    let mut received = 0usize;
    let mut entries = Vec::new();
    let mut current = res.reply.entries;
    while let Some(mut node) = current {
        current = node.nextentry.take();
        received += 1;
        last_cookie = node.cookie.0;
        let name = node.name.0.as_ref();
        if name != b"." && name != b".." {
            entries.push(convert_entry(*node).into());
        }
    }
    let eof = res.reply.eof;
    let next = DirectoryCursor::resume(position).page(last_cookie, verifier, received, eof)?;
    Ok(ReaddirplusPage { entries, next, eof })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs4::compound::{xdr_opaque, xdr_u32};

    fn reply(verifier: [u8; 8], entries: &[(u64, &[u8])], eof: bool) -> READDIRPLUS3resok {
        let mut data = vec![0; 4]; // no directory post-op attrs
        data.extend(verifier);
        for (cookie, name) in entries {
            xdr_u32(&mut data, 1);
            data.extend(42u64.to_be_bytes());
            xdr_opaque(&mut data, name);
            data.extend(cookie.to_be_bytes());
            data.extend([0; 8]); // no attrs / fh
        }
        xdr_u32(&mut data, 0);
        xdr_u32(&mut data, u32::from(eof));
        READDIRPLUS3resok::try_from(&mut Bytes::from(data)).unwrap()
    }

    fn names(page: &ReaddirplusPage) -> Vec<&str> {
        page.entries
            .iter()
            .map(|entry| entry.file_name.as_str())
            .collect()
    }

    #[test]
    fn page_omits_dot_entries_and_resumes_after_last_cookie() {
        let first = reply(
            [7; 8],
            &[(1, b"."), (2, b".."), (3, b"a"), (4, b"b")],
            false,
        );
        let page = into_page(first, DirectoryCookie::default()).unwrap();
        assert_eq!(names(&page), ["a", "b"]);
        assert!(!page.eof);
        let expected = DirectoryCookie {
            cookie: 4,
            verifier: [7; 8],
        };
        assert_eq!(page.next, expected);

        let last = into_page(reply([7; 8], &[(5, b"c")], true), page.next).unwrap();
        assert_eq!(names(&last), ["c"]);
        assert!(last.eof);
        assert_eq!(last.next.cookie, 5);

        // A page holding only `.` and `..` still makes progress.
        let dots = into_page(
            reply([7; 8], &[(1, b"."), (2, b"..")], false),
            DirectoryCookie::default(),
        )
        .unwrap();
        assert!(dots.entries.is_empty());
        assert_eq!(dots.next.cookie, 2);

        // An empty EOF page keeps the requested cookie with its own verifier.
        let empty_eof = into_page(reply([8; 8], &[], true), expected).unwrap();
        assert!(empty_eof.entries.is_empty() && empty_eof.eof);
        assert_eq!(empty_eof.next, expected);
    }

    #[test]
    fn page_without_progress_is_an_error() {
        let position = DirectoryCookie {
            cookie: 4,
            verifier: [7; 8],
        };
        for reply in [
            reply([7; 8], &[], false),
            reply([7; 8], &[(4, b"x")], false),
            reply([7; 8], &[(0, b"x")], true),
        ] {
            let error = into_page(reply, position).unwrap_err();
            assert!(error.to_string().contains("made no progress"), "{error}");
        }
    }
}
