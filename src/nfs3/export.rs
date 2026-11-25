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

use bytes::{Buf, Bytes};

use super::fastxdr::{MNTNAMLEN, MNTPATHLEN};
use super::{Mount, MountProc3, rpc_header};
use crate::error::{NfsError, Result};
use crate::mount::ExportEntry;
use crate::rpc;

/// Slice a counted MOUNT byte field from the reply, including its XDR padding.
fn decode_field(bytes: &mut Bytes, limit: u32, field: &str) -> Result<Bytes> {
    if bytes.remaining() < 4 {
        return Err(NfsError::Xdr(format!(
            "MOUNT EXPORT {field} length truncated"
        )));
    }
    let len = bytes.get_u32();
    if len > limit {
        return Err(NfsError::Xdr(format!(
            "MOUNT EXPORT {field} exceeds {limit} bytes"
        )));
    }
    let len = len as usize;
    let pad = (4 - len % 4) % 4;
    if bytes.remaining() < len + pad {
        return Err(NfsError::Xdr(format!(
            "MOUNT EXPORT {field} data truncated"
        )));
    }
    let value = bytes.split_to(len);
    bytes.advance(pad);
    Ok(value)
}

fn has_next(bytes: &mut Bytes, list: &str) -> Result<bool> {
    if bytes.remaining() < 4 {
        return Err(NfsError::Xdr(format!("MOUNT EXPORT {list} flag truncated")));
    }
    match bytes.get_u32() {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(NfsError::Xdr(format!(
            "MOUNT EXPORT {list} flag is {value}"
        ))),
    }
}

/// Decode the XDR optional-linked-list body returned by MOUNT EXPORT (procedure 5).
///
/// Wire format (RFC 1813):
/// ```text
/// loop:
///   opt     : u32   (1 = more entry, 0 = end of list)
///   ex_dir  : xdr-string
///   groups  : (nested loop)
///               opt2  : u32   (1 = more group, 0 = end)
///               name  : xdr-string
/// ```
pub(crate) fn decode_exports(bytes: &mut Bytes) -> Result<Vec<ExportEntry>> {
    let mut result = Vec::new();
    while has_next(bytes, "entries")? {
        let path = decode_field(bytes, MNTPATHLEN, "path")?;
        let mut groups = Vec::new();
        while has_next(bytes, "groups")? {
            groups.push(decode_field(bytes, MNTNAMLEN, "group")?);
        }
        result.push(ExportEntry { path, groups });
    }
    Ok(result)
}

impl Mount {
    pub(crate) async fn _export(&self) -> Result<Vec<ExportEntry>> {
        let mut buf = Vec::with_capacity(128);
        rpc_header(
            rpc::MOUNT_PROG,
            rpc::MOUNT3_VERSION,
            MountProc3::Export as u32,
            &self.auth,
        )
        .encode(&mut buf);
        let mut bytes = self
            .rpc
            .call(buf, super::NFS_REPLAY, super::METADATA_TIMEOUT)
            .await?;
        decode_exports(&mut bytes)
    }

    /// Returns the list of file systems exported by the server — equivalent to `showmount -e`.
    pub async fn export(&self) -> Result<Vec<ExportEntry>> {
        self._export().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_xdr_string(s: &[u8]) -> Vec<u8> {
        let len = s.len();
        let pad = (4 - len % 4) % 4;
        let mut v = Vec::new();
        v.extend_from_slice(&(len as u32).to_be_bytes());
        v.extend_from_slice(s);
        v.extend(std::iter::repeat_n(0u8, pad));
        v
    }

    fn build_export_response(entries: &[(&[u8], &[&[u8]])]) -> Vec<u8> {
        let mut v = Vec::new();
        for (path, groups) in entries {
            v.extend_from_slice(&1u32.to_be_bytes()); // more export entries
            v.extend(build_xdr_string(path));
            for group in *groups {
                v.extend_from_slice(&1u32.to_be_bytes()); // more groups
                v.extend(build_xdr_string(group));
            }
            v.extend_from_slice(&0u32.to_be_bytes()); // end of groups
        }
        v.extend_from_slice(&0u32.to_be_bytes()); // end of exports
        v
    }

    #[test]
    fn decode_exports_empty() {
        let raw = 0u32.to_be_bytes();
        let mut bytes = Bytes::from(raw.to_vec());
        let result = decode_exports(&mut bytes).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn decode_exports_single_no_groups() {
        let raw = build_export_response(&[(b"/vol/data", &[])]);
        let mut bytes = Bytes::from(raw);
        let result = decode_exports(&mut bytes).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path, "/vol/data");
        assert!(result[0].groups.is_empty());
    }

    #[test]
    fn decode_exports_single_with_groups() {
        let raw = build_export_response(&[(b"/exports/share", &[b"client1", b"10.0.0.0/24"])]);
        let mut bytes = Bytes::from(raw);
        let result = decode_exports(&mut bytes).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path, "/exports/share");
        assert_eq!(result[0].groups, vec!["client1", "10.0.0.0/24"]);
    }

    #[test]
    fn decode_exports_multiple_entries() {
        let raw = build_export_response(&[
            (b"/vol/home", &[b"@trusted"]),
            (b"/vol/data", &[]),
            (b"/vol/backup", &[b"host-a", b"host-b"]),
        ]);
        let mut bytes = Bytes::from(raw);
        let result = decode_exports(&mut bytes).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].path, "/vol/home");
        assert_eq!(result[0].groups, vec!["@trusted"]);
        assert_eq!(result[1].path, "/vol/data");
        assert!(result[1].groups.is_empty());
        assert_eq!(result[2].path, "/vol/backup");
        assert_eq!(result[2].groups, vec!["host-a", "host-b"]);
    }

    #[test]
    fn decode_exports_truncated_returns_error() {
        // Only the leading "1" (more entries) without any subsequent data.
        let raw = 1u32.to_be_bytes();
        let mut bytes = Bytes::from(raw.to_vec());
        assert!(decode_exports(&mut bytes).is_err());
    }

    #[test]
    fn decode_exports_borrows_raw_paths_and_groups() {
        let reply = Bytes::from(build_export_response(&[
            (b"/data/\xff", &[b"\xfe"]),
            (b"/valid", &[]),
        ]));
        let entries = decode_exports(&mut reply.clone()).unwrap();
        assert_eq!(entries[0].path.as_ref(), b"/data/\xff");
        assert_eq!(entries[0].path.as_ptr(), reply[8..].as_ptr());
        assert_eq!(entries[0].groups[0].as_ref(), b"\xfe");
        assert_eq!(entries[1].path.as_ref(), b"/valid");
    }

    #[test]
    fn decode_exports_checks_mount_field_limits() {
        let path = [&b"/"[..], &vec![b'a'; MNTPATHLEN as usize - 1]].concat();
        let group = vec![b'a'; MNTNAMLEN as usize];
        for (path, group, valid) in [
            (path.clone(), group.clone(), true),
            ([path.as_slice(), b"a"].concat(), group.clone(), false),
            (path, [group.as_slice(), b"a"].concat(), false),
        ] {
            let reply = build_export_response(&[(&path, &[&group])]);
            assert_eq!(decode_exports(&mut reply.into()).is_ok(), valid);
        }
    }
}
