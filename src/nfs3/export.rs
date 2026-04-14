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

use super::{rpc_header, Mount, MountProc3};
use crate::mount::ExportEntry;
use crate::error::{NfsError, Result};
use crate::rpc;

/// Decode one XDR variable-length string from `bytes`, consuming the data and its 4-byte padding.
fn decode_xdr_string(bytes: &mut Bytes) -> Result<String> {
    if bytes.remaining() < 4 {
        return Err(NfsError::Xdr("response truncated (string length)".to_string()));
    }
    let len = bytes.get_u32() as usize; // big-endian, advances by 4
    let pad = (4 - len % 4) % 4;
    if bytes.remaining() < len + pad {
        return Err(NfsError::Xdr("response truncated (string data)".to_string()));
    }
    let s = std::str::from_utf8(&bytes[..len])
        .map_err(|e| NfsError::Xdr(e.to_string()))?
        .to_string();
    bytes.advance(len + pad);
    Ok(s)
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
    loop {
        if bytes.remaining() < 4 {
            return Err(NfsError::Xdr("response truncated (export opt)".to_string()));
        }
        if bytes.get_u32() == 0 {
            break;
        }
        let path = decode_xdr_string(bytes)?;
        let mut groups = Vec::new();
        loop {
            if bytes.remaining() < 4 {
                return Err(NfsError::Xdr("response truncated (group opt)".to_string()));
            }
            if bytes.get_u32() == 0 {
                break;
            }
            groups.push(decode_xdr_string(bytes)?);
        }
        result.push(ExportEntry { path, groups });
    }
    Ok(result)
}

impl Mount {
    pub(crate) async fn _export(&self) -> Result<Vec<ExportEntry>> {
        let mut buf = Vec::with_capacity(128);
        rpc_header(rpc::MOUNT_PROG, rpc::MOUNT3_VERSION, MountProc3::Export as u32, &self.auth)
            .encode(&mut buf);
        let mut bytes = self.rpc.call(buf, super::NFS_RETRIES, super::METADATA_TIMEOUT).await?;
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

    fn build_xdr_string(s: &str) -> Vec<u8> {
        let len = s.len();
        let pad = (4 - len % 4) % 4;
        let mut v = Vec::new();
        v.extend_from_slice(&(len as u32).to_be_bytes());
        v.extend_from_slice(s.as_bytes());
        v.extend(std::iter::repeat(0u8).take(pad));
        v
    }

    fn build_export_response(entries: &[(&str, &[&str])]) -> Vec<u8> {
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
        let raw = build_export_response(&[("/vol/data", &[])]);
        let mut bytes = Bytes::from(raw);
        let result = decode_exports(&mut bytes).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path, "/vol/data");
        assert!(result[0].groups.is_empty());
    }

    #[test]
    fn decode_exports_single_with_groups() {
        let raw = build_export_response(&[("/exports/share", &["client1", "10.0.0.0/24"])]);
        let mut bytes = Bytes::from(raw);
        let result = decode_exports(&mut bytes).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path, "/exports/share");
        assert_eq!(result[0].groups, vec!["client1", "10.0.0.0/24"]);
    }

    #[test]
    fn decode_exports_multiple_entries() {
        let raw = build_export_response(&[
            ("/vol/home", &["@trusted"]),
            ("/vol/data", &[]),
            ("/vol/backup", &["host-a", "host-b"]),
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
}
