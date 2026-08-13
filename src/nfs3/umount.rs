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

use super::{Mount, MountProc3, Result, encode_dirpath, rpc_header};
use crate::rpc;

#[allow(unused)]
impl Mount {
    pub async fn umount(&self) -> Result<()> {
        let mut buf = Vec::<u8>::new();
        rpc_header(
            rpc::MOUNT_PROG,
            rpc::MOUNT3_VERSION,
            MountProc3::Umount as u32,
            &self.auth,
        )
        .encode(&mut buf);
        encode_dirpath(&mut buf, self.dir.trim_end_matches('/'));

        // UMNT is advisory (RFC 1813 Appendix I) — use small retry count to avoid
        // blocking the caller when the server is unreachable.
        // 注意：不调用 self.rpc.shutdown()，连接由 Arc<StreamMux>::Drop 自然清理。
        // 在共享 Client 场景下，提前 shutdown 会摧毁其他持有者正在使用的连接。
        let result = self
            .rpc
            .call(buf, super::MOUNT_REPLAY, super::METADATA_TIMEOUT)
            .await;
        result.map(|_| ())
    }
}
