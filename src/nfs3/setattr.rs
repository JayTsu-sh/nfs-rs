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
    nfs_fh3, nfstime3_req, sattr3, sattrguard3, set_atime, set_gid3, set_mode3, set_mtime,
    set_size3, set_uid3, Mount, Result, SETATTR3args, Time,
};
use bytes::Bytes;

impl Mount {
    #[allow(clippy::too_many_arguments)]
    pub async fn setattr_path(
        &self,
        path: &str,
        specify_guard: bool,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Time>,
        mtime: Option<Time>,
    ) -> Result<()> {
        let res = self.lookup_path(path).await?;
        let guard_ctime = match (specify_guard, res.attr) {
            (true, Some(attr)) => Some(Time {
                seconds: attr.ctime.seconds,
                nseconds: attr.ctime.nseconds,
            }),
            _ => None,
        };
        self.setattr(res.fh, guard_ctime, mode, uid, gid, size, atime, mtime)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn setattr(
        &self,
        fh: Bytes,
        guard_ctime: Option<Time>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Time>,
        mtime: Option<Time>,
    ) -> Result<()> {
        let args = SETATTR3args {
            object: nfs_fh3 { data: fh },
            new_attributes: sattr3 {
                mode: mode.map_or(set_mode3::default, set_mode3::TRUE),
                uid: uid.map_or(set_uid3::default, set_uid3::TRUE),
                gid: gid.map_or(set_gid3::default, set_gid3::TRUE),
                size: size.map_or(set_size3::default, set_size3::TRUE),
                atime: atime.map_or(set_atime::default, |a| {
                    set_atime::SET_TO_CLIENT_TIME(nfstime3_req {
                        seconds: a.seconds,
                        nseconds: a.nseconds,
                    })
                }),
                mtime: mtime.map_or(set_mtime::default, |m| {
                    set_mtime::SET_TO_CLIENT_TIME(nfstime3_req {
                        seconds: m.seconds,
                        nseconds: m.nseconds,
                    })
                }),
            },
            guard: match guard_ctime {
                Some(ctime) => sattrguard3::TRUE(nfstime3_req {
                    seconds: ctime.seconds,
                    nseconds: ctime.nseconds,
                }),
                None => sattrguard3::FALSE,
            },
        };
        self._setattr(args).await?;
        Ok(())
    }
}
