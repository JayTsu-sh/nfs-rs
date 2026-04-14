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

use crate::bindings::exports::component::nfs_rs::nfs::{
    Attr as WitAttr, Bytes as WitBytes, Error as WitError, Fh as WitFH, FsInfo as WitFSInfo,
    FsStat as WitFSStat, Guest as WitNFS, GuestNfsMount as WitMount, NfsMount as WitNFSMount,
    NfsVersion as WitNFSVersion, ObjRes as WitObjRes, PathConf as WitPathconf,
    ReaddirEntry as WitReaddirEntry, ReaddirplusEntry as WitReaddirplusEntry, Time as WitTime,
};
use bytes::Bytes;
use crate::{Attr, FSInfo, FSStat, Mount, NFSVersion, NfsError, ObjRes, Pathconf,
    ReaddirEntry, ReaddirplusEntry, Time,};
use crate::mount::block_on_compat;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

const FIRST_USABLE_HANDLE: u32 = 3;

static RESOURCES: LazyLock<Arc<RwLock<HashMap<u32, usize>>>> = LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

fn lock_err(msg: &str) -> WitError {
    WitError { nfs_error_code: None, message: msg.to_string() }
}

pub(crate) fn get_resource(handle: u32) -> Result<usize, WitError> {
    let resources = RESOURCES.read().map_err(|_| lock_err("RESOURCES read lock poisoned"))?;
    Ok(resources.get(&handle).copied().unwrap_or_default())
}

pub(crate) fn add_resource(res: usize) -> Result<u32, WitError> {
    let mut resources = RESOURCES.write().map_err(|_| lock_err("RESOURCES write lock poisoned"))?;
    let mut handle: u32 = rand::random();
    while handle < FIRST_USABLE_HANDLE || resources.contains_key(&handle) {
        handle = rand::random();
    }
    resources.insert(handle, res);
    Ok(handle)
}

pub(crate) fn remove_resource(handle: u32) {
    match RESOURCES.write() {
        Ok(mut resources) => { resources.remove(&handle); }
        Err(_) => tracing::warn!("RESOURCES write lock poisoned in remove_resource"),
    }
}

static MOUNTS: LazyLock<Arc<RwLock<HashMap<u32, Arc<RwLock<Box<dyn Mount>>>>>>> = LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

fn get_mount(mnt: u32) -> Result<Arc<RwLock<Box<dyn Mount>>>, WitError> {
    let mounts = MOUNTS.read().map_err(|_| lock_err("MOUNTS lock poisoned"))?;
    match mounts.get(&mnt) {
        Some(mount_guard) => Ok(mount_guard.clone()),
        None => Err(WitError {
            nfs_error_code: None,
            message: "mount not found".to_string(),
        }),
    }
}

fn read_mount(mount_guard: &RwLock<Box<dyn Mount>>) -> Result<std::sync::RwLockReadGuard<'_, Box<dyn Mount>>, WitError> {
    mount_guard.read().map_err(|_| lock_err("mount read lock poisoned"))
}

fn add_mount(mount: Box<dyn Mount>) -> Result<u32, WitError> {
    let mut mounts = MOUNTS.write().map_err(|_| lock_err("MOUNTS lock poisoned"))?;
    let mut mnt: u32 = rand::random();
    while mnt == 0 || mounts.contains_key(&mnt) {
        mnt = rand::random();
    }
    mounts.insert(mnt, Arc::new(RwLock::new(mount)));
    Ok(mnt)
}

fn remove_mount(mnt: u32) {
    match MOUNTS.write() {
        Ok(mut mounts) => { mounts.remove(&mnt); }
        Err(_) => tracing::warn!("MOUNTS write lock poisoned in remove_mount"),
    }
}

impl From<WitTime> for Time {
    fn from(time: WitTime) -> Self {
        Self {
            seconds: time.seconds,
            nseconds: time.nseconds,
        }
    }
}

impl From<Time> for WitTime {
    fn from(time: Time) -> Self {
        Self {
            seconds: time.seconds,
            nseconds: time.nseconds,
        }
    }
}

impl From<Attr> for WitAttr {
    fn from(attr: Attr) -> Self {
        Self {
            attr_type: attr.type_,
            file_mode: attr.file_mode,
            nlink: attr.nlink,
            uid: attr.uid,
            gid: attr.gid,
            filesize: attr.filesize,
            used: attr.used,
            spec_data: (attr.spec_data[0], attr.spec_data[1]),
            fsid: attr.fsid,
            fileid: attr.fileid,
            atime: attr.atime.into(),
            mtime: attr.mtime.into(),
            ctime: attr.ctime.into(),
        }
    }
}

impl From<ObjRes> for WitObjRes {
    fn from(obj: ObjRes) -> Self {
        Self {
            obj: obj.fh,
            attr: obj.attr.map(Into::into),
        }
    }
}

impl From<FSInfo> for WitFSInfo {
    fn from(info: FSInfo) -> Self {
        Self {
            attr: info.attr.map(Into::into),
            rtmax: info.rtmax,
            rtpref: info.rtpref,
            rtmult: info.rtmult,
            wtmax: info.wtmax,
            wtpref: info.wtpref,
            wtmult: info.wtmult,
            dtpref: info.dtpref,
            maxfilesize: info.maxfilesize,
            time_delta: info.time_delta.into(),
            properties: info.properties,
        }
    }
}

impl From<FSStat> for WitFSStat {
    fn from(stat: FSStat) -> Self {
        Self {
            attr: stat.attr.map(Into::into),
            tbytes: stat.tbytes,
            fbytes: stat.fbytes,
            abytes: stat.abytes,
            tfiles: stat.tfiles,
            ffiles: stat.ffiles,
            afiles: stat.afiles,
            invarsec: stat.invarsec,
        }
    }
}

impl From<Pathconf> for WitPathconf {
    fn from(conf: Pathconf) -> Self {
        Self {
            attr: conf.attr.map(Into::into),
            linkmax: conf.linkmax,
            name_max: conf.name_max,
            no_trunc: conf.no_trunc,
            chown_restricted: conf.chown_restricted,
            case_insensitive: conf.case_insensitive,
            case_preserving: conf.case_preserving,
        }
    }
}

impl From<ReaddirEntry> for WitReaddirEntry {
    fn from(entry: ReaddirEntry) -> Self {
        Self {
            fileid: entry.fileid,
            file_name: entry.file_name,
        }
    }
}

impl From<ReaddirplusEntry> for WitReaddirplusEntry {
    fn from(entry: ReaddirplusEntry) -> Self {
        Self {
            fileid: entry.fileid,
            file_name: entry.file_name,
            attr: entry.attr.map(Into::into),
            handle: entry.handle,
        }
    }
}

impl From<NfsError> for WitError {
    fn from(err: NfsError) -> WitError {
        match &err {
            NfsError::Nfs3(code) => WitError {
                nfs_error_code: Some(*code as i32),
                message: code.to_string(),
            },
            NfsError::Mount(code) => WitError {
                nfs_error_code: Some(*code as i32),
                message: code.to_string(),
            },
            _ => WitError {
                nfs_error_code: None,
                message: err.to_string(),
            },
        }
    }
}

impl WitNFS for crate::Component {
    type NfsMount = crate::NfsMount;

    fn parse_url_and_mount(url: String) -> Result<WitNFSMount, WitError> {
        let mount = block_on_compat(crate::parse_url_and_mount(&url)).map_err(Into::<WitError>::into)?;
        let id = add_mount(mount)?;
        Ok(WitNFSMount::new(crate::NfsMount { id }))
    }
}

impl WitMount for crate::NfsMount {
    fn null_op(&self) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_null().map_err(Into::into)
    }

    fn access(&self, fh: WitFH, mode: u32) -> Result<u32, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_access(Bytes::from(fh), mode).map_err(Into::into)
    }

    fn access_path(&self, path: String, mode: u32) -> Result<u32, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_access_path(&path, mode).map_err(Into::into)
    }

    fn open(&self, dir_fh: WitFH, filename: String, access: u32) -> Result<WitObjRes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_open(Bytes::from(dir_fh), &filename, access)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn open_path(&self, path: String, access: u32) -> Result<WitObjRes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_open_path(&path, access)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn close(&self, fh: WitFH) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_close(Bytes::from(fh)).map_err(Into::into)
    }

    fn commit(&self, fh: WitFH, offset: u64, count: u32) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_commit(Bytes::from(fh), offset, count).map_err(Into::into)
    }

    fn commit_path(&self, path: String, offset: u64, count: u32) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_commit_path(&path, offset, count).map_err(Into::into)
    }

    fn create(&self, dir_fh: WitFH, filename: String, mode: u32) -> Result<WitObjRes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_create(Bytes::from(dir_fh), &filename, mode)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn create_path(&self, path: String, mode: u32) -> Result<WitObjRes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_create_path(&path, mode)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn delegpurge(&self, clientid: u64) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_delegpurge(clientid).map_err(Into::into)
    }

    fn delegreturn(&self, stateid: u64) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_delegreturn(stateid).map_err(Into::into)
    }

    fn fsinfo(&self) -> Result<WitFSInfo, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_fsinfo().map(Into::into).map_err(Into::into)
    }

    fn fsstat(&self) -> Result<WitFSStat, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_fsstat().map(Into::into).map_err(Into::into)
    }

    fn getattr(&self, fh: WitFH) -> Result<WitAttr, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_getattr(Bytes::from(fh)).map(Into::into).map_err(Into::into)
    }

    fn getattr_path(&self, path: String) -> Result<WitAttr, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_getattr_path(&path)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn setattr(
        &self,
        fh: WitFH,
        guard_ctime: Option<WitTime>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<WitTime>,
        mtime: Option<WitTime>,
    ) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_setattr(
                Bytes::from(fh),
                guard_ctime.map(Into::into),
                mode,
                uid,
                gid,
                size,
                atime.map(Into::into),
                mtime.map(Into::into),
            )
            .map_err(Into::into)
    }

    fn setattr_path(
        &self,
        path: String,
        specify_guard: bool,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<WitTime>,
        mtime: Option<WitTime>,
    ) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_setattr_path(
                &path,
                specify_guard,
                mode,
                uid,
                gid,
                size,
                atime.map(Into::into),
                mtime.map(Into::into),
            )
            .map_err(Into::into)
    }

    fn getfh(&self) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        // sync_getfh returns the root Bytes file handle (infallible); WIT wraps it in result<_,error>.
        let _ = mount.sync_getfh();
        Ok(())
    }

    fn link(
        &self,
        src_fh: WitFH,
        dst_dir_fh: WitFH,
        dst_filename: String,
    ) -> Result<WitAttr, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_link(Bytes::from(src_fh), Bytes::from(dst_dir_fh), &dst_filename)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn link_path(&self, src_path: String, dst_path: String) -> Result<WitAttr, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_link_path(&src_path, &dst_path)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn symlink(
        &self,
        src_path: String,
        dst_dir_fh: WitFH,
        dst_filename: String,
    ) -> Result<WitObjRes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_symlink(&src_path, Bytes::from(dst_dir_fh), &dst_filename)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn symlink_path(&self, src_path: String, dst_path: String) -> Result<WitObjRes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_symlink_path(&src_path, &dst_path)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn readlink(&self, fh: WitFH) -> Result<String, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_readlink(Bytes::from(fh)).map_err(Into::into)
    }

    fn readlink_path(&self, path: String) -> Result<String, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_readlink_path(&path).map_err(Into::into)
    }

    fn lookup(&self, dir_fh: WitFH, filename: String) -> Result<WitObjRes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_lookup(Bytes::from(dir_fh), &filename)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn lookup_path(&self, path: String) -> Result<WitObjRes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_lookup_path(&path).map(Into::into).map_err(Into::into)
    }

    fn pathconf(&self, fh: WitFH) -> Result<WitPathconf, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_pathconf(Bytes::from(fh)).map(Into::into).map_err(Into::into)
    }

    fn pathconf_path(&self, path: String) -> Result<WitPathconf, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_pathconf_path(&path)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn read(&self, fh: WitFH, offset: u64, count: u32) -> Result<WitBytes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_read(Bytes::from(fh), offset, count)
            .map(|b| b.to_vec())
            .map_err(Into::into)
    }

    fn read_path(&self, path: String, offset: u64, count: u32) -> Result<WitBytes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_read_path(&path, offset, count)
            .map(|b| b.to_vec())
            .map_err(Into::into)
    }

    fn write(&self, fh: WitFH, offset: u64, data: WitBytes) -> Result<u32, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_write(Bytes::from(fh), offset, Bytes::from(data)).map_err(Into::into)
    }

    fn write_path(&self, path: String, offset: u64, data: WitBytes) -> Result<u32, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_write_path(&path, offset, Bytes::from(data)).map_err(Into::into)
    }

    fn readdir(&self, dir_fh: WitFH) -> Result<Vec<WitReaddirEntry>, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_readdir(Bytes::from(dir_fh))
            .map(|entries| entries.into_iter().map(Into::into).collect())
            .map_err(Into::into)
    }

    fn readdir_path(&self, dir_path: String) -> Result<Vec<WitReaddirEntry>, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_readdir_path(&dir_path)
            .map(|entries| entries.into_iter().map(Into::into).collect())
            .map_err(Into::into)
    }

    fn readdirplus(&self, dir_fh: WitFH) -> Result<Vec<WitReaddirplusEntry>, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_readdirplus(Bytes::from(dir_fh))
            .map(|entries| entries.into_iter().map(Into::into).collect())
            .map_err(Into::into)
    }

    fn readdirplus_path(&self, dir_path: String) -> Result<Vec<WitReaddirplusEntry>, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_readdirplus_path(&dir_path)
            .map(|entries| entries.into_iter().map(Into::into).collect())
            .map_err(Into::into)
    }

    fn mkdir(&self, dir_fh: WitFH, dirname: String, mode: u32) -> Result<WitObjRes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_mkdir(Bytes::from(dir_fh), &dirname, mode)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn mkdir_path(&self, path: String, mode: u32) -> Result<WitObjRes, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_mkdir_path(&path, mode)
            .map(Into::into)
            .map_err(Into::into)
    }

    fn remove(&self, dir_fh: WitFH, filename: String) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_remove(Bytes::from(dir_fh), &filename).map_err(Into::into)
    }

    fn remove_path(&self, path: String) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_remove_path(&path).map_err(Into::into)
    }

    fn rmdir(&self, dir_fh: WitFH, dirname: String) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_rmdir(Bytes::from(dir_fh), &dirname).map_err(Into::into)
    }

    fn rmdir_path(&self, path: String) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_rmdir_path(&path).map_err(Into::into)
    }

    fn rename(
        &self,
        from_dir_fh: WitFH,
        from_filename: String,
        to_dir_fh: WitFH,
        to_filename: String,
    ) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount
            .sync_rename(Bytes::from(from_dir_fh), &from_filename, Bytes::from(to_dir_fh), &to_filename)
            .map_err(Into::into)
    }

    fn rename_path(&self, from_path: String, to_path: String) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        mount.sync_rename_path(&from_path, &to_path).map_err(Into::into)
    }

    fn umount(&self) -> Result<(), WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        let ret = mount.sync_umount();
        if ret.is_ok() {
            remove_mount(self.id);
        }
        ret.map_err(Into::into)
    }

    fn version(&self) -> Result<WitNFSVersion, WitError> {
        let mount_guard = get_mount(self.id)?;
        let mount = read_mount(&mount_guard)?;
        match mount.version() {
            NFSVersion::NFSv3 => Ok(WitNFSVersion::NfsV3),
            NFSVersion::NFSv4 => Ok(WitNFSVersion::NfsV4),
            NFSVersion::NFSv4p1 => Ok(WitNFSVersion::NfsV4p1),
            _ => unreachable!(),
        }
    }
}
