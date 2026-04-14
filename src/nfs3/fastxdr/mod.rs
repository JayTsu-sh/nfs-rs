// Auto-generated NFS3 XDR types via fastxdr.
// Response decoding uses TryFrom<Bytes>.

#[allow(unused, non_camel_case_types, dead_code)]
mod nfs_generated {
    include!(concat!(env!("OUT_DIR"), "/nfs_xdr.rs"));
}

#[allow(unused, non_camel_case_types, dead_code)]
mod mount_generated {
    include!(concat!(env!("OUT_DIR"), "/mount_xdr.rs"));
}

use bytes::Bytes;

// Non-generic response types — re-export directly.
pub(crate) use nfs_generated::xdr::{
    ACCESS3resok, FSINFO3resok, FSSTAT3resok, GETATTR3resok, LINK3resok, PATHCONF3resok,
    REMOVE3resok, RENAME3resok, RMDIR3resok, SETATTR3resok,
};

// Shared types used by response inspection.
pub use nfs_generated::xdr::{fattr3, nfsstat3, nfstime3, post_op_attr};

// Generic response types — concretised with Bytes.
pub(crate) type COMMIT3resok = nfs_generated::xdr::COMMIT3resok<Bytes>;
pub(crate) type CREATE3resok = nfs_generated::xdr::CREATE3resok<Bytes>;
pub(crate) type LOOKUP3resok = nfs_generated::xdr::LOOKUP3resok<Bytes>;
pub(crate) type MKDIR3resok = nfs_generated::xdr::MKDIR3resok<Bytes>;
pub(crate) type READ3resok = nfs_generated::xdr::READ3resok<Bytes>;
pub(crate) type READDIR3resok = nfs_generated::xdr::READDIR3resok<Bytes>;
pub(crate) type READDIRPLUS3resok = nfs_generated::xdr::READDIRPLUS3resok<Bytes>;
pub(crate) type READLINK3resok = nfs_generated::xdr::READLINK3resok<Bytes>;
pub(crate) type SYMLINK3resok = nfs_generated::xdr::SYMLINK3resok<Bytes>;
pub(crate) type WRITE3resok = nfs_generated::xdr::WRITE3resok<Bytes>;

// Generic helper types used in readdir/readdirplus and response inspection.
// Names are kept lowercase to match XDR spec conventions.
#[allow(non_camel_case_types)]
pub(crate) type entry3 = nfs_generated::xdr::entry3<Bytes>;
#[allow(non_camel_case_types)]
pub(crate) type entryplus3 = nfs_generated::xdr::entryplus3<Bytes>;
#[allow(non_camel_case_types)]
pub(crate) type post_op_fh3 = nfs_generated::xdr::post_op_fh3<Bytes>;

// Write stability enum.
#[allow(non_camel_case_types)]
pub(crate) use nfs_generated::xdr::stable_how;

// Mount protocol types.
#[allow(non_camel_case_types)]
pub(crate) type mountres3_ok = mount_generated::xdr::mountres3_ok<Bytes>;
pub use mount_generated::xdr::mountstat3 as mount_mountstat3;
