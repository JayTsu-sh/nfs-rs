# Native POSIX ACLs over NFSv3

The Rust API added in 0.9.0 exposes the optional Sun NFSACL extension (RPC program
100227, version 3). Use `Mount::get_posix_acl` and `Mount::set_posix_acl` with an
NFSv3 file handle. NFSv4 engines return `NfsError::Unsupported` for these methods;
their existing `getacl`/`setacl` API operates on NFSv4 ACLs.
The `get_posix_acl_path` and `set_posix_acl_path` methods perform LOOKUP before
delegating to the same native ACL operations.

## Representation

`PosixAcl` contains two `Vec<PosixAclEntry>` lists: `access` and `default`.
Each entry has `tag`, `id`, and `permissions` fields of type `u32`.

| Tag | Subject | Identifier |
| --- | --- | --- |
| 1 | Owner | Server-defined qualifier |
| 2 | Named user | UID |
| 4 | Owning group | Server-defined qualifier |
| 8 | Named group | GID |
| 16 | Mask | Server-defined qualifier |
| 32 | Other | Server-defined qualifier |

Permissions are the raw `rwx` bits: read 4, write 2, execute 1. A named entry with
permissions 7 and mask 4 retains both values. The client does not discard masked
permissions or convert ACLs to Linux xattr bytes. Default entries use the same
tags as access entries; the wire-only default bit `0x1000` is added and removed
by the client.

Linux nfsd can return actual owner/group IDs for object entries and zero for
mask/other entries. POSIX xattrs normally use undefined IDs for those entries.
Only named user/group IDs have portable qualifier meaning. Comparing ACLs
between servers therefore requires caller-defined identity handling.

A mode-only ACL can contain four entries, including a synthesized mask equal to
the owning-group permissions. The native transport retains that mask. It does
not normalize the list into a three-entry POSIX xattr ACL.

## Reading and replacing ACLs

```rust
use nfs_rs::{Result, parse_url_and_mount};

#[tokio::main]
async fn main() -> Result<()> {
    let mount = parse_url_and_mount("nfs://127.0.0.1/export?version=3").await?;
    let mut acl = mount.get_posix_acl_path("directory").await?;
    acl.default.clear();
    mount.set_posix_acl_path("directory", &acl).await?;
    let actual = mount.get_posix_acl_path("directory").await?;
    assert!(actual.default.is_empty());
    mount.umount().await?;
    Ok(())
}
```

SETACL always sends both complete lists. To preserve defaults while changing
access entries, retain the default list from GETACL. To delete defaults, send an
empty default list. The client checks entry tags, permission bounds and the
1024-entry limit for each list; the server validates the complete POSIX ACL.

## Capability and failure behavior

`MountCapabilities::posix_acl` is true only after an actual NFSACL NULL probe.
The client first probes the existing NFS TCP connection, then uses portmapper to
discover a dedicated extension port when needed. The optional discovery has a
five-second deadline and preserves the mount's AUTH_SYS identity and source-port
policy. A failed probe leaves ordinary NFS operations available.

The mount capability describes extension availability, not guaranteed ACL support
for every object. An individual GETACL or SETACL can return `Unsupported` without
disabling ACL operations for other objects. A GETACL reply missing a requested
ACL mask bit, an entry count, or array bytes is an error; omitted ACL data is not
treated as an empty ACL.

Unknown or truncated NFS status fields retain the concrete decoder error inside
`NfsError::Io` with `ErrorKind::InvalidData`, accessible through the I/O error's
`get_ref()`. The operation wrapper still reports GETACL as `SafeToRetry` and
SETACL as `Uncertain`; callers should inspect that operation disposition before
the underlying error kind.

SETACL uses one transport attempt. Lost or malformed replies retain the existing
`OperationOutcome::Uncertain` / `RecoveryAction::VerifyThenResume` guidance.
Linux nfsd applies the access ACL before the default ACL; a later error can leave
the access ACL changed. Read back after failed or uncertain writes before
assuming the destination retained its old ACL or retrying the operation.

The API transfers native state. Applications are responsible for validating
identity mappings and ACL equivalence between endpoints, and for any conversion
between POSIX and NFSv4 permission models.

## Protocol references

NFSACL is a Sun extension without an NFS RFC. The wire layout follows the Linux
v6.12 [NFSACL server implementation](https://github.com/torvalds/linux/blob/v6.12/fs/nfsd/nfs3acl.c)
and [shared encoder/decoder](https://github.com/torvalds/linux/blob/v6.12/fs/nfs_common/nfsacl.c).
RPC service unavailability follows the `accept_stat` values in
[RFC 5531 section 9](https://www.rfc-editor.org/rfc/rfc5531.html#section-9).
