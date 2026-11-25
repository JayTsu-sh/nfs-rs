//! Client-side, mount-bounded symbolic-link traversal.
//!
//! LOOKUP does not follow links (RFC 1813 §3.3.3; RFC 8881 §18.15).
//! READLINK returns the stored target bytes for the client to interpret (RFC 8881 §18.24).

use std::collections::VecDeque;
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Attr, Mount, Nfs3ErrorCode, Nfs4ErrorCode, NfsError, ObjRes, Result};

const MAX_SYMLINKS: usize = 40;
const MAX_COMPONENTS: usize = 4096;
const MAX_PATH_BYTES: usize = 65536;

pub(crate) fn validate_name(name: &[u8]) -> Result<()> {
    if name.is_empty() || name.len() > MAX_PATH_BYTES || name.contains(&b'/') || name.contains(&0) {
        return Err(NfsError::InvalidInput("invalid NFS name component".into()));
    }
    Ok(())
}

pub(crate) fn validate_leaf(name: &[u8]) -> Result<()> {
    validate_name(name)?;
    if matches!(name, b"." | b"..") {
        return Err(NfsError::InvalidInput(
            "NFS mutation requires a leaf name".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_link_target(target: &[u8]) -> Result<()> {
    if target.len() > MAX_PATH_BYTES || target.contains(&0) {
        return Err(NfsError::InvalidInput(
            "NFS link target exceeds 64 KiB or contains NUL".into(),
        ));
    }
    Ok(())
}

/// READLINK returns opaque bytes. Limit the retained target before exposing it to callers.
pub(crate) fn checked_readlink_target(target: Bytes) -> Result<Bytes> {
    if target.len() > MAX_PATH_BYTES {
        return Err(NfsError::Xdr("READLINK target exceeds 64 KiB".into()));
    }
    Ok(target)
}

/// Interpret POSIX path syntax before submitting any LOOKUP. Backslashes
/// and all non-syntax bytes remain literal, regardless of the client OS.
pub(crate) fn path_components(path: &[u8]) -> Result<Vec<&[u8]>> {
    if path.len() > MAX_PATH_BYTES || path.contains(&0) {
        return Err(NfsError::InvalidInput(
            "NFS path exceeds 64 KiB or contains NUL".into(),
        ));
    }
    let mut components = Vec::new();
    for part in path.split(|byte| *byte == b'/') {
        match part {
            b"" | b"." => (),
            b".." => {
                components.pop().ok_or_else(|| {
                    NfsError::InvalidInput("NFS path escapes the export root".into())
                })?;
            }
            name => components.push(name),
        }
    }
    Ok(components)
}

pub(crate) fn split_byte_path(path: &[u8]) -> Result<(&[u8], &[u8])> {
    let components = path_components(path)?;
    if let Some(leaf) = path
        .rsplit(|byte| *byte == b'/')
        .find(|part| !part.is_empty())
    {
        validate_leaf(leaf)?;
    }
    let name = components
        .last()
        .ok_or_else(|| NfsError::InvalidInput("NFS path has no leaf name".into()))?;
    // Both slices borrow the original path. The parent lookup applies the
    // same path syntax to the prefix, including any canceled components.
    let offset = name.as_ptr().addr() - path.as_ptr().addr();
    Ok((&path[..offset], name))
}

/// Result of resolving a path within one mounted namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PathResolutionStatus {
    /// A final object and its attributes were obtained; this is not an access check.
    Resolved,
    /// A path component does not exist (including a dangling link target).
    Missing,
    /// A component that must be a directory is another object type.
    NotDirectory,
    /// An absolute link target or `..` requires a namespace outside this mount.
    OutsideMount,
    /// More than 40 symbolic links would need to be followed, possibly a cycle.
    TooManySymlinks,
}

/// An advisory namespace observation, not a promise about a subsequent open.
/// Permission, transport, stale-handle and other server errors remain errors.
#[derive(Clone, Debug, PartialEq)]
pub struct PathResolution {
    pub status: PathResolutionStatus,
    /// Canonical export-relative path, present only for `Resolved` (`.` for root).
    pub path: Option<Bytes>,
    /// Final handle and attributes, present only for `Resolved`.
    pub object: Option<ObjRes>,
}

impl PathResolution {
    /// `None` means the mount boundary or link limit prevented a determination.
    pub fn exists(&self) -> Option<bool> {
        match self.status {
            PathResolutionStatus::Resolved => Some(true),
            PathResolutionStatus::Missing | PathResolutionStatus::NotDirectory => Some(false),
            PathResolutionStatus::OutsideMount | PathResolutionStatus::TooManySymlinks => None,
        }
    }

    fn unresolved(status: PathResolutionStatus) -> Self {
        Self {
            status,
            path: None,
            object: None,
        }
    }
}

/// Only the three read-only namespace operations needed by the traversal.
/// The fixture uses this same algorithm without implementing unrelated Mount I/O.
#[async_trait]
pub(crate) trait Namespace: Sync {
    async fn root(&self) -> Result<Node>;
    async fn child(&self, directory: Bytes, name: &[u8]) -> Result<Node>;
    async fn link_target(&self, fh: Bytes) -> Result<Bytes>;
}

pub(crate) struct Node {
    pub fh: Bytes,
    pub attr: Attr,
}

async fn complete_node<M: Mount + ?Sized>(mount: &M, object: ObjRes) -> Result<Node> {
    let attr = match object.attr {
        Some(attr) => attr,
        None => mount.getattr(object.fh.clone()).await?,
    };
    if !(1..=7).contains(&attr.type_) {
        return Err(NfsError::Xdr(
            "path resolution requires a valid object type".into(),
        ));
    }
    Ok(Node {
        fh: object.fh,
        attr,
    })
}

#[async_trait]
impl<M: Mount + ?Sized> Namespace for M {
    async fn root(&self) -> Result<Node> {
        // Empty path selects the mounted root in every protocol engine.
        complete_node(self, self.lookup_path(b"").await?).await
    }

    async fn child(&self, directory: Bytes, name: &[u8]) -> Result<Node> {
        complete_node(self, self.lookup(directory, name).await?).await
    }

    async fn link_target(&self, fh: Bytes) -> Result<Bytes> {
        self.readlink(fh).await
    }
}

fn unavailable(error: &NfsError) -> Option<PathResolutionStatus> {
    match error {
        NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_NOENT)
        | NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_NOENT) => Some(PathResolutionStatus::Missing),
        NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_NOTDIR)
        | NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_NOTDIR) => Some(PathResolutionStatus::NotDirectory),
        _ => None,
    }
}

enum Component<'a> {
    Input(&'a [u8]),
    Link(Bytes),
}

impl<'a> Component<'a> {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Input(bytes) => bytes,
            Self::Link(bytes) => bytes,
        }
    }

    fn slice(&self, range: Range<usize>) -> Self {
        match self {
            Self::Input(bytes) => Self::Input(&bytes[range]),
            Self::Link(bytes) => Self::Link(bytes.slice(range)),
        }
    }
}

fn prepend<'a>(
    path: &Component<'a>,
    pending: &mut VecDeque<Component<'a>>,
    bytes: &mut usize,
    components: &mut usize,
) -> Result<()> {
    let path_bytes = path.bytes();
    *bytes = bytes.saturating_add(path_bytes.len());
    if *bytes > MAX_PATH_BYTES || path_bytes.contains(&0) {
        return Err(NfsError::InvalidInput(
            "path resolution exceeds 64 KiB of cumulative path bytes or contains NUL".into(),
        ));
    }
    let mut push = |part| {
        *components += 1;
        if *components > MAX_COMPONENTS {
            return Err(NfsError::InvalidInput(
                "path resolution exceeds 4096 cumulative components".into(),
            ));
        }
        pending.push_front(part);
        Ok(())
    };
    // A trailing slash requires a directory, even when the last name is a link.
    if path_bytes.ends_with(b"/") {
        push(Component::Input(b"."))?;
    }
    let mut end = path_bytes.len();
    for part in path_bytes.rsplit(|byte| *byte == b'/') {
        let start = end - part.len();
        if !part.is_empty() {
            push(path.slice(start..end))?;
        }
        end = start.saturating_sub(1);
    }
    Ok(())
}

pub(crate) async fn resolve<N: Namespace + ?Sized>(
    namespace: &N,
    path: &[u8],
) -> Result<PathResolution> {
    if path.starts_with(b"/") {
        return Err(NfsError::InvalidInput(
            "resolve paths must be export-relative".into(),
        ));
    }
    let mut pending = VecDeque::new();
    let (mut bytes, mut components) = (0, 0);
    prepend(
        &Component::Input(path),
        &mut pending,
        &mut bytes,
        &mut components,
    )?;
    let mut stack = vec![(Component::Input(b""), namespace.root().await?)];
    let mut links = 0;
    while let Some(name) = pending.pop_front() {
        let Some((_, directory)) = stack.last() else {
            return Err(NfsError::Xdr("path resolution lost its root".into()));
        };
        if directory.attr.type_ != 2 {
            return Ok(PathResolution::unresolved(
                PathResolutionStatus::NotDirectory,
            ));
        }
        match name.bytes() {
            b"." => continue,
            b".." => {
                if stack.len() == 1 {
                    return Ok(PathResolution::unresolved(
                        PathResolutionStatus::OutsideMount,
                    ));
                }
                stack.pop();
                continue;
            }
            _ => {}
        }
        let node = match namespace.child(directory.fh.clone(), name.bytes()).await {
            Ok(node) => node,
            Err(error) => match unavailable(&error) {
                Some(status) => return Ok(PathResolution::unresolved(status)),
                None => return Err(error),
            },
        };
        if node.attr.type_ == 5 {
            if links == MAX_SYMLINKS {
                return Ok(PathResolution::unresolved(
                    PathResolutionStatus::TooManySymlinks,
                ));
            }
            links += 1;
            let target = namespace.link_target(node.fh).await?;
            prepend(
                &Component::Link(target.clone()),
                &mut pending,
                &mut bytes,
                &mut components,
            )?;
            // Absolute links refer to the client's root namespace, which this
            // userspace mount cannot map. Never reinterpret them as export-relative.
            if target.starts_with(b"/") {
                return Ok(PathResolution::unresolved(
                    PathResolutionStatus::OutsideMount,
                ));
            }
            if target.is_empty() {
                return Ok(PathResolution::unresolved(PathResolutionStatus::Missing));
            }
        } else {
            stack.push((name, node));
        }
    }
    let mut resolved_path = Vec::with_capacity(
        stack
            .iter()
            .skip(1)
            .map(|(name, _)| name.bytes().len() + 1)
            .sum(),
    );
    for (name, _) in stack.iter().skip(1) {
        if !resolved_path.is_empty() {
            resolved_path.push(b'/');
        }
        resolved_path.extend_from_slice(name.bytes());
    }
    let Some((_, node)) = stack.pop() else {
        return Err(NfsError::Xdr(
            "path resolution lost its final object".into(),
        ));
    };
    Ok(PathResolution {
        status: PathResolutionStatus::Resolved,
        path: Some(if resolved_path.is_empty() {
            Bytes::from_static(b".")
        } else {
            Bytes::from(resolved_path)
        }),
        object: Some(ObjRes {
            fh: node.fh,
            attr: Some(node.attr),
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_paths_borrow_raw_bytes_and_protect_the_export_root() {
        let path = b"/parent/cancel/../raw-\xff/";
        let (parent, leaf) = split_byte_path(path).unwrap();
        assert_eq!(path_components(parent).unwrap(), [b"parent"]);
        assert_eq!(leaf, b"raw-\xff");
        assert_eq!(leaf.as_ptr(), path[18..].as_ptr());
        for path in [b"".as_slice(), b"/", b"a/..", b"a/.", b"../escape", b"a/\0"] {
            assert!(split_byte_path(path).is_err());
        }
    }
}

#[cfg(any(test, feature = "python-test-support"))]
pub(crate) mod fixture;
