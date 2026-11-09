//! Client-side, mount-bounded symbolic-link traversal.
//!
//! LOOKUP does not follow links (RFC 1813 §3.3.3; RFC 8881 §18.15).
//! READLINK returns text for the client to interpret (RFC 8881 §18.24).

use std::collections::VecDeque;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Attr, Mount, Nfs3ErrorCode, Nfs4ErrorCode, NfsError, ObjRes, Result};

const MAX_SYMLINKS: usize = 40;
const MAX_COMPONENTS: usize = 4096;
const MAX_PATH_BYTES: usize = 65536;

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
    pub path: Option<String>,
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
    async fn child(&self, directory: Bytes, name: &str) -> Result<Node>;
    async fn link_text(&self, fh: Bytes) -> Result<String>;
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
        complete_node(self, self.lookup_path("").await?).await
    }

    async fn child(&self, directory: Bytes, name: &str) -> Result<Node> {
        complete_node(self, self.lookup(directory, name).await?).await
    }

    async fn link_text(&self, fh: Bytes) -> Result<String> {
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

fn prepend(
    text: &str,
    pending: &mut VecDeque<String>,
    bytes: &mut usize,
    components: &mut usize,
) -> Result<()> {
    *bytes = bytes.saturating_add(text.len());
    if *bytes > MAX_PATH_BYTES {
        return Err(NfsError::InvalidInput(
            "path resolution exceeds 64 KiB of cumulative path text".into(),
        ));
    }
    let count =
        text.split('/').filter(|part| !part.is_empty()).count() + usize::from(text.ends_with('/'));
    *components = components.saturating_add(count);
    if *components > MAX_COMPONENTS {
        return Err(NfsError::InvalidInput(
            "path resolution exceeds 4096 cumulative components".into(),
        ));
    }
    if text.contains('\0') {
        return Err(NfsError::InvalidInput(
            "path resolution text contains NUL".into(),
        ));
    }
    // A trailing slash requires a directory, even when the last name is a link.
    if text.ends_with('/') {
        pending.push_front(".".into());
    }
    for part in text.rsplit('/').filter(|part| !part.is_empty()) {
        pending.push_front(part.into());
    }
    Ok(())
}

pub(crate) async fn resolve<N: Namespace + ?Sized>(
    namespace: &N,
    path: &str,
) -> Result<PathResolution> {
    if path.starts_with('/') {
        return Err(NfsError::InvalidInput(
            "resolve paths must be export-relative".into(),
        ));
    }
    let mut pending = VecDeque::new();
    let (mut bytes, mut components) = (0, 0);
    prepend(path, &mut pending, &mut bytes, &mut components)?;
    let mut stack = vec![(String::new(), namespace.root().await?)];
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
        match name.as_str() {
            "." => continue,
            ".." => {
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
        let node = match namespace.child(directory.fh.clone(), &name).await {
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
            let target = namespace.link_text(node.fh).await?;
            prepend(&target, &mut pending, &mut bytes, &mut components)?;
            // Absolute links refer to the client's root namespace, which this
            // userspace mount cannot map. Never reinterpret them as export-relative.
            if target.starts_with('/') {
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
    let resolved_path = stack
        .iter()
        .skip(1)
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join("/");
    let Some((_, node)) = stack.pop() else {
        return Err(NfsError::Xdr(
            "path resolution lost its final object".into(),
        ));
    };
    Ok(PathResolution {
        status: PathResolutionStatus::Resolved,
        path: Some(if resolved_path.is_empty() {
            ".".into()
        } else {
            resolved_path
        }),
        object: Some(ObjRes {
            fh: node.fh,
            attr: Some(node.attr),
        }),
    })
}

#[cfg(any(test, feature = "python-test-support"))]
pub(crate) mod fixture;
