//! In-memory namespace used only by deterministic Rust tests and test-support wheels.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

#[derive(Default)]
pub(crate) struct FixtureNamespace {
    pub v4: bool,
    pub calls: AtomicUsize,
}

impl FixtureNamespace {
    fn node(&self, path: &str, type_: u32) -> Node {
        Node {
            fh: Bytes::copy_from_slice(path.as_bytes()),
            attr: Attr {
                type_,
                fileid: 9,
                filesize: 12,
                ..Attr::default()
            },
        }
    }
}

#[async_trait]
impl Namespace for FixtureNamespace {
    async fn root(&self) -> Result<Node> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.node("", 2))
    }

    async fn child(&self, directory: Bytes, name: &[u8]) -> Result<Node> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if directory.is_empty() && name == b"raw-\xff" {
            let mut node = self.node("", 1);
            node.fh = Bytes::from_static(b"raw-\xff");
            return Ok(node);
        }
        let name = std::str::from_utf8(name).map_err(|error| NfsError::Xdr(error.to_string()))?;
        let directory =
            std::str::from_utf8(&directory).map_err(|error| NfsError::Xdr(error.to_string()))?;
        let path = if directory.is_empty() {
            name.into()
        } else {
            format!("{directory}/{name}")
        };
        match path.as_str() {
            "denied" => Err(if self.v4 {
                NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_ACCESS)
            } else {
                NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_ACCES)
            }),
            "disconnected" => Err(NfsError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "fixture disconnect",
            ))),
            "stale" => Err(if self.v4 {
                NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_STALE)
            } else {
                NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_STALE)
            }),
            "server-notdir" => Err(if self.v4 {
                NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_NOTDIR)
            } else {
                NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_NOTDIR)
            }),
            "nested" | "nested/deep" => Ok(self.node(&path, 2)),
            "file" | "nested/file" | "nested/peer" => Ok(self.node(&path, 1)),
            "link" | "dangling" | "absolute" | "outside" | "cycle" | "cycle-a" | "cycle-b"
            | "alias" | "deep-alias" | "nested/up" | "nested/back" | "empty" | "nul" | "long"
            | "many" | "slow" | "readlink-error" | "encoding-error" | "raw-link" => {
                Ok(self.node(&path, 5))
            }
            _ if path.starts_with("chain-") => Ok(self.node(&path, 5)),
            _ => Err(if self.v4 {
                NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_NOENT)
            } else {
                NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_NOENT)
            }),
        }
    }

    async fn link_target(&self, fh: Bytes) -> Result<Bytes> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let path = std::str::from_utf8(&fh).map_err(|error| NfsError::Xdr(error.to_string()))?;
        Ok(match path {
            "link" => "file".into(),
            "raw-link" => Bytes::from_static(b"raw-\xff"),
            "dangling" => "missing".into(),
            "absolute" => "/file".into(),
            "outside" => "../file".into(),
            "cycle" => "cycle".into(),
            "cycle-a" => "cycle-b".into(),
            "cycle-b" => "cycle-a".into(),
            "alias" => "nested".into(),
            "deep-alias" => "nested/deep".into(),
            "nested/up" => "../file".into(),
            "nested/back" => "..".into(),
            "empty" => Bytes::new(),
            "nul" => "bad\0name".into(),
            "long" => "x".repeat(MAX_PATH_BYTES + 1).into(),
            "many" => "./".repeat(MAX_COMPONENTS).into(),
            "slow" => std::future::pending().await,
            "readlink-error" => return Err(NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_IO)),
            "encoding-error" => return Err(NfsError::Xdr("malformed READLINK reply".into())),
            _ => {
                let number = path
                    .strip_prefix("chain-")
                    .and_then(|value| value.parse::<usize>().ok())
                    .ok_or_else(|| NfsError::InvalidInput("unknown fixture link".into()))?;
                if number == 0 {
                    "file".into()
                } else {
                    format!("chain-{}", number - 1).into()
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn raw_link_resolves_to_the_exact_target() {
        let result = resolve(&FixtureNamespace::default(), b"raw-link")
            .await
            .unwrap();
        assert_eq!(result.path.as_deref(), Some(b"raw-\xff".as_slice()));
        assert_eq!(result.object.unwrap().fh, b"raw-\xff"[..]);
    }

    #[tokio::test]
    async fn follows_final_and_intermediate_links_and_preserves_dotdot_order() {
        let namespace = FixtureNamespace::default();
        for (input, expected) in [
            ("", "."),
            (".", "."),
            ("file", "file"),
            ("link", "file"),
            ("nested/up", "file"),
            ("alias/file", "nested/file"),
            ("alias/up", "file"),
            ("alias/../link", "file"),
            ("deep-alias/../peer", "nested/peer"),
            ("nested/back/nested/file", "nested/file"),
            // Revisiting a link is not necessarily a cycle.
            ("alias/../alias/file", "nested/file"),
            ("alias/", "nested"),
        ] {
            let result = resolve(&namespace, input.as_bytes()).await.unwrap();
            assert_eq!(result.status, PathResolutionStatus::Resolved, "{input}");
            assert_eq!(result.path.as_deref(), Some(expected.as_bytes()), "{input}");
            assert_eq!(result.exists(), Some(true));
            assert!(result.object.unwrap().attr.is_some());
        }
    }

    #[tokio::test]
    async fn distinguishes_dangling_not_directory_and_unresolved_namespace() {
        for v4 in [false, true] {
            let namespace = FixtureNamespace {
                v4,
                ..Default::default()
            };
            for (input, status, exists) in [
                ("missing", PathResolutionStatus::Missing, Some(false)),
                ("dangling", PathResolutionStatus::Missing, Some(false)),
                ("empty", PathResolutionStatus::Missing, Some(false)),
                (
                    "file/child",
                    PathResolutionStatus::NotDirectory,
                    Some(false),
                ),
                ("file/..", PathResolutionStatus::NotDirectory, Some(false)),
                ("link/", PathResolutionStatus::NotDirectory, Some(false)),
                ("link/.", PathResolutionStatus::NotDirectory, Some(false)),
                (
                    "server-notdir",
                    PathResolutionStatus::NotDirectory,
                    Some(false),
                ),
                ("absolute", PathResolutionStatus::OutsideMount, None),
                ("outside", PathResolutionStatus::OutsideMount, None),
                (
                    "nested/../../file",
                    PathResolutionStatus::OutsideMount,
                    None,
                ),
                ("cycle", PathResolutionStatus::TooManySymlinks, None),
                ("cycle-a", PathResolutionStatus::TooManySymlinks, None),
            ] {
                let result = resolve(&namespace, input.as_bytes()).await.unwrap();
                assert_eq!(
                    (result.status, result.exists()),
                    (status, exists),
                    "{input}"
                );
                assert!(result.path.is_none() && result.object.is_none());
            }
        }
    }

    #[tokio::test]
    async fn permits_exactly_forty_links_and_stops_cycles() {
        let namespace = FixtureNamespace::default();
        assert_eq!(
            resolve(&namespace, b"chain-39").await.unwrap().exists(),
            Some(true)
        );
        assert_eq!(
            resolve(&namespace, b"chain-40").await.unwrap().status,
            PathResolutionStatus::TooManySymlinks
        );
        let before = namespace.calls.load(Ordering::Relaxed);
        resolve(&namespace, b"cycle").await.unwrap();
        assert_eq!(namespace.calls.load(Ordering::Relaxed) - before, 82);
    }

    #[tokio::test]
    async fn retains_permission_disconnect_stale_and_readlink_errors() {
        for v4 in [false, true] {
            let namespace = FixtureNamespace {
                v4,
                ..Default::default()
            };
            assert!(matches!(
                resolve(&namespace, b"denied").await,
                Err(NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_ACCES)
                    | NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_ACCESS))
            ));
            assert!(matches!(resolve(&namespace, b"disconnected").await,
                Err(NfsError::Io(error)) if error.kind() == std::io::ErrorKind::ConnectionReset));
            assert!(matches!(
                resolve(&namespace, b"stale").await,
                Err(NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_STALE)
                    | NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_STALE))
            ));
            assert!(matches!(
                resolve(&namespace, b"readlink-error").await,
                Err(NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_IO))
            ));
        }
    }

    #[tokio::test]
    async fn rejects_unbounded_or_invalid_text_before_lookup_and_during_expansion() {
        let namespace = FixtureNamespace::default();
        for path in [
            "/file".into(),
            "bad\0name".into(),
            "x".repeat(MAX_PATH_BYTES + 1),
            "./".repeat(MAX_COMPONENTS),
        ] {
            assert!(matches!(
                resolve(&namespace, path.as_bytes()).await,
                Err(NfsError::InvalidInput(_))
            ));
        }
        assert_eq!(namespace.calls.load(Ordering::Relaxed), 0);
        let at_limit = vec!["."; MAX_COMPONENTS].join("/");
        assert_eq!(
            resolve(&namespace, at_limit.as_bytes())
                .await
                .unwrap()
                .path
                .as_deref(),
            Some(b".".as_slice())
        );
        for path in ["nul", "long", "many"] {
            assert!(matches!(
                resolve(&namespace, path.as_bytes()).await,
                Err(NfsError::InvalidInput(_))
            ));
        }
        assert_eq!(
            resolve(&namespace, (".".repeat(MAX_PATH_BYTES)).as_bytes())
                .await
                .unwrap()
                .status,
            PathResolutionStatus::Missing
        );
    }

    #[tokio::test]
    async fn resolution_can_be_cancelled_while_readlink_is_pending() {
        let namespace = FixtureNamespace::default();
        assert!(
            timeout(Duration::from_millis(10), resolve(&namespace, b"slow"))
                .await
                .is_err()
        );
        let calls = namespace.calls.load(Ordering::Relaxed);
        tokio::task::yield_now().await;
        assert_eq!(namespace.calls.load(Ordering::Relaxed), calls);
        assert_eq!(
            resolve(&namespace, b"link").await.unwrap().exists(),
            Some(true)
        );
    }
}
