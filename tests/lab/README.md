# Terrasync integration lab

The lab is shared by `nfs-rs`, `data-mover-rs`, and `terrasync-rs`.

| Role | Management | Data | Services |
|---|---|---|---|
| Controller | 10.131.9.11 | 10.10.1.11 | GitHub Actions Runner |
| Source | 10.131.9.12 | 10.10.1.12 | NFSv3, NFSv4.1, RustFS |
| Destination | 10.131.9.13 | 10.10.1.13 | NFSv3, NFSv4.1, RustFS |
| Worker | 10.131.9.14 | 10.10.1.14 | RustFS, fault injection |

Every run must call `prepare-run.sh` with a unique `nightly-*` or `release-*`
identifier and call `cleanup-run.sh` from an `always()` step.

Management traffic uses `10.131.9.0/20`. Test data uses `10.10.1.0/24`.
Credentials are provisioned on the self-hosted runner and must not be committed.

`capability-report.sh` performs read-only discovery of the NFS implementation,
pNFS configuration, installed fault tools, repository-owned lab commands, and
the `ci-runner` sudo allow-list. Nightly uploads its output as an artifact. It
must not change service or network state.

`run-e2e.sh` mounts the isolated run directory on both source and destination
over NFSv3 and NFSv4.1. For each endpoint it exercises server discovery,
directory and file creation, chunked write/commit/read, attributes, READDIR,
READDIRPLUS, rename, hard links, symbolic links, removal, and unmount.

The Rust integration test is ignored by default and requires both
`NFS_RS_LAB_E2E=1` and a whitespace-separated `NFS_RS_LAB_URLS` value. This
keeps normal CI from accidentally accessing the private lab.
