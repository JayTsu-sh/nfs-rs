# Lossless names: second-review verification

Base: PR #172 after merging upstream `172accd`, reviewed as `e43082b`.
Host: Rocky Linux 9.7, Rust 1.95.0, CPython 3.11. The source and wheels were
built in an isolated temporary directory; no running application was replaced.

## Findings and implementation

1. A public `MountOptions::from_url("nfs://server/data/%FF?version=3")`
   regression test fails on `e43082b`: URL parsing insists on UTF-8 after
   percent decoding. The corrected parser keeps the connection URL and host as
   text and stores decoded export paths as `Bytes`. NFSv3 MNT and UMNT encode
   the same path bytes; NFSv4.0/4.1/4.2 export traversal sends byte components.
   Encoded slashes and NUL remain rejected; percent decoding occurs once.
   MNT/UMNT also preserve root and trailing-slash spellings instead of trimming
   the root to an empty path.
2. Python ordinary path normalization and Rust `split_path` already collapsed
   parent components before this PR. The byte revision additionally aligned
   direct Rust NFSv4 `lookup_path` with lexical normalization, where earlier
   implementations forwarded parent components. The contract now states this
   breaking behavior explicitly. `resolve`/`resolve_path` preserves traversal
   order, follows symlinks, and distinguishes missing entries, non-directory
   components and mount boundaries. Resolution is advisory, not an atomic
   authorization or mutation primitive.
3. Upstream added real-lab recovery tests with text arguments during the merge.
   Those call sites now explicitly pass path bytes, so all targets compile.
4. CI run `45551377512` tested `e43082b`, not the unpushed working tree.
   Its artifact smoke script also retained two text paths. The failure was
   reproduced against the installed wheel; both sync and async calls now use
   bytes and the smoke script passes. A passing installed pytest suite alone
   did not cover this separate artifact script.

## Deterministic checks

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo check --locked --all-targets` | Passed |
| `cargo clippy --locked --all-targets -- -D warnings` | Passed |
| `cargo test --locked --all-targets --no-fail-fast` | 909 passed, 33 ignored lab tests; benchmark test mode passed |
| `python3.11 scripts/check-reliability-test-results.py --results <rust-log>` | Concrete test mappings passed; existing partial/unmapped scenarios remain reported |
| `cargo test --locked --features python-bindings --lib -q` | 872 passed |
| `cargo test --locked --doc -q` | 49 passed |
| `cargo clippy --locked --features python-bindings --lib -- -D warnings` | Passed |
| `cargo check --locked --features python-extension` | Passed |
| Current-source test-support wheel, installed in a fresh venv; `NFS_RS_TEST_INSTALLED=1 python -m pytest -q python/tests` | 440 passed, 1 skipped |
| `python -m mypy.stubtest nfs_rs --ignore-missing-stub --allowlist python/stubtest-allowlist.txt` | Passed |
| `python scripts/smoke-python-artifact.py` against installed test-support wheel | Passed after reproducing the old script's TypeError |

The added wire-level tests verify EXPORT-to-URL-to-MNT-to-UMNT bytes; NFSv4
export traversal; paginated raw names versus a literal `~nfs-rs-raw:FF`
sibling followed by RENAME and REMOVE in v3, v4.0, v4.1 and v4.2; and v4.0
recovery reopening the exact non-UTF-8 name after reclaim rejection. Existing
OPEN/CREATE/LINK/SYMLINK codec tests and installed byte-name I/O tests also ran.
Python and Rust contract tests distinguish lexical `missing/../file` from
ordered resolution and cover regular-file and symlink parent components.

## Real server checks and limits

A separate **production wheel without `python-test-support`** was installed
for physical-server checks. The test-support wheel intentionally supplies
synthetic metadata for contract tests and is not used to prove live stat/exists.

On the existing user-provided share, an isolated UUID directory contained UTF-8
Chinese, GBK bytes, invalid UTF-8, newline/backslash names, and a literal former
prefix sibling. NFSv3 and NFSv4.0 both passed create/write/list/read, rename,
hard link, symlink/readlink, remove, mkdir/rmdir, and sibling-preservation checks.
The Linux-mounted view matched the listed raw bytes. NFSv4.0 also mounted a
non-UTF-8 subtree through its percent-encoded URL. NFSv3's subtree MNT was
rejected with `MNT3ERR_ACCES`; the client preserved that error. NFSv4.1 and
NFSv4.2 were rejected by that server with minor-version mismatch. Their raw
namespace behavior is covered by deterministic protocol fixtures, not claimed
as physical-server verification. Test-owned files and directories were cleaned.

No service restart, physical state-loss injection, or cross-environment
performance release gate was performed in this round. The previous
[byte-model CPU benchmark](../benchmarks/byte-name-model-2026-11-24.md) remains
supplemental performance evidence; the export control-path fix does not change
the file I/O data path.

## Architecture review

Percent decoding belongs at the connection URL boundary; MOUNT and NFSv4
navigation consume those bytes without charset policy. Protocol error handling,
replay rules and recovery ownership remain in their existing engines. Display
decoding stays in callers. Regression tests live with the affected protocol
modules, and the ordinary-versus-explicit-resolution distinction is documented
at the public Rust/Python boundaries.
