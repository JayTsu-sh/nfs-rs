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
| `cargo test --locked --all-targets --no-fail-fast` | 911 passed, 33 ignored lab tests; benchmark test mode passed |
| `python3.11 scripts/check-reliability-test-results.py --results <rust-log>` | Concrete test mappings passed; existing partial/unmapped scenarios remain reported |
| `cargo test --locked --features python-bindings --lib -q` | 874 passed |
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
as physical-server verification on that server. The new Linux server validation
below supplies the v4.1/v4.2 physical coverage. Test-owned files and directories
were cleaned.

### Linux v4.1/v4.2 follow-up on 2026-11-25

- Server: `10.131.6.79`, Rocky Linux 9.4, kernel `5.14.0-427.13.1.el9_4.x86_64`,
  knfsd reports `+3 +4 +4.1 +4.2`.
  Created a dedicated `/srv/nfsrs-lisa4413` export, writable only by
  `10.131.7.194`, using AUTH_SYS, sync and reserved source ports. Existing
  exports were not replaced and the NFS service was not restarted.
- Client: `10.131.7.194`, Rocky Linux 9.7. A temporary Linux mount with exact
  `vers=4.2` succeeded before testing the library. A separately rebuilt
  production wheel without `python-test-support` connected directly to the
  server with exact `version=4.1` and `version=4.2`.
- Initial v4.1 namespace checks passed. Initial v4.2 failed at EXCHANGE_ID with
  `NFS4ERR_INVAL`, before filesystem operations. Wire evidence showed request
  flags `0x20004`; removing only optional FENCE_OPS (`0x4`) in a diagnostic
  relay allowed v4.2 setup. That relay result is diagnosis, not final validation.
- RFC 7862 §14.1 recommends requesting FENCE_OPS, but this client does not
  depend on fencing support. The session layer now retries once without that
  bit only after a definite, single-operation EXCHANGE_ID INVAL rejection.
  Owner, verifier, requested minor version and MDS/DS role remain unchanged.
  Repeated INVAL, access errors, minor-version mismatch and transport errors
  are not suppressed or converted into an unbounded fallback.
- The regression failed before the session change and passed after it. Wire
  tests assert unchanged identity/version, bounded repeated rejection, and
  preservation of v4.1 INVAL and v4.2 ACCESS errors. The existing exact-version
  rejection and recovery tests also pass.
- Final direct-server production-wheel tests passed for **both v4.1 and v4.2**:
  create/write/list/read, rename, hard link, symlink/readlink, remove,
  mkdir/rmdir and percent-encoded non-UTF-8 subtree mount. Names include
  UTF-8 Chinese, GBK bytes, invalid UTF-8, newline/backslash and a literal
  former-prefix sibling; removing/renaming the raw entry preserves that sibling.
  Linux `os.listdir(bytes_path)` supplies the independent raw-byte oracle.
  Both versions also enumerated 1,500 long non-UTF-8 names with no missing or
  duplicate entries; the returned byte set matched the Linux mount exactly.
- Production wheel SHA-256:
  `3237bd015420db5369f80b27849ecdc303e5a129df2b6e0d022866addf5726d7`.
- All UUID-owned test files were removed. The dedicated server export remains
  available for follow-up verification; credentials and raw wire captures are
  outside the repository. Both exact Linux v4.1 and v4.2 mounts succeeded;
  temporary client mounts were unmounted and their empty mount directory removed.
  No physical restart/state-loss injection was run.

Final logs on 194: `/tmp/nfs679-rust.log`, `/tmp/nfs679-doc.log`,
`/tmp/nfs679-clippy.log`, `/tmp/nfs679-regression-before.log`,
`/tmp/nfs679-regression-after.log`, `/tmp/nfs679-live-final.log`,
`/tmp/nfs679-pages-final.log`, `/tmp/nfs679-bindings.log`,
`/tmp/nfs679-python.log`, `/tmp/nfs679-stubtest.log` and `/tmp/nfs679-smoke.log`.

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

The optional FENCE_OPS negotiation belongs in the existing shared v4.1/v4.2
session bootstrap, so MDS and DS do not acquire separate compatibility rules.
It changes no filename encoding, transport replay policy or mutation retry
semantics, and the client continues to treat uncertain operations conservatively.
