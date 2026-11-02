# NFSv4.2 optional operations and AFX revalidation — 2026-11-02

This report preserves the original disabled-switch run. The subsequent
[enabled-feature validation](nfsv42-afx-enabled-2026-11-02.md) records working
ordinary ACLs, ALLOCATE, SEEK and whole-file DEALLOCATE, plus the partial
DEALLOCATE failure that remains an explicit failed gate.

## Result and correction of the earlier xattr conclusion

All four AFX LIFs (`192.168.9.101`–`.104`) on `svm2` / `/pnfs_test2` were
retested with the implemented Rust and Python clients. **RFC 8276 extended
attributes work on files and directories.** The earlier
[baseline report](nfsv42-afx-2026-11-02.md) exercised OPENATTR named attributes,
a separate namespace; its negative result does not establish RFC 8276 support.
The client now uses the correct extension on v4.2 and keeps OPENATTR on
v4.0/v4.1, with no implicit namespace fallback.

Read-only management queries returned ONTAP **9.19.1P2**, v4.2 enabled,
`v4.2-xattrs=enabled`, `v4.1-acl=disabled` and
`v4.2-sparsefile-ops=disabled` (the sparse field required advanced CLI
visibility). No SVM setting, export policy, LIF, volume or service was changed.

| Capability | Outcome on all four LIFs | Evidence/boundary |
|---|---|---|
| GETXATTR/SETXATTR/LISTXATTRS/REMOVEXATTR | positive validation passed | RFC 8276, advertised attribute 82=true |
| Atomic CREATE/REPLACE | positive validation passed | CREATE-existing returned EXIST 17; REPLACE-missing returned NOXATTR 10095 |
| Ordinary ACL / ACLSUPPORT | unavailable in current configuration | supported_attrs omitted attributes 12/13; ACL switch disabled |
| DACL / SACL | unavailable in current configuration | supported_attrs omitted attributes 58/59; sync/async calls reported Unsupported |
| COPY | unsupported response | NFS4ERR_NOTSUPP 10004 for synchronous intra-server request |
| CLONE | unsupported response | NFS4ERR_NOTSUPP 10004; clone_blksize 77 not advertised |
| ALLOCATE / DEALLOCATE | unsupported response | NFS4ERR_NOTSUPP 10004; sparse operations configured disabled |
| SEEK DATA / HOLE | unsupported response | NFS4ERR_NOTSUPP 10004; sparse operations configured disabled |
| READ_PLUS | unsupported response | NFS4ERR_NOTSUPP 10004; sparse operations configured disabled |

These negative outcomes describe the configured export and software version;
they are not a claim that every AFX configuration/version lacks the features.
Rust did not replace an ordinary ACL without first obtaining its original
value, so ordinary SETACL is recorded **not exercised**, not passed.

## Implementation

The common `Mount` trait exposes allocate/deallocate, seek_content, read_plus,
copy_range, clone_range, nfs42_attributes and setxattr_with_mode. Unsupported
protocol versions retain explicit default Unsupported behavior. Shared-session
XDR and operation classification live in `src/nfs41/compound.rs`; dedicated
`v42_ops.rs` and `v42_xattr.rs` own the optional semantics. Public result/option
types live in `src/nfs42.rs`. Python sync/async clients provide path methods
with typed results, plus XattrSetMode.EITHER/CREATE/REPLACE.

Modifying calls retain barriers after caller cancellation and settle prior
pNFS writes/layouts before changing data. COPY is synchronous and intra-server;
its committed level/verifier is checked and COMMIT completes before returning
durable bytes. Short results remain explicit. An unexpected asynchronous
success triggers an OFFLOAD_CANCEL attempt and an uncertain result. Session
loss does not authorize replay with a fresh session/sequence; the caller gets
Uncertain/VerifyThenResume. COPY/CLONE retain both temporary Python OPENs until
the request and CLOSEs settle. Cross-server/async offload and software-copy
fallback are outside this API.

READ_PLUS bounds and validates arrays/ranges, clips expanded holes without
allocating their contents and adjusts EOF when clipping. Xattr listing checks
opaque cookies, progress and the complete reply-byte budget. New status codes
are owned by the source XDR definitions, not edited generated output. Missing
keys preserve NOXATTR, mapped to Python FileNotFoundError; XATTR2BIG maps E2BIG.
ACL checks use target-filesystem supported_attrs, preserving malformed-response
errors when advertised attributes are unexpectedly omitted. Mount-root `.`
paths are resolved locally rather than sent as invalid LOOKUP names.
Protocol sources and constraints are in
[the operation notes](../nfsv42-operations-protocol-notes.md).

## Accepted physical run

Run ID: **nightly-v42-ops-20261102-3**. Execution used the existing AFX data
executor and shared `/tmp/nfsrs-svm2-issue40.lock`, AUTH_SYS UID/GID0 and
non-reserved ports. `prepare-run.sh` created a unique kernel-client run leaf;
the runner unconditionally called cleanup for its owned directory.

- Rust ordinary file E2E passed on all four endpoints.
- The optional-operation matrix recorded each result independently. RFC 8276
  success covered binary values, shorter/empty replacement, existence modes,
  49-key multi-page listing, deletion and missing-key status. Directory xattrs
  were independently written/read/listed/removed, with file-namespace isolation.
- Python public-API validation passed on every endpoint: 68 sync and 67 async
  check labels per endpoint, including explicit unsupported outcomes. RFC 8276
  xattr checks succeeded; DACL/SACL and the six new file-operation APIs were
  explicitly recorded as unavailable on this server. They are not counted as
  successful operation execution.
- Repeated Python validation produced five valid 4 MiB integrity runs per LIF.
  `.101` failed the initial RSS plateau test, then passed its first automatic
  retest using the existing policy. The initial failure is retained in private
  JSON evidence. `.102`–`.104` passed on the first attempt. No thresholds or
  performance baselines were changed; these are not release performance gates.

The ordinary wheel was built **without** python-test-support and installed on
CPython 3.11.15 / glibc 2.34. Test-support wheel preflight rejection remains enabled.
Its manylinux_2_34 tag is a diagnostic host build, not a manylinux2014 release
artifact, and it must not be published. The static musl Rust test binary and
wheel SHA256 values matched on the executor:

- Rust lab_e2e: `30ea21345ec85f582fb12c3b648c600365941a8d6cdc3c4d401190a204973b03`
- Ordinary wheel: `8e980ad6b9197c106d2fad48d9a967eb0180633c67a81055b4b767a91704b31a`

Independent kernel NFSv4.2 remount verified absence of the accepted run and both
failed investigation-run directories. Remote temporary test tools/interpreter,
wheel and evidence were removed after preserving raw evidence privately outside
the checkout. Credentials and raw machine-local transcripts are not committed.

## Deterministic validation and limits

- cargo fmt --all -- --check: passed.
- cargo clippy --all-targets --features python-bindings -- -D warnings: passed.
- cargo test --all-targets --no-fail-fast: **701 passed**, including 658 library
  tests; 30 physical-lab tests remained ignored in that deterministic command.
- cargo test --doc: **47 passed**.
- Current-checkout installed test-support wheel with NFS_RS_TEST_INSTALLED=1:
  **388 passed**, one separate real-server fixture module skipped.
- Installed-wheel mypy.stubtest: passed; no allowlist weakening.
- AFX runner ownership/cleanup tests: four passed; shell syntax checks passed.

The 15 new Rust regressions exercise positive wire encodings and replies,
COPY durability/verifier changes, reply-cache capacity/cachethis, cancellation
barriers, unexpected async cancellation, refusal to retry mutations after
session loss, hole clipping/EOF, malformed replies, xattr pagination and
atomic modes, pre-send size rejection without slot loss, ACL unsupported
classification and default root-path resolution. Python regressions cover
all new sync/async public methods, result types, set modes, validation and
error context.

Positive COPY/CLONE/sparse-operation interoperability on physical AFX remains
unverified because that server rejected the requests. Their positive paths
were verified using deterministic RPC fixtures. This run did not inject
server/network faults or verify distinct-DS routing, physical callback recall,
maximum filesystem xattr sizes/counts, other endpoints, release performance
acceptance or registry publication. Source is the uncommitted working tree
based on b1009a6415e9d96ae2f1dbbbf0f6fe05ef0e5b11, not the unchanged public
0.9.1 registry artifacts.
