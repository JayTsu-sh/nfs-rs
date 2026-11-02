# AFX NFSv4.2 validation after enabling features — 2026-11-02

The subsequent [pre-merge review](nfsv42-review-2026-11-02.md) records the
cancellation fixes and the final corrected build's four-LIF revalidation.

At the user's request, `svm2` was changed to enable `v4.1-acl` and
`v4.2-sparsefile-ops`. Both settings remain enabled after validation.
`v4.2` and `v4.2-xattrs` were already enabled and remain enabled.
`v4.0-acl` and `vstorage` remain disabled. No export policy, volume, LIF or
service restart was changed. The NFSv4.1 ACL switch also controls later
minor versions, as described by the
[ONTAP CLI reference](https://docs.netapp.com/us-en/ontap-cli/vserver-nfs-modify.html).
vStorage is a separate facility; no COPY/CLONE-specific switch was identified
in that reference.

## Physical results

All four `192.168.9.101`–`.104` LIFs were exercised on the existing
`/pnfs_test2` export, ONTAP 9.19.1P2. Each result below was consistent across
the four endpoints, using fresh NFSv4.2 sessions.

| Operation | Enabled configuration result | Validation |
|---|---|---|
| Ordinary ACL / ACLSUPPORT | passed | Original ACL read, replaced and reread with equality; ACLSUPPORT=3 |
| DACL / SACL | unsupported | Attributes 58/59 absent from supported_attrs; Python get/set calls explicitly unavailable |
| RFC 8276 xattrs | passed | Files/directories; binary values, atomic CREATE/REPLACE, empty/short replacement, 49-key pagination, removal and NOXATTR |
| ALLOCATE | passed | Allocation beyond EOF extends the file to cover the requested range |
| DEALLOCATE, whole file | passed | EOF unchanged and the entire range reads as zeros |
| DEALLOCATE, partial range | **failed** | NFS4ERR_IO (5) for offset 4096, length 4096 |
| SEEK DATA / HOLE | passed | DATA locates existing data; HOLE returns within EOF |
| COPY / CLONE / READ_PLUS | unsupported | NFS4ERR_NOTSUPP (10004); clone_blksize remains unadvertised |

The supported_attrs bitmap changed to `[4210999295, 1627258878, 264215]`:
ordinary ACL/ACLSUPPORT are now advertised, DACL/SACL remain absent, and
xattr_support remains true.

The partial DEALLOCATE failure was minimized on `.101`: both nfs-rs and a
Linux kernel NFSv4.2 mount returned EIO, with and without a preceding ALLOCATE.
Both clients succeeded for a whole-file range. This matches NetApp's documented
[whole-file-only limitation](https://kb.netapp.com/on-prem/ontap/da/NAS/NAS-KBs/Does_this_v4.2-sparsefile-ops_perform_ALLOCATE_DEALLOCATE%3F).
The Rust matrix retains partial-range failure independently of its new
whole-file content/EOF check. No error is converted to Unsupported or success,
and no client-side zero-write fallback was added.

## Runs and gates

The primary enabled run was `nightly-v42-enabled-20261102-2`. The initial run
`nightly-v42-enabled-20261102-1` exposed the partial DEALLOCATE error.
Two isolated `nightly-v42-enabled-probe-20261102-*` runs provided kernel-client
comparisons. Configuration changes and runs used the existing shared AFX lab
lock; every owned run used prepare-run.sh and unconditional cleanup-run.sh.

- Rust ordinary file E2E passed across all four LIFs.
- Rust optional-operation gate **failed**, solely for partial DEALLOCATE on
  each endpoint. All other recorded outcomes appear in the table above.
- Python public API validation passed on all four endpoints, with 68 sync and
  67 async labels per endpoint. Its DEALLOCATE request covers the whole file;
  the Rust matrix independently retains the failing partial-range probe.
  Each endpoint records 14 unavailable method labels: COPY, CLONE, READ_PLUS,
  get/set DACL and get/set SACL for both clients. These are not positive passes.
- Python repeated integrity/GIL/event-loop/memory validation produced five
  valid 4 MiB runs per LIF, with no invalid samples or RSS retests. Existing
  thresholds were unchanged; this is not release performance acceptance.

Python phases ran independently after the failed Rust gate to complete the
capability evidence. The diagnostic driver still exited nonzero for that gate.
The repository's fail-fast AFX runner was not weakened.

The Rust binary was rebuilt after adding the whole-file regression probe.
The ordinary Python wheel was unchanged from the preceding report, built
without python-test-support, and installed into a dedicated executor venv.
Executor SHA256 values:

- Rust: `724d2a54e69267a731b9556ddd63d65d5dd46ac681ac15aa89dbba2ca88d6865`
- Wheel: `8e980ad6b9197c106d2fad48d9a967eb0180633c67a81055b4b767a91704b31a`

This wheel is a glibc 2.34 diagnostic build, not a manylinux2014 release
artifact. No artifact was published. This is the uncommitted working tree,
not the public 0.9.1 registry artifact.

Targeted deterministic checks after this follow-up: Rust formatting passed;
lab_e2e deterministic tests passed (3 passed, 30 physical cases ignored);
installed-wheel Python tests passed (388 passed, one separate real-server
fixture module skipped); AFX runner ownership/cleanup tests passed (4 tests).
Full Rust/clippy/doc/stubtest gates passed in the preceding implementation
report and were not rerun for these lab-only probe changes.

An independent kernel remount verified that both primary run directories and
both probe directories were absent. Raw JSON, configuration transcripts and
diagnostic logs remain private outside the checkout. Temporary executor tools
were removed after preserving evidence. Credential files are not committed.
Other exports, firmware versions, failure injection, distinct-DS routing,
release gates and maximum filesystem xattr limits were not validated here.
