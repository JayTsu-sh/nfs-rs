# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.1] - 2026-09-03

### Fixed

- Prevent NFSv4.0 create rollback from deleting a concurrently created file
  after an unchecked OPEN and failed SETATTR.
- Preserve failed NFSv4.1 CLOSE state for cleanup retry and surface the
  terminal cleanup error to Python callers.
- Enforce configured operation and connection deadlines consistently across
  synchronous and asynchronous Python clients.
- Preserve write-only share access during NFSv4 file creation and reject
  unknown NFS URL query options before connecting.

### Testing

- Gate release validation on Python 3.14 in addition to the supported lower
  and intermediate Python versions.

## [0.6.0] - 2026-08-30

### Added

- Add NFSv4.1 `dacl` and `sacl` GET and full-replacement SET protocol
  primitives for file handles and paths.
- Expose immutable DACL/SACL values and matching synchronous and asyncio
  operations through the typed Python API.

### Testing

- Validate file and directory DACL/SACL capability behavior against Linux
  knfsd and FAS2750 NFSv4.1 exports.
- Add an ACL-faithful migration matrix spanning NFSv4.0/NFSv4.1 and
  Linux/FAS2750, including ordered ACE fidelity and descendant inheritance.

### Known limitations

- The current Linux knfsd and FAS2750 validation exports omit NFSv4.1 DACL and
  SACL attributes and reject SET with `NFS4ERR_ATTRNOTSUPP`; callers receive a
  structured unsupported error. Ordinary NFSv4 ACL primitives remain usable.

## [0.5.8] - 2026-08-29

### Fixed

- Give the PyPI distribution a Python-specific README with verified sync,
  asyncio, metadata, xattr, protocol-selection, and recovery examples.
- Expand the Python user guide and state the tested NFSv3, experimental
  NFSv4.0, NFSv4.1, and x86_64 support boundaries precisely.
- Set the Python minimum to CPython 3.11 and publish a matching `abi3-py311`
  wheel instead of advertising Python 3.10 support.

## [0.5.7] - 2026-08-29

### Fixed

- Apply the release policy of three independent supplemental attempts to each
  Python artifact RSS plateau failure, accepting any successful retest while
  preserving failed-attempt evidence.

## [0.5.6] - 2026-08-29

### Security

- Upgrade `bytes`, `crossbeam-epoch`, and PyO3 dependencies to versions that
  address the current RustSec advisories enforced by the release gate.

## [0.5.5] - 2026-08-29

### Fixed

- Flatten immutable release artifacts before upload so audit and registry
  publication consume the exact validated wheel, sdist, and crate files.

## [0.5.4] - 2026-08-29

### Fixed

- Run release metadata validation with Python 3.11 for standard-library TOML
  support, then restore Python 3.10 for minimum-version artifact validation.

## [0.5.3] - 2026-08-29

### Fixed

- Bootstrap Python before validating release tag/version coupling on the
  self-hosted release runner.

## [0.5.2] - 2026-08-29

### Added

- Add typed synchronous and asyncio Python clients for NFSv3, experimental
  NFSv4.0, and NFSv4.1, including files, directories, namespace mutations,
  metadata, ACLs, xattrs, recovery events, and structured operation outcomes.
- Add an `abi3-py310` Linux x86_64 wheel and tested source distribution with
  complete stubs and a `py.typed` marker.
- Negotiate NFSv4 ACL capability from server-supported attributes.

### Changed

- Refresh the DXN NFSv4.0 performance baseline from nine independent capture
  windows and keep the repository storage gate as the sole release-blocking
  performance decision.
- Restrict the first Python artifact release to Linux x86_64.

### Testing

- Validate the final wheel and the wheel rebuilt from the source distribution
  across real NFSv3, NFSv4.0, NFSv4.1, and NetApp pNFS environments.
- Add deterministic Python contract, typing, concurrency, cancellation,
  lifecycle, memory-bound, fault, and packaging coverage.
- Run performance baseline capture every 20 minutes during bootstrap, reducing
  the expected nine-window collection period from about 18 hours to 160 minutes.

### Fixed

- Accept NFSv4 PATHCONF responses that omit RECOMMENDED attributes, expose
  per-field availability and filesystem scope, and retain one-RPC discovery.

## [0.5.1] - 2026-08-19

### Testing

- Add fail-closed nightly NFSv4.0 validation against the DXN
  `10.131.7.201:/jay_nfs` fixture, including negotiated I/O limits,
  self-contained namespace and data integrity checks, and concurrent I/O.
- Add independent cross-environment performance baselines for every real
  Linux, DXN, FAS2750, and NetApp pNFS endpoint/protocol combination, with
  scheduled multi-window capture, candidate release-gate tooling, and
  JSON/Markdown reports.

## [0.5.0] - 2026-08-14

### Added

- Add experimental AUTH_SYS NFSv4.0 support through the common `Mount` API,
  including namespace, metadata, ACL, stateful I/O, locks, lease recovery and
  opt-in automatic delegation callbacks.
- Add deterministic RFC/scripted coverage and physical FAS2750 validation
  through both reference data LIFs.

### Changed

- Accept exact `version=4.0` and ordered fallback lists while continuing to
  reject ambiguous `version=4`; NFSv3 remains the default.

### Known limitations

- RPCSEC_GSS is not included. NFSv4.0 remains experimental, and real server
  restart grace/reclaim evidence requires a dedicated fixture or maintenance
  window.

## [0.4.0] - 2026-08-13

### Added

- Add NFSv4.1 pNFS file-layout I/O with multi-file distribution, striped
  writes, incremental layout acquisition, and proactive layout refresh during
  large writes.
- Add explicit uncertain-outcome reporting for operations whose completion
  cannot be determined safely after a transport failure.

### Fixed

- Harden NFSv4.1 session, slot, lease, callback, reconnect, layout recall,
  dirty-range, and partial pNFS data-server failure handling.
- Singleflight concurrent cold connections to the same pNFS data server and
  fence cached reachability by session generation.
- Retry concurrent reserved-source-port tuple collisions for NFSv3 and
  NFSv4.1 connections while preserving the final concrete endpoint error.

### Testing

- Add scripted reliability coverage and physical NetApp NFSv4.1/pNFS nightly
  validation.

## [0.3.1] - 2026-07-30

### Fixed

- Limit the effective NFSv4.1 write size by the negotiated session request
  size, reserving protocol framing space to prevent `NFS4ERR_REQ_TOO_BIG`.

### Testing

- Exercise the full negotiated NFSv4.1 write size against the physical
  integration lab.

## [0.3.0] - 2026-07-29

### Added

- Asynchronous NFSv3 and NFSv4.1 client support over TCP.
- File, directory, metadata, link, ACL, and filesystem operations through the
  shared `Mount` interface.
- Configurable privileged or ephemeral source-port behavior.
- Physical-lab end-to-end coverage for NFSv3 and NFSv4.1.

[Unreleased]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.6.1...HEAD
[0.6.1]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.5.8...v0.6.0
[0.5.8]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.5.7...v0.5.8
[0.5.7]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.5.6...v0.5.7
[0.5.6]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.5.5...v0.5.6
[0.5.5]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.5.4...v0.5.5
[0.5.4]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/JayTsu-sh/nfs-rs/releases/tag/v0.3.0
