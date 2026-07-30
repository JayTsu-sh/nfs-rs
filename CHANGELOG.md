# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/JayTsu-sh/nfs-rs/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/JayTsu-sh/nfs-rs/releases/tag/v0.3.0
