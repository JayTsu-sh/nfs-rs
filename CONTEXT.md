# NFS Client Domain

This context defines the protocol-support and validation language used by the
nfs-rs project.

## Language

**NFSv4.0 client support**:
Standards-based RFC 7530 client behavior selected only by the exact protocol
version `4.0`.
_Avoid_: NFSv4 support, version 4

**Mount capability parity**:
Support for every public `Mount` capability that the selected NFS protocol can
express, with protocol-specific capabilities reported explicitly as unsupported.
_Avoid_: All RFC operations, identical protocol behavior

**Reference validation platform**:
A real server environment that provides mandatory interoperability evidence
without defining vendor-specific client behavior. FAS2750 is the NFSv4.0
reference validation platform.
_Avoid_: Supported server, ONTAP-only client

**Uncertain outcome**:
A modifying operation whose request may have reached the server but whose
result cannot be established safely after communication failure.
_Avoid_: Failed operation, retryable error
