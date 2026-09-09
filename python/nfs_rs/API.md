# nfs-rs Python API reference

This reference ships as `nfs_rs/API.md` in every wheel and source distribution.
The companion [user guide](GUIDE.md) contains workflows and examples. Public
symbols are imported from `nfs_rs`; underscore-prefixed implementation modules
are private. `__version__` is the installed distribution version string.

## Shared argument, I/O and error rules

Paths accept `str` or `os.PathLike[str]` and are relative to the mounted export.
Absolute paths, escaping `..`, NULs and byte-string paths are rejected. Symlink
target strings are the explicit exception: their stored text is preserved.
Offsets and sizes are byte counts. File reads require a readable binary mode;
writes and truncation require a writable mode. Read sizes accept -1 for EOF or
non-negative values; other negative sizes and invalid ranges are rejected.
Bytes-like write inputs are snapshotted; readinto targets must be writable and
C-contiguous. Async methods must be awaited on their creating event loop.

All I/O methods may raise NfsError subclasses for transport, protocol, permission,
closed-resource or state failures. Validation may raise TypeError, ValueError or
OverflowError before NFS I/O. Unsupported server features raise typed unsupported
or protocol errors. Cancellation raises asyncio.CancelledError; a mutation may
still be settling. Consult outcome/recovery_action before retrying and do not use
completed_bytes as a resume offset. File readinto buffers cannot be modified by
abandoned background work after the public call returns. Context managers
preserve a body exception and attach cleanup failures rather than replacing it.

Supported deployment: CPython 3.11+ on Linux/glibc x86_64, NFSv3 and NFSv4.1,
experimental NFSv4.0, AUTH_SYS only. No NFSv4.2, Kerberos or RPCSEC_GSS.

## Connection options

`Client.connect`, `AsyncClient.connect`, `list_exports` and `list_exports_async`
share these keyword options. None means defer to URL/server/default policy.
Unknown options are rejected. There are no rsize, wsize, readahead or writeback
options. Transfer sizes are negotiated; concurrency is capped at eight.

| Parameter | Default | Meaning |
|---|---|---|
| versions | None | Nonempty ordered list/tuple containing "3", "4.0", "4.1". Overrides URL version. Without either, use NFSv3. |
| uid, gid | None | AUTH_SYS unsigned 32-bit numeric identity; default comes from the process/platform. |
| nfs_port | None | NFS service port, 1–65535; URL spelling nfsport. |
| mount_port | None | NFSv3 MOUNT service port, 1–65535; URL spelling mountport. |
| readdir_buffer | None | Positive response-size limit, or positive (dircount, maxcount) pair, in bytes. |
| noresvport | None | Boolean; URL default false uses a privileged source port. True requires an export accepting unprivileged ports. |
| retain_delegations | None | Boolean delegation-retention policy; effective support is reported by capabilities. |
| connect_timeout | None | Positive seconds for connection setup; None adds no Python deadline. |
| operation_timeout | None | Positive seconds for an operation; None adds no Python deadline. |
| recovery_event_capacity | 256 | Positive maximum retained diagnostic-event count. |

## Module functions and version

### __version__

`__version__: str` reports installed package metadata; it is "0+unknown" when
running an unpackaged source tree without distribution metadata.

### list_exports

```python
def list_exports(
    host: str,
    *,
    versions: tuple[str, ...] | list[str] | None = None,
    uid: int | None = None,
    gid: int | None = None,
    nfs_port: int | None = None,
    mount_port: int | None = None,
    readdir_buffer: int | tuple[int, int] | None = None,
    noresvport: bool | None = None,
    retain_delegations: bool | None = None,
    connect_timeout: float | None = None,
    operation_timeout: float | None = None,
    recovery_event_capacity: int = 256,
) -> tuple[ExportEntry, ...]: ...
```

Query the NFSv3 MOUNT export service (like showmount -e) and return a tuple of ExportEntry values. host accepts a hostname/address or NFS URL; the connection options above also apply. This is not NFSv4 namespace discovery and requires the server to expose MOUNT. This function blocks until discovery completes.

### list_exports_async

```python
async def list_exports_async(
    host: str,
    *,
    versions: tuple[str, ...] | list[str] | None = None,
    uid: int | None = None,
    gid: int | None = None,
    nfs_port: int | None = None,
    mount_port: int | None = None,
    readdir_buffer: int | tuple[int, int] | None = None,
    noresvport: bool | None = None,
    retain_delegations: bool | None = None,
    connect_timeout: float | None = None,
    operation_timeout: float | None = None,
    recovery_event_capacity: int = 256,
) -> tuple[ExportEntry, ...]: ...
```

Query the NFSv3 MOUNT export service (like showmount -e) and return a tuple of ExportEntry values. host accepts a hostname/address or NFS URL; the connection options above also apply. This is not NFSv4 namespace discovery and requires the server to expose MOUNT. Await this function.

## Types and their members

Frozen dataclasses below can be constructed from the listed fields; fields without
a default are required. Their returned values are read-only. Enum members expose
standard `.name` and `.value`; IntFlag values support bitwise combination.
Exception subclasses inherit NfsError construction, fields and with_context.

### Version

Connected protocol values. NFSv4.0 is experimental. Construct from a listed string value; unknown strings raise ValueError.

| Member | Type / value | Meaning |
|---|---|---|
| `Version.NFS_V3` | `'3'` | Named enum value; see this type description for interpretation. |
| `Version.NFS_V4_0` | `'4.0'` | Named enum value; see this type description for interpretation. |
| `Version.NFS_V4_1` | `'4.1'` | Named enum value; see this type description for interpretation. |

#### Version.__new__

```python
def __new__(cls, value: str) -> Version: ...
```

Construct the enum from a listed string value; invalid values raise ValueError. Prefer the named enum members.

### Lifecycle

Local client lifecycle values. Construct from a listed string value; unknown strings raise ValueError.

| Member | Type / value | Meaning |
|---|---|---|
| `Lifecycle.READY` | `'ready'` | Named enum value; see this type description for interpretation. |
| `Lifecycle.CLOSING` | `'closing'` | Named enum value; see this type description for interpretation. |
| `Lifecycle.CLOSED` | `'closed'` | Named enum value; see this type description for interpretation. |

#### Lifecycle.__new__

```python
def __new__(cls, value: str) -> Lifecycle: ...
```

Construct the enum from a listed string value; invalid values raise ValueError. Prefer the named enum members.

### FileType

File kind reported in FileInfo; UNKNOWN is used when the kind is not recognized. Construct from a listed value.

| Member | Type / value | Meaning |
|---|---|---|
| `FileType.FILE` | `'file'` | Named enum value; see this type description for interpretation. |
| `FileType.DIRECTORY` | `'directory'` | Named enum value; see this type description for interpretation. |
| `FileType.SYMLINK` | `'symlink'` | Named enum value; see this type description for interpretation. |
| `FileType.BLOCK_DEVICE` | `'block_device'` | Named enum value; see this type description for interpretation. |
| `FileType.CHARACTER_DEVICE` | `'character_device'` | Named enum value; see this type description for interpretation. |
| `FileType.FIFO` | `'fifo'` | Named enum value; see this type description for interpretation. |
| `FileType.SOCKET` | `'socket'` | Named enum value; see this type description for interpretation. |
| `FileType.UNKNOWN` | `'unknown'` | Named enum value; see this type description for interpretation. |

#### FileType.__new__

```python
def __new__(cls, value: str) -> FileType: ...
```

Construct the enum from a listed string value; invalid values raise ValueError. Prefer the named enum members.

### AceType

ACE action: allow, deny, audit or alarm. Integer values are listed below; not a bitmask.

| Member | Type / value | Meaning |
|---|---|---|
| `AceType.ALLOW` | `0` | Named enum value; see this type description for interpretation. |
| `AceType.DENY` | `1` | Named enum value; see this type description for interpretation. |
| `AceType.AUDIT` | `2` | Named enum value; see this type description for interpretation. |
| `AceType.ALARM` | `3` | Named enum value; see this type description for interpretation. |

### AceFlags

ACE inheritance/auditing flags. Combine IntFlag members with |; use AceFlags(0) for no flags. IDENTIFIER_GROUP marks who as a group principal.

| Member | Type / value | Meaning |
|---|---|---|
| `AceFlags.FILE_INHERIT` | `1` | Named enum value; see this type description for interpretation. |
| `AceFlags.DIRECTORY_INHERIT` | `2` | Named enum value; see this type description for interpretation. |
| `AceFlags.NO_PROPAGATE_INHERIT` | `4` | Named enum value; see this type description for interpretation. |
| `AceFlags.INHERIT_ONLY` | `8` | Named enum value; see this type description for interpretation. |
| `AceFlags.SUCCESSFUL_ACCESS` | `16` | Named enum value; see this type description for interpretation. |
| `AceFlags.FAILED_ACCESS` | `32` | Named enum value; see this type description for interpretation. |
| `AceFlags.IDENTIFIER_GROUP` | `64` | Named enum value; see this type description for interpretation. |
| `AceFlags.INHERITED` | `128` | Named enum value; see this type description for interpretation. |

### AceMask

ACE access-right bits. Combine members with |. For directories, READ_DATA/WRITE_DATA/APPEND_DATA correspond to directory-specific list/add rights under NFS ACL rules.

| Member | Type / value | Meaning |
|---|---|---|
| `AceMask.READ_DATA` | `1` | Named enum value; see this type description for interpretation. |
| `AceMask.WRITE_DATA` | `2` | Named enum value; see this type description for interpretation. |
| `AceMask.APPEND_DATA` | `4` | Named enum value; see this type description for interpretation. |
| `AceMask.READ_NAMED_ATTRS` | `8` | Named enum value; see this type description for interpretation. |
| `AceMask.WRITE_NAMED_ATTRS` | `16` | Named enum value; see this type description for interpretation. |
| `AceMask.EXECUTE` | `32` | Named enum value; see this type description for interpretation. |
| `AceMask.DELETE_CHILD` | `64` | Named enum value; see this type description for interpretation. |
| `AceMask.READ_ATTRIBUTES` | `128` | Named enum value; see this type description for interpretation. |
| `AceMask.WRITE_ATTRIBUTES` | `256` | Named enum value; see this type description for interpretation. |
| `AceMask.DELETE` | `65536` | Named enum value; see this type description for interpretation. |
| `AceMask.READ_ACL` | `131072` | Named enum value; see this type description for interpretation. |
| `AceMask.WRITE_ACL` | `262144` | Named enum value; see this type description for interpretation. |
| `AceMask.WRITE_OWNER` | `524288` | Named enum value; see this type description for interpretation. |
| `AceMask.SYNCHRONIZE` | `1048576` | Named enum value; see this type description for interpretation. |

### Acl41Flags

NFSv4.1 ACL inheritance flags; combine with | or use Acl41Flags(0).

| Member | Type / value | Meaning |
|---|---|---|
| `Acl41Flags.AUTO_INHERIT` | `1` | Named enum value; see this type description for interpretation. |
| `Acl41Flags.PROTECTED` | `2` | Named enum value; see this type description for interpretation. |
| `Acl41Flags.DEFAULTED` | `4` | Named enum value; see this type description for interpretation. |

### NfsAce

Frozen value object describing one access-control entry. Construct with all fields; server-side principal and permission validation still applies.

| Member | Type / value | Meaning |
|---|---|---|
| `NfsAce.type` | `AceType` | AceType action. |
| `NfsAce.flags` | `AceFlags` | AceFlags inheritance/auditing bits. |
| `NfsAce.access_mask` | `AceMask` | AceMask permission bits. |
| `NfsAce.who` | `str` | Principal string, such as OWNER@, GROUP@, EVERYONE@ or a server-recognized user/group. |

### NfsAcl41

Frozen NFSv4.1 ACL value. Construct with flags and an ordered tuple of NfsAce entries; used by DACL/SACL methods.

| Member | Type / value | Meaning |
|---|---|---|
| `NfsAcl41.flags` | `Acl41Flags` | Acl41Flags inheritance policy. |
| `NfsAcl41.aces` | `tuple[NfsAce, ...]` | Ordered tuple of NfsAce entries. Order affects ACL evaluation. |

### OperationOutcome

Failure classification: definite_failure has a known failure result; safe_to_retry allows the indicated safe recovery; uncertain means effects may have occurred. Consult RecoveryAction before retrying.

| Member | Type / value | Meaning |
|---|---|---|
| `OperationOutcome.DEFINITE_FAILURE` | `'definite_failure'` | Named enum value; see this type description for interpretation. |
| `OperationOutcome.SAFE_TO_RETRY` | `'safe_to_retry'` | Named enum value; see this type description for interpretation. |
| `OperationOutcome.UNCERTAIN` | `'uncertain'` | Named enum value; see this type description for interpretation. |

#### OperationOutcome.__new__

```python
def __new__(cls, value: str) -> OperationOutcome: ...
```

Construct the enum from a listed string value; invalid values raise ValueError. Prefer the named enum members.

### OperationClass

Request classification: read_only does not mutate file data; session_control manages session state; replay_sensitive operations require care after an ambiguous response.

| Member | Type / value | Meaning |
|---|---|---|
| `OperationClass.READ_ONLY` | `'read_only'` | Named enum value; see this type description for interpretation. |
| `OperationClass.SESSION_CONTROL` | `'session_control'` | Named enum value; see this type description for interpretation. |
| `OperationClass.REPLAY_SENSITIVE` | `'replay_sensitive'` | Named enum value; see this type description for interpretation. |

#### OperationClass.__new__

```python
def __new__(cls, value: str) -> OperationClass: ...
```

Construct the enum from a listed string value; invalid values raise ValueError. Prefer the named enum members.

### RecoveryAction

Suggested next action: retry, reopen, remount, verify_then_resume or do_not_retry. Verification is application-specific; do not blindly replay uncertain mutations.

| Member | Type / value | Meaning |
|---|---|---|
| `RecoveryAction.RETRY` | `'retry'` | Named enum value; see this type description for interpretation. |
| `RecoveryAction.REOPEN` | `'reopen'` | Named enum value; see this type description for interpretation. |
| `RecoveryAction.REMOUNT` | `'remount'` | Named enum value; see this type description for interpretation. |
| `RecoveryAction.VERIFY_THEN_RESUME` | `'verify_then_resume'` | Named enum value; see this type description for interpretation. |
| `RecoveryAction.DO_NOT_RETRY` | `'do_not_retry'` | Named enum value; see this type description for interpretation. |

#### RecoveryAction.__new__

```python
def __new__(cls, value: str) -> RecoveryAction: ...
```

Construct the enum from a listed string value; invalid values raise ValueError. Prefer the named enum members.

### Health

Frozen health snapshot. Values describe local state rather than a fresh server probe.

| Member | Type / value | Meaning |
|---|---|---|
| `Health.lifecycle` | `Lifecycle` | Lifecycle enum for local state. |
| `Health.generation` | `int` | Recovery-generation counter. |
| `Health.lease_healthy` | `bool &#124; None` | Lease-health flag, or None when not applicable/available. |

### RecoveryEvent

Frozen diagnostic event retained in a bounded client queue. Read it with recovery_events or consume it with drain_recovery_events.

| Member | Type / value | Meaning |
|---|---|---|
| `RecoveryEvent.operation` | `str` | Operation name, or None when no operation context is available. |
| `RecoveryEvent.path` | `str &#124; None` | Export-relative path or diagnostic path; see the containing type for normalization rules. |
| `RecoveryEvent.protocol` | `str` | Protocol identifier, or None on an error without protocol context. |
| `RecoveryEvent.outcome` | `OperationOutcome` | OperationOutcome failure classification; optional on generic NfsError. |
| `RecoveryEvent.recovery_action` | `RecoveryAction` | RecoveryAction to consider next; optional on generic NfsError. |
| `RecoveryEvent.completed_bytes` | `int &#124; None` | Acknowledged write bytes, or None when unknown/not applicable. May cover non-contiguous ranges; not a resume offset or proof of durability. |
| `RecoveryEvent.message` | `str` | Human-readable diagnostic message. |

### FileInfo

Frozen file metadata. Counts are integers; atime, mtime and ctime retain their names but are nanoseconds, not Python stat floating-point seconds.

| Member | Type / value | Meaning |
|---|---|---|
| `FileInfo.path` | `str` | Export-relative path or diagnostic path; see the containing type for normalization rules. |
| `FileInfo.type` | `FileType` | FileType kind. |
| `FileInfo.mode` | `int` | Unix permission/mode bits. |
| `FileInfo.nlink` | `int` | Hard-link count. |
| `FileInfo.uid` | `int` | Numeric owner ID. |
| `FileInfo.gid` | `int` | Numeric group ID. |
| `FileInfo.size` | `int` | Logical length in bytes. |
| `FileInfo.used` | `int` | Storage used in bytes as reported by the server. |
| `FileInfo.fsid` | `int` | Filesystem identifier. |
| `FileInfo.fileid` | `int` | Server file identifier. |
| `FileInfo.atime` | `int` | Last access time, integer nanoseconds since Unix epoch (UTC). |
| `FileInfo.mtime` | `int` | Last data modification time, integer nanoseconds since Unix epoch (UTC). |
| `FileInfo.ctime` | `int` | Last metadata/status change time, integer nanoseconds since Unix epoch (UTC); not creation time. |
| `FileInfo.owner` | `str &#124; None` | Server owner name, or None when unavailable. |
| `FileInfo.group` | `str &#124; None` | Server group name, or None when unavailable. |

### DirectoryRef

Frozen scan input. path is used for returned entry paths and diagnostics. When fh is present, it identifies the directory directly; otherwise path lookup obtains the handle.

| Member | Type / value | Meaning |
|---|---|---|
| `DirectoryRef.path` | `os.PathLike[str] &#124; str` | Export-relative path or diagnostic path; see the containing type for normalization rules. |
| `DirectoryRef.fh` | `bytes &#124; None = None` | Opaque bytes file handle, or None when unavailable. Do not synthesize it or reuse it across unrelated clients. |

### DirEntry

Frozen directory entry with eager metadata and an optional opaque file handle. It can be passed directly to scandir when it describes a directory.

| Member | Type / value | Meaning |
|---|---|---|
| `DirEntry.name` | `str` | Entry basename. |
| `DirEntry.path` | `str` | Export-relative path or diagnostic path; see the containing type for normalization rules. |
| `DirEntry.info` | `FileInfo` | Eager FileInfo metadata for the entry. |
| `DirEntry.fh` | `bytes &#124; None = None` | Opaque bytes file handle, or None when unavailable. Do not synthesize it or reuse it across unrelated clients. |

### ExportEntry

Frozen MOUNT export-list entry. This is export discovery information, not proof that the caller can mount or access the export.

| Member | Type / value | Meaning |
|---|---|---|
| `ExportEntry.path` | `str` | Server-advertised export path (normally absolute). |
| `ExportEntry.groups` | `tuple[str, ...]` | Tuple of server-advertised export access groups/hosts. |

### FsInfo

Frozen filesystem transfer preferences and capability information. Preferences are hints; IoLimits reports the effective negotiated chunk limits used by the client.

| Member | Type / value | Meaning |
|---|---|---|
| `FsInfo.max_read` | `int` | Maximum read transfer size in bytes. |
| `FsInfo.preferred_read` | `int` | Preferred read transfer size in bytes. |
| `FsInfo.read_multiple` | `int` | Recommended read-size multiple in bytes. |
| `FsInfo.max_write` | `int` | Maximum write transfer size in bytes. |
| `FsInfo.preferred_write` | `int` | Preferred write transfer size in bytes. |
| `FsInfo.write_multiple` | `int` | Recommended write-size multiple in bytes. |
| `FsInfo.preferred_directory` | `int` | Preferred directory response size in bytes. |
| `FsInfo.max_file_size` | `int` | Maximum supported file length in bytes. |
| `FsInfo.time_delta_ns` | `int` | Server timestamp resolution in nanoseconds. |
| `FsInfo.supports_links` | `bool` | Whether hard links are supported. |
| `FsInfo.supports_symlinks` | `bool` | Whether symbolic links are supported. |
| `FsInfo.homogeneous` | `bool` | Whether filesystem properties are reported homogeneous. |
| `FsInfo.can_set_time` | `bool` | Whether client-selected timestamps can be set. |

### FsStat

Frozen filesystem capacity snapshot. Byte counters and file counters have different units, as listed below.

| Member | Type / value | Meaning |
|---|---|---|
| `FsStat.total_bytes` | `int` | Total capacity in bytes. |
| `FsStat.free_bytes` | `int` | Free capacity in bytes. |
| `FsStat.available_bytes` | `int` | Capacity available to the caller in bytes. |
| `FsStat.total_files` | `int` | Total file slots/inodes. |
| `FsStat.free_files` | `int` | Free file slots/inodes. |
| `FsStat.available_files` | `int` | File slots/inodes available to the caller. |
| `FsStat.invariant_seconds` | `int` | Server-reported interval in seconds for which these statistics may be invariant. |

### Capabilities

Frozen negotiated feature flags. Server support and a Python operation being exposed are separate questions; for example, there is no public Python lock API.

| Member | Type / value | Meaning |
|---|---|---|
| `Capabilities.acl` | `bool` | Negotiated ACL support; DACL/SACL still require their specific server/protocol support. |
| `Capabilities.named_attributes` | `bool` | Negotiated named/extended-attribute support. |
| `Capabilities.locks` | `bool` | Protocol locking capability; no public Python locking methods. |
| `Capabilities.callbacks` | `bool` | Protocol callback support. |
| `Capabilities.delegation_retention` | `bool` | Whether delegation retention is available under the connection policy. |
| `Capabilities.pnfs` | `bool` | Negotiated pNFS support. |
| `Capabilities.session_diagnostics` | `bool` | Protocol session-diagnostics capability; no separate Python diagnostics call. |

### IoLimits

Frozen effective transfer limits negotiated at mount time; both fields are byte counts, not concurrency settings.

| Member | Type / value | Meaning |
|---|---|---|
| `IoLimits.max_read` | `int` | Maximum read transfer size in bytes. |
| `IoLimits.max_write` | `int` | Maximum write transfer size in bytes. |

### NfsError

Immutable structured base for all library NFS failures. Subclasses also preserve useful Python built-in exception inheritance. Plain Python TypeError/ValueError can be raised by facade argument validation. str(error) returns message; errors support exception chaining and serialization.

| Member | Type / value | Meaning |
|---|---|---|
| `NfsError.message` | `str` | Human-readable diagnostic message. |
| `NfsError.operation` | `str &#124; None` | Operation name, or None when no operation context is available. |
| `NfsError.protocol` | `str &#124; None` | Protocol identifier, or None on an error without protocol context. |
| `NfsError.code` | `int &#124; None` | Numeric protocol status, or None. |
| `NfsError.code_name` | `str &#124; None` | Symbolic protocol status, or None. |
| `NfsError.recovery_action` | `RecoveryAction &#124; None` | RecoveryAction to consider next; optional on generic NfsError. |
| `NfsError.outcome` | `OperationOutcome &#124; None` | OperationOutcome failure classification; optional on generic NfsError. |
| `NfsError.operation_class` | `OperationClass &#124; None` | OperationClass request classification, or None. |
| `NfsError.completed_bytes` | `int &#124; None` | Acknowledged write bytes, or None when unknown/not applicable. May cover non-contiguous ranges; not a resume offset or proof of durability. |
| `NfsError.errno` | `int &#124; None` | Mapped OS error number, or None. |
| `NfsError.filename` | `str &#124; None` | Path context, or None. |
| `NfsError.errors` | `tuple[NfsError, ...]` | Tuple of child NfsError values, especially for cleanup aggregation. |

#### NfsError.__init__

```python
def __init__(
    self,
    *,
    message: str,
    operation: str | None = None,
    protocol: str | None = None,
    code: int | None = None,
    code_name: str | None = None,
    recovery_action: RecoveryAction | str | None = None,
    outcome: OperationOutcome | str | None = None,
    operation_class: OperationClass | str | None = None,
    completed_bytes: int | None = None,
    errno: int | None = None,
    filename: str | None = None,
    errors: tuple[NfsError, ...] = (),
) -> None: ...
```

Construct a structured error with keyword-only fields. message is required; optional context defaults to None and errors defaults to an empty tuple. Enum-valued context accepts the enum or its listed string. Fields are immutable.

#### NfsError.with_context

```python
def with_context(
    self,
    *,
    operation: str | None = None,
    protocol: str | None = None,
    filename: str | None = None,
) -> NfsError: ...
```

Return a new error of the same concrete type, filling only currently missing operation/protocol/filename context. Existing context is preserved; this method does not mutate the original error.

### NfsNotFoundError

Path or entry not found; also FileNotFoundError.

Bases: `FileNotFoundError`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsAlreadyExistsError

An entry already exists; also FileExistsError.

Bases: `FileExistsError`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsPermissionError

Access or ownership permission denied; also PermissionError.

Bases: `PermissionError`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsIsADirectoryError

An operation requiring a non-directory received a directory; also IsADirectoryError.

Bases: `IsADirectoryError`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsNotADirectoryError

An operation requiring a directory received another type; also NotADirectoryError.

Bases: `NotADirectoryError`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsTimeoutError

Connection or operation deadline expired; also TimeoutError. Timeout alone does not prove a mutation had no effect.

Bases: `TimeoutError`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsConnectionError

Transport connection failure; also ConnectionError.

Bases: `ConnectionError`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsOSError

Other mapped operating-system error; also OSError. Inspect errno.

Bases: `OSError`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsMountError

Mount setup failed.

Bases: `NfsError`. Inherits all documented NfsError fields and methods.

### NfsRpcError

RPC framing, decoding or request handling failed.

Bases: `NfsError`. Inherits all documented NfsError fields and methods.

### NfsEncodingError

A name/value could not be represented using the API encoding rules.

Bases: `NfsError`. Inherits all documented NfsError fields and methods.

### NfsDirectoryEntryError

Directory entry conversion or required entry metadata failed.

Bases: `NfsError`. Inherits all documented NfsError fields and methods.

### NfsUnsupportedError

Operation or feature is unsupported; also NotImplementedError.

Bases: `NotImplementedError`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsInvalidInputError

Invalid native-operation input; also ValueError.

Bases: `ValueError`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsProtocolError

NFS protocol status failure; inspect protocol, code and code_name.

Bases: `NfsError`. Inherits all documented NfsError fields and methods.

### NfsStateLostError

Protocol state was lost; follow the supplied recovery action.

Bases: `NfsProtocolError`. Inherits all documented NfsError fields and methods.

### NfsRetryableError

A protocol failure classified as retryable; consult recovery_action and outcome.

Bases: `NfsProtocolError`. Inherits all documented NfsError fields and methods.

### NfsOperationOutcomeError

Failure carrying explicit outcome and recovery information.

Bases: `NfsError`. Inherits all documented NfsError fields and methods.

### NfsUncertainOutcomeError

A replay-sensitive operation may have taken effect. Verify server state before deciding what to resume.

Bases: `NfsOperationOutcomeError`. Inherits all documented NfsError fields and methods.

### NfsPositionUncertainError

The relative cursor is uncertain; use an absolute seek before further relative I/O.

Bases: `NfsError`. Inherits all documented NfsError fields and methods.

### NfsLostOpenStateError

Open state was lost; reopen the file as directed by recovery_action.

Bases: `NfsStateLostError`. Inherits all documented NfsError fields and methods.

### NfsClosedResourceError

Operation on a closed resource; also ValueError.

Bases: `ValueError`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsClientClosedError

The owning client is closing or closed.

Bases: `NfsClosedResourceError`. Inherits all documented NfsError fields and methods.

### NfsModeError

Operation conflicts with file mode or unsupported RawIOBase behavior; also io.UnsupportedOperation.

Bases: `io.UnsupportedOperation`, `NfsError`. Inherits all documented NfsError fields and methods.

### NfsFileCloseError

File cleanup completed with one or more failures. Inspect errors for each child failure.

Bases: `NfsError`. Inherits all documented NfsError fields and methods.

### NfsClientCloseError

Client/resource cleanup completed with one or more failures. Inspect errors.

Bases: `NfsError`. Inherits all documented NfsError fields and methods.

### Client

Synchronous client. Blocking native I/O releases the GIL. Create with connect; the client owns its open files.

#### Client.__init__

```python
def __init__(self, _private: NoReturn) -> None: ...
```

Direct construction is unsupported and raises TypeError. Use Client.connect or await AsyncClient.connect.

#### Client.connect

```python
def connect(
    cls,
    url: str,
    *,
    versions: tuple[str, ...] | list[str] | None = None,
    uid: int | None = None,
    gid: int | None = None,
    nfs_port: int | None = None,
    mount_port: int | None = None,
    readdir_buffer: int | tuple[int, int] | None = None,
    noresvport: bool | None = None,
    retain_delegations: bool | None = None,
    connect_timeout: float | None = None,
    operation_timeout: float | None = None,
    recovery_event_capacity: int = 256,
) -> Client: ...
```

Connect to the export in `url` and return an owned client. Use the connection options table below; explicit keyword options override matching URL options. Connection errors use the NfsError hierarchy. This is the public construction entry point.

#### Client.version

```python
def version(self) -> Version: ...
```

Read as a property; do not call it. Read-only connected protocol; no network request. This reports the selected version, not the requested fallback list.

#### Client.health

```python
def health(self) -> Health: ...
```

Read as a property; do not call it. Read-only lifecycle and recovery-generation snapshot; lease health can be None when not applicable. Reading it does not perform a liveness probe.

#### Client.capabilities

```python
def capabilities(self) -> Capabilities: ...
```

Read as a property; do not call it. Read-only negotiated feature snapshot. A flag does not add a Python method; locks, callbacks and session diagnostics are not separate public Python operations.

#### Client.io_limits

```python
def io_limits(self) -> IoLimits: ...
```

Read as a property; do not call it. Read-only effective negotiated read/write limits in bytes. All four File read methods and both write methods use these limits and at most eight concurrent chunks per call. pNFS data-server limits can reduce individual RPC sizes further.

#### Client.closed

```python
def closed(self) -> bool: ...
```

Read as a property; do not call it. Read-only local closed-state indicator.

#### Client.dropped_recovery_event_count

```python
def dropped_recovery_event_count(self) -> int: ...
```

Read as a property; do not call it. Read-only number of events discarded from the bounded recovery-event queue. Use it to detect incomplete diagnostic history.

#### Client.recovery_events

```python
def recovery_events(self) -> tuple[RecoveryEvent, ...]: ...
```

Return a tuple snapshot of retained recovery events without consuming them. This is a local synchronous operation, including on AsyncClient.

#### Client.drain_recovery_events

```python
def drain_recovery_events(self) -> tuple[RecoveryEvent, ...]: ...
```

Return and remove the currently retained events. This is synchronous on AsyncClient as well; it does not perform NFS I/O.

#### Client.close

```python
def close(self) -> None: ...
```

Close the client and its owned resources, wait for active operations to settle, and reject further operations. Repeated close is supported. Cleanup failures are reported by NfsClientCloseError with child errors; use a context manager for deterministic cleanup.

#### Client.stat

```python
def stat(self, path: os.PathLike[str] | str) -> FileInfo: ...
```

Return FileInfo for the export-relative path. Missing paths raise NfsNotFoundError; other failures are not converted into a missing result. See FileInfo for byte counts and nanosecond timestamps.

#### Client.exists

```python
def exists(self, path: os.PathLike[str] | str) -> bool: ...
```

Return False only when stat raises FileNotFoundError; permission, connection and other errors propagate. Return True when stat succeeds.

#### Client.scandir

```python
def scandir(
    self,
    path: DirectoryRef | DirEntry | os.PathLike[str] | str = '.',
) -> Iterator[DirEntry]: ...
```

Return a lazy iterator of DirEntry values backed by Mount.readdirplus. A DirectoryRef or DirEntry carrying fh skips path lookup; otherwise the path is resolved first. No recursive traversal or handle fallback is performed. Metadata accompanies each entry; fh can be None. Reuse handles only with the originating client. Errors may arise while consuming the iterator. AsyncClient.scandir is called without await and consumed with async for. Do not assume dot entries are filtered.

#### Client.listdir

```python
def listdir(self, path: os.PathLike[str] | str='.') -> list[str]: ...
```

Materialize scandir names into a list, without metadata or handles. The default path is the export root ("."). Errors propagate during enumeration; directory contents are not an atomic snapshot.

#### Client.chmod

```python
def chmod(self, path: os.PathLike[str] | str, mode: int) -> None: ...
```

Set permission/mode bits on path (for example 0o640). Returns None; server permission and protocol errors propagate.

#### Client.chown

```python
def chown(self, path: os.PathLike[str] | str, uid: int, gid: int) -> None: ...
```

Set numeric owner uid and group gid. Use -1 for either identity to leave it unchanged. Returns None; server permissions apply.

#### Client.utime

```python
def utime(self, path: os.PathLike[str] | str, *, ns: tuple[int, int]) -> None: ...
```

Set access and modification times using the keyword-only tuple ns=(atime_ns, mtime_ns), both integers in nanoseconds since the Unix epoch. This does not set ctime. A non-tuple or wrong tuple length is rejected.

#### Client.truncate

```python
def truncate(self, path: os.PathLike[str] | str, size: int) -> None: ...
```

Set the file size at path to a non-negative byte count. Shrinking discards the tail; extension follows server filesystem semantics. Returns None. This path operation is independent of any Python File cursor.

#### Client.access

```python
def access(self, path: os.PathLike[str] | str, mode: int) -> bool: ...
```

Check the requested access flags, combining os.R_OK, os.W_OK and os.X_OK; os.F_OK is zero. Returns a bool, while unrelated protocol/transport failures raise. Rejects booleans and bits outside 0o7. This check does not reserve permission for a later operation.

#### Client.getdacl

```python
def getdacl(self, path: os.PathLike[str] | str) -> NfsAcl41: ...
```

Return the NFSv4.1 discretionary ACL as NfsAcl41. This is a DACL attribute, not the ordinary NFSv4 acl attribute. Requires protocol/server support; unsupported cases raise NfsUnsupportedError or a server protocol error.

#### Client.setdacl

```python
def setdacl(self, path: os.PathLike[str] | str, acl: NfsAcl41) -> None: ...
```

Set the NFSv4.1 discretionary ACL from an NfsAcl41 instance. ACE order is preserved; the caller supplies the entire intended value. Returns None. Requires server support and authorization.

#### Client.getsacl

```python
def getsacl(self, path: os.PathLike[str] | str) -> NfsAcl41: ...
```

Return the NFSv4.1 system/audit ACL as NfsAcl41. Requires protocol/server support and suitable permissions; it is distinct from getdacl.

#### Client.setsacl

```python
def setsacl(self, path: os.PathLike[str] | str, acl: NfsAcl41) -> None: ...
```

Set the NFSv4.1 system/audit ACL from an NfsAcl41 instance. Returns None; server feature and authorization checks still apply.

#### Client.getxattr

```python
def getxattr(self, path: os.PathLike[str] | str, name: str) -> bytes: ...
```

Read a named extended attribute and return its value as bytes. name is a string. Requires named-attribute support; missing attributes and unsupported operations raise.

#### Client.setxattr

```python
def setxattr(self, path: os.PathLike[str] | str, name: str, value: Any) -> None: ...
```

Set a named extended attribute from a bytes-like value. The input is snapshotted before native I/O; returns None. No create-only/replace-only flag is exposed. Requires named-attribute support.

#### Client.listxattr

```python
def listxattr(self, path: os.PathLike[str] | str) -> list[str]: ...
```

Return available extended-attribute names as a list of strings. Requires named-attribute support; names are server dependent.

#### Client.removexattr

```python
def removexattr(self, path: os.PathLike[str] | str, name: str) -> None: ...
```

Remove one named extended attribute. Returns None; missing attributes and unsupported operations raise.

#### Client.fs_info

```python
def fs_info(self) -> FsInfo: ...
```

Return FsInfo describing the mounted filesystem transfer preferences, time resolution, limits and feature indicators.

#### Client.fs_stat

```python
def fs_stat(self) -> FsStat: ...
```

Return FsStat capacity/inode statistics for the mounted filesystem. Availability can differ from total free space because it reflects access restrictions or reserved space.

#### Client.mkdir

```python
def mkdir(
    self,
    path: os.PathLike[str] | str,
    mode: int = 511,
    *,
    parents: bool = False,
    exist_ok: bool = False,
) -> None: ...
```

Create a directory with mode=0o777 by default. parents=True creates missing components; exist_ok=True permits an existing target only when it is a directory. Parent creation is non-transactional: earlier directories can remain if a later step fails. Returns None.

#### Client.remove

```python
def remove(self, path: os.PathLike[str] | str, *, missing_ok: bool=False) -> None: ...
```

Remove a non-directory entry. missing_ok=True suppresses only FileNotFoundError; all other failures propagate. Use rmdir for directories. Returns None.

#### Client.unlink

```python
def unlink(self, path: os.PathLike[str] | str, *, missing_ok: bool=False) -> None: ...
```

Alias of remove, including missing_ok handling and errors. Returns None.

#### Client.rmdir

```python
def rmdir(self, path: os.PathLike[str] | str) -> None: ...
```

Remove an empty directory. A nonempty directory, wrong file type or denied access raises a server error. Returns None.

#### Client.rename

```python
def rename(self, source: os.PathLike[str] | str, destination: os.PathLike[str] | str) -> None: ...
```

Rename source to destination within the mounted export. Server replacement and permission rules apply. Returns None; uncertain mutation outcomes require verification before retry.

#### Client.link

```python
def link(self, source: os.PathLike[str] | str, destination: os.PathLike[str] | str) -> None: ...
```

Create destination as a hard link to source within the export. Requires filesystem link support. Returns None; an existing destination normally raises NfsAlreadyExistsError.

#### Client.symlink

```python
def symlink(self, target: os.PathLike[str] | str, link_path: os.PathLike[str] | str) -> None: ...
```

Create link_path containing target. link_path is normalized relative to the export; target is stored as supplied, including ../ or absolute targets, and is not normalized. Byte-string or NUL-containing targets are rejected. Returns None.

#### Client.readlink

```python
def readlink(self, path: os.PathLike[str] | str) -> str: ...
```

Return the stored symlink target as a string without normalizing or resolving it. A non-symlink path raises a server error.

#### Client.touch

```python
def touch(self, path: os.PathLike[str] | str, *, exist_ok: bool=True) -> None: ...
```

Create a missing file without truncating an existing one, then set atime/mtime to the current time. exist_ok=False is unsupported and raises NotImplementedError because atomic exclusive creation is unavailable. The create/close/time-update sequence is non-transactional.

#### Client.open

```python
def open(self, path: os.PathLike[str] | str, mode: str='rb') -> File: ...
```

Return a binary File (AsyncFile for AsyncClient). Supported modes are rb, wb, ab, r+b, w+b and a+b (equivalent b/+ ordering is accepted). r requires an existing file; w creates/truncates; a creates if needed and writes at EOF. Text and exclusive-create modes are rejected. Creating/truncating opens are non-transactional and can leave effects if a later step fails. Close the returned file explicitly or use its context manager.

#### Client.__enter__

```python
def __enter__(self) -> Client: ...
```

Return this client for a with block.

#### Client.__exit__

```python
def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None: ...
```

Close on leaving a with block. A body exception remains primary; additional cleanup failures are attached to it. Without a body exception, close errors propagate.

### AsyncClient

Asyncio client bound to its creating event loop. Network methods are awaited except scandir, which returns an async iterator directly. Properties, recovery_events and drain_recovery_events remain synchronous. Use async with await AsyncClient.connect(...).

#### AsyncClient.__init__

```python
def __init__(self, _private: NoReturn) -> None: ...
```

Direct construction is unsupported and raises TypeError. Use Client.connect or await AsyncClient.connect.

#### AsyncClient.connect

```python
async def connect(
    cls,
    url: str,
    *,
    versions: tuple[str, ...] | list[str] | None = None,
    uid: int | None = None,
    gid: int | None = None,
    nfs_port: int | None = None,
    mount_port: int | None = None,
    readdir_buffer: int | tuple[int, int] | None = None,
    noresvport: bool | None = None,
    retain_delegations: bool | None = None,
    connect_timeout: float | None = None,
    operation_timeout: float | None = None,
    recovery_event_capacity: int = 256,
) -> AsyncClient: ...
```

Connect to the export in `url` and return an owned client. Use the connection options table below; explicit keyword options override matching URL options. Connection errors use the NfsError hierarchy. This is the public construction entry point.

#### AsyncClient.version

```python
def version(self) -> Version: ...
```

Read as a property; do not call it. Read-only connected protocol; no network request. This reports the selected version, not the requested fallback list.

#### AsyncClient.health

```python
def health(self) -> Health: ...
```

Read as a property; do not call it. Read-only lifecycle and recovery-generation snapshot; lease health can be None when not applicable. Reading it does not perform a liveness probe.

#### AsyncClient.capabilities

```python
def capabilities(self) -> Capabilities: ...
```

Read as a property; do not call it. Read-only negotiated feature snapshot. A flag does not add a Python method; locks, callbacks and session diagnostics are not separate public Python operations.

#### AsyncClient.io_limits

```python
def io_limits(self) -> IoLimits: ...
```

Read as a property; do not call it. Read-only effective negotiated read/write limits in bytes. All four File read methods and both write methods use these limits and at most eight concurrent chunks per call. pNFS data-server limits can reduce individual RPC sizes further.

#### AsyncClient.closed

```python
def closed(self) -> bool: ...
```

Read as a property; do not call it. Read-only local closed-state indicator.

#### AsyncClient.dropped_recovery_event_count

```python
def dropped_recovery_event_count(self) -> int: ...
```

Read as a property; do not call it. Read-only number of events discarded from the bounded recovery-event queue. Use it to detect incomplete diagnostic history.

#### AsyncClient.recovery_events

```python
def recovery_events(self) -> tuple[RecoveryEvent, ...]: ...
```

Return a tuple snapshot of retained recovery events without consuming them. This is a local synchronous operation, including on AsyncClient.

#### AsyncClient.drain_recovery_events

```python
def drain_recovery_events(self) -> tuple[RecoveryEvent, ...]: ...
```

Return and remove the currently retained events. This is synchronous on AsyncClient as well; it does not perform NFS I/O.

#### AsyncClient.close

```python
async def close(self) -> None: ...
```

Close the client and its owned resources, wait for active operations to settle, and reject further operations. Repeated close is supported. Cleanup failures are reported by NfsClientCloseError with child errors; use a context manager for deterministic cleanup.

#### AsyncClient.stat

```python
async def stat(self, path: os.PathLike[str] | str) -> FileInfo: ...
```

Return FileInfo for the export-relative path. Missing paths raise NfsNotFoundError; other failures are not converted into a missing result. See FileInfo for byte counts and nanosecond timestamps.

#### AsyncClient.exists

```python
async def exists(self, path: os.PathLike[str] | str) -> bool: ...
```

Return False only when stat raises FileNotFoundError; permission, connection and other errors propagate. Return True when stat succeeds.

#### AsyncClient.scandir

```python
def scandir(
    self,
    path: DirectoryRef | DirEntry | os.PathLike[str] | str = '.',
) -> AsyncIterator[DirEntry]: ...
```

Return a lazy iterator of DirEntry values backed by Mount.readdirplus. A DirectoryRef or DirEntry carrying fh skips path lookup; otherwise the path is resolved first. No recursive traversal or handle fallback is performed. Metadata accompanies each entry; fh can be None. Reuse handles only with the originating client. Errors may arise while consuming the iterator. AsyncClient.scandir is called without await and consumed with async for. Do not assume dot entries are filtered.

#### AsyncClient.listdir

```python
async def listdir(self, path: os.PathLike[str] | str='.') -> list[str]: ...
```

Materialize scandir names into a list, without metadata or handles. The default path is the export root ("."). Errors propagate during enumeration; directory contents are not an atomic snapshot.

#### AsyncClient.chmod

```python
async def chmod(self, path: os.PathLike[str] | str, mode: int) -> None: ...
```

Set permission/mode bits on path (for example 0o640). Returns None; server permission and protocol errors propagate.

#### AsyncClient.chown

```python
async def chown(self, path: os.PathLike[str] | str, uid: int, gid: int) -> None: ...
```

Set numeric owner uid and group gid. Use -1 for either identity to leave it unchanged. Returns None; server permissions apply.

#### AsyncClient.utime

```python
async def utime(self, path: os.PathLike[str] | str, *, ns: tuple[int, int]) -> None: ...
```

Set access and modification times using the keyword-only tuple ns=(atime_ns, mtime_ns), both integers in nanoseconds since the Unix epoch. This does not set ctime. A non-tuple or wrong tuple length is rejected.

#### AsyncClient.truncate

```python
async def truncate(self, path: os.PathLike[str] | str, size: int) -> None: ...
```

Set the file size at path to a non-negative byte count. Shrinking discards the tail; extension follows server filesystem semantics. Returns None. This path operation is independent of any Python File cursor.

#### AsyncClient.access

```python
async def access(self, path: os.PathLike[str] | str, mode: int) -> bool: ...
```

Check the requested access flags, combining os.R_OK, os.W_OK and os.X_OK; os.F_OK is zero. Returns a bool, while unrelated protocol/transport failures raise. Rejects booleans and bits outside 0o7. This check does not reserve permission for a later operation.

#### AsyncClient.getdacl

```python
async def getdacl(self, path: os.PathLike[str] | str) -> NfsAcl41: ...
```

Return the NFSv4.1 discretionary ACL as NfsAcl41. This is a DACL attribute, not the ordinary NFSv4 acl attribute. Requires protocol/server support; unsupported cases raise NfsUnsupportedError or a server protocol error.

#### AsyncClient.setdacl

```python
async def setdacl(self, path: os.PathLike[str] | str, acl: NfsAcl41) -> None: ...
```

Set the NFSv4.1 discretionary ACL from an NfsAcl41 instance. ACE order is preserved; the caller supplies the entire intended value. Returns None. Requires server support and authorization.

#### AsyncClient.getsacl

```python
async def getsacl(self, path: os.PathLike[str] | str) -> NfsAcl41: ...
```

Return the NFSv4.1 system/audit ACL as NfsAcl41. Requires protocol/server support and suitable permissions; it is distinct from getdacl.

#### AsyncClient.setsacl

```python
async def setsacl(self, path: os.PathLike[str] | str, acl: NfsAcl41) -> None: ...
```

Set the NFSv4.1 system/audit ACL from an NfsAcl41 instance. Returns None; server feature and authorization checks still apply.

#### AsyncClient.getxattr

```python
async def getxattr(self, path: os.PathLike[str] | str, name: str) -> bytes: ...
```

Read a named extended attribute and return its value as bytes. name is a string. Requires named-attribute support; missing attributes and unsupported operations raise.

#### AsyncClient.setxattr

```python
async def setxattr(self, path: os.PathLike[str] | str, name: str, value: Any) -> None: ...
```

Set a named extended attribute from a bytes-like value. The input is snapshotted before native I/O; returns None. No create-only/replace-only flag is exposed. Requires named-attribute support.

#### AsyncClient.listxattr

```python
async def listxattr(self, path: os.PathLike[str] | str) -> list[str]: ...
```

Return available extended-attribute names as a list of strings. Requires named-attribute support; names are server dependent.

#### AsyncClient.removexattr

```python
async def removexattr(self, path: os.PathLike[str] | str, name: str) -> None: ...
```

Remove one named extended attribute. Returns None; missing attributes and unsupported operations raise.

#### AsyncClient.fs_info

```python
async def fs_info(self) -> FsInfo: ...
```

Return FsInfo describing the mounted filesystem transfer preferences, time resolution, limits and feature indicators.

#### AsyncClient.fs_stat

```python
async def fs_stat(self) -> FsStat: ...
```

Return FsStat capacity/inode statistics for the mounted filesystem. Availability can differ from total free space because it reflects access restrictions or reserved space.

#### AsyncClient.mkdir

```python
async def mkdir(
    self,
    path: os.PathLike[str] | str,
    mode: int = 511,
    *,
    parents: bool = False,
    exist_ok: bool = False,
) -> None: ...
```

Create a directory with mode=0o777 by default. parents=True creates missing components; exist_ok=True permits an existing target only when it is a directory. Parent creation is non-transactional: earlier directories can remain if a later step fails. Returns None.

#### AsyncClient.remove

```python
async def remove(self, path: os.PathLike[str] | str, *, missing_ok: bool=False) -> None: ...
```

Remove a non-directory entry. missing_ok=True suppresses only FileNotFoundError; all other failures propagate. Use rmdir for directories. Returns None.

#### AsyncClient.unlink

```python
async def unlink(self, path: os.PathLike[str] | str, *, missing_ok: bool=False) -> None: ...
```

Alias of remove, including missing_ok handling and errors. Returns None.

#### AsyncClient.rmdir

```python
async def rmdir(self, path: os.PathLike[str] | str) -> None: ...
```

Remove an empty directory. A nonempty directory, wrong file type or denied access raises a server error. Returns None.

#### AsyncClient.rename

```python
async def rename(
    self,
    source: os.PathLike[str] | str,
    destination: os.PathLike[str] | str,
) -> None: ...
```

Rename source to destination within the mounted export. Server replacement and permission rules apply. Returns None; uncertain mutation outcomes require verification before retry.

#### AsyncClient.link

```python
async def link(
    self,
    source: os.PathLike[str] | str,
    destination: os.PathLike[str] | str,
) -> None: ...
```

Create destination as a hard link to source within the export. Requires filesystem link support. Returns None; an existing destination normally raises NfsAlreadyExistsError.

#### AsyncClient.symlink

```python
async def symlink(
    self,
    target: os.PathLike[str] | str,
    link_path: os.PathLike[str] | str,
) -> None: ...
```

Create link_path containing target. link_path is normalized relative to the export; target is stored as supplied, including ../ or absolute targets, and is not normalized. Byte-string or NUL-containing targets are rejected. Returns None.

#### AsyncClient.readlink

```python
async def readlink(self, path: os.PathLike[str] | str) -> str: ...
```

Return the stored symlink target as a string without normalizing or resolving it. A non-symlink path raises a server error.

#### AsyncClient.touch

```python
async def touch(self, path: os.PathLike[str] | str, *, exist_ok: bool=True) -> None: ...
```

Create a missing file without truncating an existing one, then set atime/mtime to the current time. exist_ok=False is unsupported and raises NotImplementedError because atomic exclusive creation is unavailable. The create/close/time-update sequence is non-transactional.

#### AsyncClient.open

```python
async def open(self, path: os.PathLike[str] | str, mode: str='rb') -> AsyncFile: ...
```

Return a binary File (AsyncFile for AsyncClient). Supported modes are rb, wb, ab, r+b, w+b and a+b (equivalent b/+ ordering is accepted). r requires an existing file; w creates/truncates; a creates if needed and writes at EOF. Text and exclusive-create modes are rejected. Creating/truncating opens are non-transactional and can leave effects if a later step fails. Close the returned file explicitly or use its context manager.

#### AsyncClient.__aenter__

```python
async def __aenter__(self) -> AsyncClient: ...
```

Return this client for an async with block.

#### AsyncClient.__aexit__

```python
async def __aexit__(
    self,
    exc_type: object,
    exc: BaseException | None,
    traceback: object,
) -> None: ...
```

Await close on leaving an async with block. A body exception remains primary and cleanup failures are attached; otherwise close errors propagate.

### File

Synchronous binary file implementing io.RawIOBase. Relative operations share one cursor and are serialized; positional operations leave it unchanged. Use independent offsets for concurrent callers. Closed and incompatible-mode operations raise typed errors.

#### File.__init__

```python
def __init__(self, _private: NoReturn) -> None: ...
```

Direct construction is unsupported and raises TypeError. Use client.open; await the open when using AsyncClient.

#### File.name

```python
def name(self) -> str: ...
```

Read as a property; do not call it. Read-only normalized export-relative filename retained by this object; renaming externally does not update this label.

#### File.mode

```python
def mode(self) -> str: ...
```

Read as a property; do not call it. Read-only validated binary open mode.

#### File.closed

```python
def closed(self) -> bool: ...
```

Read as a property; do not call it. Read-only local closed-state indicator.

#### File.readable

```python
def readable(self) -> bool: ...
```

Return whether the open mode permits reading. This checks the mode, not current server access permissions.

#### File.writable

```python
def writable(self) -> bool: ...
```

Return whether the open mode permits writing. This checks the mode, not current server access permissions.

#### File.seekable

```python
def seekable(self) -> bool: ...
```

Return True: the synchronous File supports explicit positioning, although append writes still choose EOF.

#### File.read

```python
def read(self, size: int=-1) -> bytes: ...
```

Read size bytes from the current position and return bytes; size=-1 reads to EOF in bounded batches. Zero returns b"" without a READ; size below -1 is rejected. Split by max_read with up to eight concurrent chunks, continuing short responses until filled or EOF. Advance the position by the returned length. Owned RPC buffers are copied once into the final Python bytes allocation.

#### File.readinto

```python
def readinto(self, target: Any) -> int: ...
```

Fill a writable C-contiguous buffer from the current position and return the number of bytes filled. Continue short responses until full or EOF, with at most eight negotiated-size chunks active. Only target[:count] is valid; leave its tail untouched. Advance the position by count. No full-size intermediate bytes result is allocated. Do not access or resize the target concurrently; it is pinned during the call. On failure/cancellation a prefix may have changed, but background work cannot modify it after the call returns.

#### File.read_at

```python
def read_at(self, offset: int, size: int=-1) -> bytes: ...
```

Same byte-returning and concurrent-read behavior as read, starting at the non-negative absolute byte offset. Leave the relative position unchanged. size=-1 reads from offset to EOF. A range overflowing the supported offset representation is rejected.

#### File.readinto_at

```python
def readinto_at(self, target: Any, offset: int) -> int: ...
```

Same buffer requirements, concurrency, EOF behavior and cancellation guarantees as readinto, starting at the non-negative byte offset. Leave the relative position unchanged. An empty target returns zero without a READ.

#### File.seek

```python
def seek(self, offset: int, whence: int=io.SEEK_SET) -> int: ...
```

Set the byte position and return it. whence accepts io.SEEK_SET (0), io.SEEK_CUR (1) or io.SEEK_END (2); the default is SEEK_SET. The resulting position must be non-negative. An absolute seek can restore a position marked uncertain after a failed relative write. Seeking does not change where append-mode writes choose EOF.

#### File.tell

```python
def tell(self) -> int: ...
```

Return the current byte position without NFS I/O. Synchronous on AsyncFile too. Raise NfsPositionUncertainError when a failed write left the position uncertain, or a closed-resource error after close.

#### File.write

```python
def write(self, data: Any) -> int: ...
```

Write the whole bytes-like input and return its byte count after durability completes. Snapshot the input, split by max_write, run at most eight UNSTABLE chunks, complete short writes, then synchronize once for the batch as required (including pNFS metadata). If every response is FILE_SYNC, no extra COMMIT is needed. Advance the relative position; append mode selects EOF. This is not an atomic append guarantee across independent clients. Failure may have changed non-contiguous ranges; completed_bytes is not a resume offset or durability guarantee.

#### File.write_at

```python
def write_at(self, data: Any, offset: int) -> int: ...
```

Same input snapshot, concurrency and durability guarantees as write, at a non-negative byte offset without changing the relative position. Append mode rejects positional writes with NfsModeError.

#### File.truncate

```python
def truncate(self, size: int | None=None) -> int: ...
```

Set the file length to size bytes and return the resulting size. None uses the current position. Requires a writable mode; does not move the position. Shrinking discards data after the new end.

#### File.flush

```python
def flush(self) -> None: ...
```

Wait for active writable-file mutations to settle. Successful writes are already durable; this does not defer normal write commits. Returns None. A read-only flush is a no-op; a closed file raises.

#### File.fileno

```python
def fileno(self) -> int: ...
```

Always raise NfsModeError (an io.UnsupportedOperation subclass). A userspace NFS file has no OS file descriptor.

#### File.close

```python
def close(self) -> None: ...
```

Drain active operations and release open state. Repeated close is supported. Report cleanup failures through NfsFileCloseError.errors. Closing also occurs when its owning client closes. Use explicit close or a context manager; garbage collection is not a reliable cleanup mechanism.

#### File.__enter__

```python
def __enter__(self) -> File: ...
```

Return this File for a with block.

#### File.__exit__

```python
def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None: ...
```

Close the File on block exit; preserve an existing body exception and attach cleanup failures, otherwise propagate a close failure.

### AsyncFile

Async binary file bound to its creating event loop. Await I/O and lifecycle methods; name, mode, closed, readable, writable and tell remain synchronous. It does not inherit io.RawIOBase. Relative operations share a serialized cursor; positional operations do not change it.

#### AsyncFile.__init__

```python
def __init__(self, _private: NoReturn) -> None: ...
```

Direct construction is unsupported and raises TypeError. Use client.open; await the open when using AsyncClient.

#### AsyncFile.name

```python
def name(self) -> str: ...
```

Read as a property; do not call it. Read-only normalized export-relative filename retained by this object; renaming externally does not update this label.

#### AsyncFile.mode

```python
def mode(self) -> str: ...
```

Read as a property; do not call it. Read-only validated binary open mode.

#### AsyncFile.closed

```python
def closed(self) -> bool: ...
```

Read as a property; do not call it. Read-only local closed-state indicator.

#### AsyncFile.tell

```python
def tell(self) -> int: ...
```

Return the current byte position without NFS I/O. Synchronous on AsyncFile too. Raise NfsPositionUncertainError when a failed write left the position uncertain, or a closed-resource error after close.

#### AsyncFile.readable

```python
def readable(self) -> bool: ...
```

Return whether the open mode permits reading. This checks the mode, not current server access permissions.

#### AsyncFile.writable

```python
def writable(self) -> bool: ...
```

Return whether the open mode permits writing. This checks the mode, not current server access permissions.

#### AsyncFile.read

```python
async def read(self, size: int=-1) -> bytes: ...
```

Read size bytes from the current position and return bytes; size=-1 reads to EOF in bounded batches. Zero returns b"" without a READ; size below -1 is rejected. Split by max_read with up to eight concurrent chunks, continuing short responses until filled or EOF. Advance the position by the returned length. Owned RPC buffers are copied once into the final Python bytes allocation.

#### AsyncFile.readinto

```python
async def readinto(self, target: Any) -> int: ...
```

Fill a writable C-contiguous buffer from the current position and return the number of bytes filled. Continue short responses until full or EOF, with at most eight negotiated-size chunks active. Only target[:count] is valid; leave its tail untouched. Advance the position by count. No full-size intermediate bytes result is allocated. Do not access or resize the target concurrently; it is pinned during the call. On failure/cancellation a prefix may have changed, but background work cannot modify it after the call returns.

#### AsyncFile.read_at

```python
async def read_at(self, offset: int, size: int=-1) -> bytes: ...
```

Same byte-returning and concurrent-read behavior as read, starting at the non-negative absolute byte offset. Leave the relative position unchanged. size=-1 reads from offset to EOF. A range overflowing the supported offset representation is rejected.

#### AsyncFile.readinto_at

```python
async def readinto_at(self, target: Any, offset: int) -> int: ...
```

Same buffer requirements, concurrency, EOF behavior and cancellation guarantees as readinto, starting at the non-negative byte offset. Leave the relative position unchanged. An empty target returns zero without a READ.

#### AsyncFile.seek

```python
async def seek(self, offset: int, whence: int=io.SEEK_SET) -> int: ...
```

Set the byte position and return it. whence accepts io.SEEK_SET (0), io.SEEK_CUR (1) or io.SEEK_END (2); the default is SEEK_SET. The resulting position must be non-negative. An absolute seek can restore a position marked uncertain after a failed relative write. Seeking does not change where append-mode writes choose EOF.

#### AsyncFile.write

```python
async def write(self, data: Any) -> int: ...
```

Write the whole bytes-like input and return its byte count after durability completes. Snapshot the input, split by max_write, run at most eight UNSTABLE chunks, complete short writes, then synchronize once for the batch as required (including pNFS metadata). If every response is FILE_SYNC, no extra COMMIT is needed. Advance the relative position; append mode selects EOF. This is not an atomic append guarantee across independent clients. Failure may have changed non-contiguous ranges; completed_bytes is not a resume offset or durability guarantee.

#### AsyncFile.write_at

```python
async def write_at(self, data: Any, offset: int) -> int: ...
```

Same input snapshot, concurrency and durability guarantees as write, at a non-negative byte offset without changing the relative position. Append mode rejects positional writes with NfsModeError.

#### AsyncFile.truncate

```python
async def truncate(self, size: int | None=None) -> int: ...
```

Set the file length to size bytes and return the resulting size. None uses the current position. Requires a writable mode; does not move the position. Shrinking discards data after the new end.

#### AsyncFile.flush

```python
async def flush(self) -> None: ...
```

Wait for active writable-file mutations to settle. Successful writes are already durable; this does not defer normal write commits. Returns None. A read-only flush is a no-op; a closed file raises.

#### AsyncFile.close

```python
async def close(self) -> None: ...
```

Drain active operations and release open state. Repeated close is supported. Report cleanup failures through NfsFileCloseError.errors. Closing also occurs when its owning client closes. Use explicit close or a context manager; garbage collection is not a reliable cleanup mechanism.

#### AsyncFile.__aenter__

```python
async def __aenter__(self) -> AsyncFile: ...
```

Return this AsyncFile for an async with block.

#### AsyncFile.__aexit__

```python
async def __aexit__(
    self,
    exc_type: object,
    exc: BaseException | None,
    traceback: object,
) -> None: ...
```

Await close on block exit; preserve a body exception and attach cleanup failures, otherwise propagate a close failure.

## Inherited synchronous File conveniences

File also inherits the Python io.RawIOBase/IOBase convenience surface. These
methods call the File primitives above; they do not add new NFS operations or
change write durability. They are not methods on AsyncFile.

| Interface | Behavior |
|---|---|
| `File.readall()` | Return bytes from the current position to EOF. |
| `File.readline(size=-1)` | Return one binary line, at most size bytes if non-negative. |
| `File.readlines(hint=-1)` | Return a list of binary lines; a positive hint may stop after that total size is exceeded. |
| `File.writelines(lines)` | Write each bytes-like line without adding separators; returns None. |
| `File.isatty()` | False for an open file; closed-file checks follow IOBase. |
| `File.__iter__()` / `File.__next__()` | Iterate binary lines until StopIteration. |

For buffered small reads, wrap File in io.BufferedReader. For bulk concurrent
reads, call read(size) or readinto(buffer) with the desired buffer size. The
inherited line-reading helpers are not a parallel line-scanning interface.

## Migration and packaged documentation

Client.read_bytes and Client.write_bytes (including async twins) were removed.
Use `with client.open(path, "rb") as file: data = file.read()` and the analogous
"wb"/write workflow; in async code use `async with await client.open(...)`.
All four File read methods remain supported.

Read the complete installed documentation without network access:

```python
from importlib.resources import files

api = files("nfs_rs").joinpath("API.md").read_text(encoding="utf-8")
guide = files("nfs_rs").joinpath("GUIDE.md").read_text(encoding="utf-8")
print(api)
```
