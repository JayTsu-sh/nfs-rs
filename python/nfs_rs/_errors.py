from __future__ import annotations

from enum import Enum
import io
from typing import Any


class OperationOutcome(str, Enum):
    DEFINITE_FAILURE = "definite_failure"
    SAFE_TO_RETRY = "safe_to_retry"
    UNCERTAIN = "uncertain"


class OperationClass(str, Enum):
    READ_ONLY = "read_only"
    SESSION_CONTROL = "session_control"
    REPLAY_SENSITIVE = "replay_sensitive"


class RecoveryAction(str, Enum):
    RETRY = "retry"
    REOPEN = "reopen"
    REMOUNT = "remount"
    VERIFY_THEN_RESUME = "verify_then_resume"
    DO_NOT_RETRY = "do_not_retry"


def _restore_error(error_type: type[NfsError], state: dict[str, Any]) -> NfsError:
    return error_type(**state)


class NfsError(RuntimeError):
    """Structured, immutable public base for every nfs-rs failure."""

    message: str
    operation: str | None
    protocol: str | None
    code: int | None
    code_name: str | None
    recovery_action: RecoveryAction | None
    outcome: OperationOutcome | None
    operation_class: OperationClass | None
    completed_bytes: int | None
    errno: int | None
    filename: str | None
    errors: tuple[NfsError, ...]

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
    ) -> None:
        object.__setattr__(self, "_frozen", False)
        values = {
            "message": message,
            "operation": operation,
            "protocol": protocol,
            "code": code,
            "code_name": code_name,
            "recovery_action": RecoveryAction(recovery_action) if recovery_action else None,
            "outcome": OperationOutcome(outcome) if outcome else None,
            "operation_class": OperationClass(operation_class) if operation_class else None,
            "completed_bytes": completed_bytes,
            "errno": errno,
            "filename": filename,
            "errors": tuple(errors),
        }
        for name, value in values.items():
            object.__setattr__(self, name, value)
        Exception.__init__(self, message)
        object.__setattr__(self, "_frozen", True)

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_frozen", False) and name not in {
            "__traceback__", "__cause__", "__context__", "__suppress_context__", "__notes__"
        }:
            raise AttributeError(f"{type(self).__name__} fields are read-only")
        object.__setattr__(self, name, value)

    def __reduce__(self):
        state = {
            name: getattr(self, name)
            for name in (
                "message", "operation", "protocol", "code", "code_name",
                "recovery_action", "outcome", "operation_class", "completed_bytes",
                "errno", "filename", "errors",
            )
        }
        return _restore_error, (type(self), state)

    def __str__(self) -> str:
        return self.message

    def with_context(
        self,
        *,
        operation: str | None = None,
        protocol: str | None = None,
        filename: str | None = None,
    ) -> NfsError:
        state = self.__reduce__()[1][1]
        if self.operation is None:
            state["operation"] = operation
        if self.protocol is None:
            state["protocol"] = protocol
        if self.filename is None:
            state["filename"] = filename
        return type(self)(**state)


def _structured_init(self: NfsError, **kwargs: Any) -> None:
    NfsError.__init__(self, **kwargs)


class NfsNotFoundError(FileNotFoundError, NfsError): __init__ = _structured_init
class NfsAlreadyExistsError(FileExistsError, NfsError): __init__ = _structured_init
class NfsPermissionError(PermissionError, NfsError): __init__ = _structured_init
class NfsIsADirectoryError(IsADirectoryError, NfsError): __init__ = _structured_init
class NfsNotADirectoryError(NotADirectoryError, NfsError): __init__ = _structured_init
class NfsTimeoutError(TimeoutError, NfsError): __init__ = _structured_init
class NfsConnectionError(ConnectionError, NfsError): __init__ = _structured_init
class NfsOSError(OSError, NfsError): __init__ = _structured_init
class NfsMountError(NfsError): pass
class NfsRpcError(NfsError): pass
class NfsEncodingError(NfsError): pass
class NfsDirectoryEntryError(NfsError): pass
class NfsUnsupportedError(NotImplementedError, NfsError): __init__ = _structured_init
class NfsInvalidInputError(ValueError, NfsError): __init__ = _structured_init
class NfsProtocolError(NfsError): pass
class NfsStateLostError(NfsProtocolError): pass
class NfsRetryableError(NfsProtocolError): pass
class NfsOperationOutcomeError(NfsError): pass
class NfsUncertainOutcomeError(NfsOperationOutcomeError): pass
class NfsPositionUncertainError(NfsError): pass
class NfsLostOpenStateError(NfsStateLostError): pass
class NfsClosedResourceError(ValueError, NfsError): __init__ = _structured_init
class NfsClientClosedError(NfsClosedResourceError): pass
class NfsModeError(io.UnsupportedOperation, NfsError): __init__ = _structured_init
class NfsFileCloseError(NfsError): pass
class NfsClientCloseError(NfsError): pass

for _builtin_error_type in (
    NfsNotFoundError, NfsAlreadyExistsError, NfsPermissionError,
    NfsIsADirectoryError, NfsNotADirectoryError, NfsTimeoutError,
    NfsConnectionError, NfsOSError, NfsUnsupportedError,
    NfsInvalidInputError, NfsClosedResourceError, NfsModeError,
):
    _builtin_error_type.__reduce__ = NfsError.__reduce__
    _builtin_error_type.__str__ = NfsError.__str__


__all__ = [name for name in tuple(globals()) if name.startswith("Nfs") or name in {"OperationOutcome", "OperationClass", "RecoveryAction"}]
