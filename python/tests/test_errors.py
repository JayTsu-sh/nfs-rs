import io
import pickle

import pytest
import nfs_rs

from nfs_rs import (
    NfsAlreadyExistsError, NfsClosedResourceError, NfsConnectionError, NfsError,
    NfsInvalidInputError, NfsIsADirectoryError, NfsModeError,
    NfsNotADirectoryError, NfsNotFoundError, NfsOSError, NfsPermissionError,
    NfsTimeoutError, NfsUncertainOutcomeError, NfsUnsupportedError, OperationClass,
    OperationOutcome, RecoveryAction,
)


@pytest.mark.parametrize(
    ("error_type", "builtin"),
    [
        (NfsNotFoundError, FileNotFoundError),
        (NfsAlreadyExistsError, FileExistsError),
        (NfsPermissionError, PermissionError),
        (NfsIsADirectoryError, IsADirectoryError),
        (NfsNotADirectoryError, NotADirectoryError),
        (NfsTimeoutError, TimeoutError),
        (NfsConnectionError, ConnectionError),
        (NfsOSError, OSError),
        (NfsUnsupportedError, NotImplementedError),
        (NfsInvalidInputError, ValueError),
        (NfsClosedResourceError, ValueError),
        (NfsModeError, io.UnsupportedOperation),
    ],
)
def test_filesystem_errors_keep_builtin_and_nfs_inheritance(error_type, builtin):
    error = error_type(message="failure", operation="stat", filename="safe/path")
    assert isinstance(error, builtin)
    assert isinstance(error, NfsError)
    assert str(error) == "failure"


def test_structured_fields_are_immutable_and_pickle_stable():
    error = NfsUncertainOutcomeError(
        message="unknown result",
        operation="write",
        protocol="4.1",
        code=10086,
        code_name="NFS4ERR_RETRY_UNCACHED_REP",
        recovery_action=RecoveryAction.VERIFY_THEN_RESUME,
        outcome=OperationOutcome.UNCERTAIN,
        operation_class=OperationClass.REPLAY_SENSITIVE,
        completed_bytes=8,
        filename="safe/file",
    )
    restored = pickle.loads(pickle.dumps(error))
    assert type(restored) is type(error)
    assert restored.__reduce__()[1][1] == error.__reduce__()[1][1]
    with pytest.raises(AttributeError):
        error.completed_bytes = 9
    error.__traceback__ = None


def test_context_enrichment_never_overwrites_authoritative_fields():
    original = NfsNotFoundError(message="missing", operation="lookup", protocol="3")
    enriched = original.with_context(operation="stat", protocol="4.1", filename="safe")
    assert enriched.operation == "lookup"
    assert enriched.protocol == "3"
    assert enriched.filename == "safe"


@pytest.mark.parametrize(
    "error_type",
    [
        getattr(nfs_rs, name)
        for name in nfs_rs.__all__
        if name.startswith("Nfs")
        and isinstance(getattr(nfs_rs, name), type)
        and issubclass(getattr(nfs_rs, name), NfsError)
    ],
)
def test_every_public_exception_is_pickle_stable(error_type):
    error = error_type(message="stable", operation="test", errors=())
    restored = pickle.loads(pickle.dumps(error))
    assert type(restored) is error_type
    assert restored.__reduce__()[1][1] == error.__reduce__()[1][1]
