# NFSv4.2 baseline protocol notes

## Scope

The baseline implementation runs the existing Rust/Python file APIs over
NFSv4.2. Subsequent optional COPY/CLONE, sparse operations and RFC 8276 xattrs
are described in [the operation notes](nfsv42-operations-protocol-notes.md).
Cross-server/asynchronous offload remains outside the public API.

## Protocol evidence

- NFSv4.2 retains v4.1 infrastructure and existing feature statuses; its
  extensions are independently optional. Clients implement operations needed
  by their environment. This permits reusing the existing session/file engine.
  [RFC 7862 sections 2 and 13](https://www.rfc-editor.org/rfc/rfc7862.html#section-2).
- A clientid belongs to one minor version. Do not reuse it across v4.1/v4.2
  mounts or change minor version during recovery.
  [RFC 8178 section 8.1](https://www.rfc-editor.org/rfc/rfc8178.html#section-8.1).
- COMPOUND carries the minor version. An unsupported version produces
  `NFS4ERR_MINOR_VERS_MISMATCH` with an empty result array; handle compound
  status before assuming an operation result exists.
  [RFC 8881 section 16.2.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-16.2.3).
- CB_COMPOUND must use the minor version that created its clientid/session.
  A v4.2 session therefore requires callback minor version 2, including
  CB_SEQUENCE and layout recalls.
  [RFC 8881 section 19.2.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-19.2.3).
- A v4.2 metadata server may supply a file layout targeting v4.1 data servers,
  including stateids usable there. Negotiate DS minor versions independently;
  trying 2 then 1 on `MINOR_VERS_MISMATCH` is explicitly described.
  [RFC 7862 sections 3.2–3.3](https://www.rfc-editor.org/rfc/rfc7862.html#section-3.2).
- v4.2 clients should request `EXCHGID4_FLAG_SUPP_FENCE_OPS` (`0x00000004`).
  Fencing guarantees apply only when the server returns that flag. Preserve
  existing uncertainty semantics when it is absent.
  [RFC 7862 section 14.1](https://www.rfc-editor.org/rfc/rfc7862.html#section-14.1).
- v4.2 prohibits GETDEVICELIST; discover devices through layouts and
  GETDEVICEINFO.
  [RFC 7862 section 14.2](https://www.rfc-editor.org/rfc/rfc7862.html#section-14.2).

## Implementation and validation decisions

Store the selected minor version with session identity and use it for
establishment, ordinary requests, lease renewal, connection binding,
session recovery, and teardown. Bind callback validation to that same
session version. Keep the existing state, retry, cancellation, and cleanup
rules.

The public exact selector `version=4.2` should fail if unavailable; explicit
ordered selectors can authorize MDS fallback. This is the API policy for
observable version selection. RFC 8178 section 8.2 recommends searching
lower versions, but does not prescribe URL semantics.
[RFC 8178 section 8.2](https://www.rfc-editor.org/rfc/rfc8178.html#section-8.2).

Regression coverage should inspect request minor versions across setup,
I/O, recovery and cleanup; exercise v4.2 callbacks and mixed-version pNFS;
verify actual reported Rust/Python versions; and preserve explicit failure
on an unsupported exact selector. Physical AFX evidence must identify each
endpoint and record unsupported capabilities separately.
