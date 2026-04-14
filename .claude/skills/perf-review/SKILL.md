---
name: perf-review
description: Review recent code changes for async Rust and NFS performance anti-patterns
user-invocable: false
---

Review changed files for these performance anti-patterns:

## Memory / Allocation
- `Vec<u8>` clone instead of `Bytes`/`BytesMut` zero-copy
- Unnecessary `.clone()` on `Bytes` or `Arc` (though Bytes clone is cheap)
- `String::from_utf8_lossy().into_owned()` where `Cow` or `&str` would suffice
- Per-request `Vec` allocations in hot loops (prefer reuse or iterators)

## Async / Concurrency
- `std::sync::Mutex` held across `.await` points (use `tokio::sync::Mutex`)
- `std::sync::Mutex` on hot paths where `DashMap` or atomics would be better
- Blocking IO (`std::fs`, `std::net`) in async context
- Sequential independent operations that could use `tokio::try_join!`
- `futures::executor::block_on` instead of `block_on_compat` in async context

## Network / RPC
- Missing `TCP_NODELAY` on new connections
- Multiple `write_all` that could be `write_vectored`
- Unbounded response buffer allocation (check MAX_RPC_RESPONSE_SIZE)
- Fixed timeouts that should scale with data size

## NFS-Specific
- `readdir`/`readdirplus` not using `paged_dir_stream!` macro
- Hardcoded buffer sizes instead of using `self.dircount`/`self.maxcount`/`self.rsize`/`self.wsize`
- Path resolution doing sequential lookups without caching

Report findings with severity and specific line references.
