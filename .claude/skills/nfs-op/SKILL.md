---
name: nfs-op
description: Scaffold a new NFSv3 procedure implementation with XDR encoding, nfs3_call!, path variant, and trait wiring
---

When adding a new NFSv3 procedure, follow this checklist:

## 1. Create `src/nfs3/<op>.rs`

Define the XDR request args struct with `encode()`:
```rust
use super::{nfs_fh3, Mount, Result};
use bytes::Bytes;

#[allow(unused)]
impl Mount {
    pub async fn <op>(&self, fh: Bytes, ...) -> Result<...> {
        let args = <OP>3args { ... };
        self._<op>(args).await
    }

    pub async fn <op>_path(&self, path: &str, ...) -> Result<...> {
        let fh = self.lookup_path(path).await?.fh;
        self.<op>(fh, ...).await
    }
}
```

## 2. Add to `src/nfs3/mod.rs`

- Add `mod <op>;` to the module list
- Add `nfs3_call!(<op>, <ProcName>, <ArgsType>, <ResType>);` macro invocation
- Add the procedure enum variant to `NFSProc3`
- Define the request struct with `XdrEncode` impl if needed

## 3. Wire into `src/nfs3/mount.rs` (Mount3 adapter)

Add delegation methods in the `impl crate::Mount for Mount3` block:
```rust
async fn <op>(&self, fh: Bytes, ...) -> Result<...> {
    self.m.<op>(fh, ...).await
}
async fn <op>_path(&self, path: &str, ...) -> Result<...> {
    self.m.<op>_path(path, ...).await
}
```

## 4. Add to `src/mount.rs` (Mount trait)

- Add async trait methods: `<op>()` and `<op>_path()`
- Add `sync_<op>()` and `sync_<op>_path()` default methods using `block_on_compat`
- Add doc examples

## 5. Wire into WASI bridge (if needed)

Add WIT method in `src/component.rs` following the existing pattern:
```rust
fn <op>(&self, ...) -> Result<..., WitError> {
    let mount_guard = get_mount(self.id)?;
    let mount = read_mount(&mount_guard)?;
    mount.sync_<op>(...).map_err(Into::into)
}
```

## 6. Verify

```bash
cargo build && cargo test
```
