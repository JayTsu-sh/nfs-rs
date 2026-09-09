# Contributing

Use this repository's [issues](https://github.com/JayTsu-sh/nfs-rs/issues)
and pull requests for changes to this fork. Describe the observable problem,
expected behavior, and validation before submitting a change. For contributions
to the original NetApp repository, consult its
[upstream contribution instructions](https://github.com/netapplabs/nfs-rs/blob/main/CONTRIBUTING.md),
including its contributor agreement process.

## Rust validation

Use the version pinned in `rust-toolchain.toml`. For behavior changes, add a
regression test that fails on the previous implementation. Protocol changes
must cite the relevant RFC and exercise request/response encoding where possible.

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets --no-fail-fast
cargo test --doc
```

CI also checks concrete reliability mappings against successful test execution,
using `scripts/check-reliability-test-results.py`. Update
`tests/nfs41-reliability-coverage.json` when renaming mapped tests; preserve
explicit partial/unmapped and unavailable-lab evidence.

## Python validation

Use a dedicated virtual environment and build the wheel from the current
checkout. An older installed distribution can have a different version and
native implementation even when Python source imports succeed.

```sh
python3 -m venv /tmp/nfs-rs-test-venv
. /tmp/nfs-rs-test-venv/bin/activate
python -m pip install 'maturin>=1,<2' pytest mypy
maturin build --locked --features python-test-support --out /tmp/nfs-rs-test-wheels
python -m pip install --force-reinstall /tmp/nfs-rs-test-wheels/nfs_rs-*.whl
NFS_RS_TEST_INSTALLED=1 python -m pytest -q python/tests
python -m mypy.stubtest nfs_rs --ignore-missing-stub --allowlist python/stubtest-allowlist.txt
```

Use an empty wheel output directory so the glob cannot select stale wheels.
Without `NFS_RS_TEST_INSTALLED=1`, installed-native tests are skipped; that run
alone does not validate the Rust adapter. Test-support wheels are for testing,
not release distribution.

## Real servers

The local TCP fixtures run without an NFS server. Real-server coverage and
fault experiments use the dedicated exports and run-scoped setup described in
[tests/lab/README.md](tests/lab/README.md). Report separately which deterministic
tests passed and which physical-lab scenarios were actually run. Do not claim
capability-dependent scenarios passed because they were skipped or unsupported.
