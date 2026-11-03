use std::process::Command;

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_nfs-memory-benchmark"));
    command.args([
        "--url",
        "nfs://127.0.0.1/export?version=4.1",
        "--run-id",
        "release-memory-fixture",
        "--case",
        "payload",
        "--validate-only",
    ]);
    command
}

#[test]
fn validates_memory_workloads_without_contacting_storage() {
    for mode in ["write", "read", "write-read"] {
        let output = command().args(["--mode", mode]).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["status"], "configuration_valid");
        assert_eq!(report["config"]["mode"], mode);
        assert_eq!(report["config"]["protocol"], "4.1");
        assert_eq!(report["config"]["payload_bytes"], 1024 * 1024 * 1024);
    }
}

#[test]
fn rejects_unsafe_paths_and_invalid_measurements_before_network_io() {
    for args in [
        vec!["--case", "../other"],
        vec!["--run-id", "release-../other"],
        vec!["--case", "."],
        vec!["--qd", "0"],
        vec!["--qd", "65"],
        vec!["--rounds", "0"],
        vec!["--mib", "0"],
        vec!["--url", "nfs://127.0.0.1/e?version=4.1,3"],
        vec!["--url", "nfs://127.0.0.1/e?version=3&version=4.1"],
        vec!["--url", "nfs://user:secret@127.0.0.1/e?version=3"],
        vec!["--wait-for-stdin"],
    ] {
        let output = command().args(&args).output().unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn memory_report_validation_and_concurrent_reader_barrier_are_tested_without_storage() {
    let output = Command::new("python3")
        .args([
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests/benchmarks",
            "-p",
            "test_memory_benchmark.py",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("memory report tests should start");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn memory_runner_stops_workers_before_cleanup_on_signals() {
    let output = Command::new("python3")
        .args([
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests/lab",
            "-p",
            "test_memory_runner.py",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("memory runner tests should start");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
