use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

fn workspace_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn every_real_storage_protocol_has_an_independent_baseline() {
    let manifest: Value = serde_json::from_slice(
        &fs::read(workspace_path("tests/benchmarks/baselines/manifest.json"))
            .expect("performance baseline manifest must exist"),
    )
    .expect("performance baseline manifest must be JSON");
    let entries = manifest["environments"]
        .as_array()
        .expect("environments must be an array");
    let expected = HashSet::from([
        "linux-source-v3",
        "linux-source-v40",
        "linux-source-v41",
        "linux-destination-v3",
        "linux-destination-v40",
        "linux-destination-v41",
        "dxn-v40",
        "fas2750-v40-lif-a",
        "fas2750-v40-lif-b",
        "netapp-pnfs-mds",
        "netapp-pnfs-ds",
    ]);
    let observed = entries
        .iter()
        .map(|entry| {
            entry["id"]
                .as_str()
                .expect("environment ID must be a string")
        })
        .collect::<HashSet<_>>();
    assert_eq!(observed, expected);

    let mut baselines = HashSet::new();
    let mut identities = HashSet::new();
    for entry in entries {
        let id = entry["id"]
            .as_str()
            .expect("environment ID must be a string");
        let protocol = entry["protocol"]
            .as_str()
            .expect("protocol must be a string");
        let endpoint = entry["endpoint"]
            .as_str()
            .expect("endpoint must be a string");
        let baseline = entry["baseline"]
            .as_str()
            .expect("baseline path must be a string");
        assert!(
            identities.insert((endpoint, protocol)),
            "duplicate endpoint/protocol for {id}"
        );
        assert!(baselines.insert(baseline), "shared baseline file for {id}");
        assert!(
            workspace_path(baseline).is_file(),
            "missing baseline file for {id}"
        );
    }
}

#[test]
fn report_marks_uncaptured_environments_and_fails_closed() {
    let output_dir =
        std::env::temp_dir().join(format!("nfsrs-performance-report-{}", std::process::id()));
    fs::create_dir_all(&output_dir).expect("temporary report directory must be created");
    let status = Command::new("python3")
        .arg(workspace_path(
            "tests/benchmarks/generate-baseline-report.py",
        ))
        .args([
            "--manifest",
            "tests/benchmarks/baselines/manifest.json",
            "--output-dir",
        ])
        .arg(&output_dir)
        .current_dir(workspace_path("."))
        .status()
        .expect("report generator should start");
    assert_eq!(status.code(), Some(2));
    let markdown = fs::read_to_string(output_dir.join("performance-baselines.md"))
        .expect("Markdown report must be generated even when incomplete");
    assert!(markdown.contains("baseline_missing"));
    assert!(markdown.contains("dxn-v40"));
    assert!(markdown.contains("netapp-pnfs-ds"));
}

#[test]
fn scheduled_capture_and_release_gate_use_the_global_performance_lock() {
    let capture = fs::read_to_string(workspace_path(
        ".github/workflows/performance-baselines.yml",
    ))
    .expect("performance capture workflow must exist");
    let release = fs::read_to_string(workspace_path(".github/workflows/release-validation.yml"))
        .expect("release workflow must exist");
    let runner = fs::read_to_string(workspace_path(
        "tests/benchmarks/run-storage-benchmark-suite.sh",
    ))
    .expect("benchmark suite runner must exist");
    assert!(capture.contains("0 2,10,18 * * *"));
    assert!(capture.contains("NFS_RS_BENCHMARK_CAPTURE_RUNS: 5"));
    assert!(capture.contains("run-storage-benchmark-suite.sh capture"));
    assert!(release.contains("run-storage-benchmark-suite.sh gate"));
    assert!(runner.contains("/tmp/terrasync-lab-tests.lock"));
    assert!(runner.contains("/tmp/terrasync-lab-performance.lock"));
    assert!(runner.contains("--window-id"));
    assert!(runner.contains("flock --wait 1800"));
    assert!(!runner.contains("rm -rf"));
}

#[test]
fn baseline_builder_covers_every_benchmarked_interface() {
    let builder = fs::read_to_string(workspace_path(
        "tests/benchmarks/build-performance-baselines.py",
    ))
    .expect("baseline builder must exist");
    for operation in [
        "mount_ms",
        "umount_ms",
        "null_ms",
        "fsinfo_ms",
        "fsstat_ms",
        "mkdir_ms",
        "create_ms",
        "lookup_ms",
        "getattr_ms",
        "access_ms",
        "pathconf_ms",
        "write_ms",
        "commit_ms",
        "close_ms",
        "open_ms",
        "read_ms",
        "rename_ms",
        "link_ms",
        "symlink_ms",
        "readlink_ms",
        "readdir_ms",
        "remove_ms",
        "rmdir_ms",
        "write_mib_s",
        "read_mib_s",
    ] {
        assert!(
            builder.contains(&format!("\"{operation}\"")),
            "missing {operation}"
        );
    }
}
