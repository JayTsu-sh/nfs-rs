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
fn report_accepts_every_fully_captured_environment() {
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
    assert_eq!(status.code(), Some(0));
    let markdown = fs::read_to_string(output_dir.join("performance-baselines.md"))
        .expect("Markdown report must be generated for accepted baselines");
    assert!(markdown.contains("Overall status: `complete`"));
    assert!(markdown.contains(
        "| dxn-v40 | `10.131.7.201:/jay_nfs` | 4.0 | `accepted` | 45 | 35.158 | 99.908 |"
    ));
    assert!(
        markdown
            .contains("| netapp-pnfs-ds | `10.128.56.161:/nfsrs_pnfs_b` | 4.1 | `accepted` | 45 |")
    );
    assert!(markdown.contains("## Baseline analysis summary"));
    assert!(markdown.contains("### Linux protocol comparison"));
    assert!(markdown.contains("### Highest baseline p95 latency observations"));
    let html = fs::read_to_string(output_dir.join("performance-baselines.html"))
        .expect("HTML report must be generated for accepted baselines");
    assert!(html.contains("<!doctype html>"));
    assert!(html.contains("Overall status:"));
    assert!(html.contains("35.158"));
    assert!(html.contains("pass_with_defaults: case_insensitive"));
    assert!(html.contains("Baseline analysis summary"));
    assert!(html.contains("Write-throughput ranking"));
    assert!(html.contains("Read-throughput ranking"));
    assert!(html.contains("Linux protocol comparison"));
    assert!(html.contains("Per-interface latency"));
    let json: Value = serde_json::from_slice(
        &fs::read(output_dir.join("performance-baselines.json"))
            .expect("JSON report must be generated for accepted baselines"),
    )
    .expect("performance baseline report must be JSON");
    assert_eq!(
        json["analysis"]["write_throughput_ranking"][0]["environment"],
        "netapp-pnfs-ds"
    );
    assert_eq!(
        json["analysis"]["protocol_comparisons"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        json["analysis"]["tail_latency_hotspots"]
            .as_array()
            .unwrap()
            .len(),
        10
    );
}

#[test]
fn report_fails_closed_and_still_renders_when_a_baseline_is_incomplete() {
    let fixture_dir = std::env::temp_dir().join(format!(
        "nfsrs-incomplete-performance-report-{}",
        std::process::id()
    ));
    let output_dir = fixture_dir.join("report");
    fs::create_dir_all(&fixture_dir).expect("temporary fixture directory must be created");
    let baseline_path = fixture_dir.join("partial.json");
    fs::write(
        &baseline_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "environment": "partial",
            "endpoint": "127.0.0.1:/partial",
            "protocol": "4.0",
            "status": "bootstrap_required",
            "capture_runs": 0,
            "capture_windows": 0,
            "benchmarks": {"storage_path": {}}
        }))
        .expect("partial baseline fixture must serialize"),
    )
    .expect("partial baseline fixture must be written");
    let manifest_path = fixture_dir.join("manifest.json");
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "minimum_capture_runs": 45,
            "minimum_capture_windows": 9,
            "environments": [{
                "id": "partial",
                "endpoint": "127.0.0.1:/partial",
                "protocol": "4.0",
                "baseline": baseline_path
            }]
        }))
        .expect("partial manifest fixture must serialize"),
    )
    .expect("partial manifest fixture must be written");

    let status = Command::new("python3")
        .arg(workspace_path(
            "tests/benchmarks/generate-baseline-report.py",
        ))
        .arg("--manifest")
        .arg(&manifest_path)
        .arg("--output-dir")
        .arg(&output_dir)
        .current_dir(workspace_path("."))
        .status()
        .expect("report generator should start");
    assert_eq!(status.code(), Some(2));
    for extension in ["json", "md", "html"] {
        let report =
            fs::read_to_string(output_dir.join(format!("performance-baselines.{extension}")))
                .expect("fail-closed report must still be rendered");
        assert!(report.contains("baseline_missing"));
    }
}

#[test]
fn gate_floors_metadata_latency_without_masking_data_path_regressions() {
    let fixture_dir =
        std::env::temp_dir().join(format!("nfsrs-window-p95-gate-{}", std::process::id()));
    fs::create_dir_all(&fixture_dir).expect("temporary gate directory must be created");
    let baseline_path = fixture_dir.join("windowed.json");
    fs::write(
        &baseline_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "status": "accepted",
            "capture_runs": 45,
            "capture_windows": 9,
            "capabilities": {"pathconf": "pass"},
            "thresholds": {
                "throughput_regression_percent": 15,
                "p95_latency_regression_percent": 30,
                "metadata_p95_absolute_floor_ms": 10
            },
            "benchmarks": {"storage_path": {
                "write_mib_s": {"median": 10.0},
                "read_mib_s": {"median": 10.0},
                "create_ms": {"p95": 10.0, "window_p95": {"p95": 50.0}},
                "null_ms": {"p95": 0.5, "window_p95": {"p95": 0.5}},
                "write_ms": {"p95": 0.5, "window_p95": {"p95": 0.5}}
            }}
        }))
        .expect("windowed baseline must serialize"),
    )
    .expect("windowed baseline must be written");
    let manifest_path = fixture_dir.join("manifest.json");
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "minimum_capture_runs": 45,
            "minimum_capture_windows": 9,
            "environments": [{"id": "windowed", "baseline": baseline_path}]
        }))
        .expect("gate manifest must serialize"),
    )
    .expect("gate manifest must be written");
    for run in 1..=4 {
        fs::write(
            fixture_dir.join(format!("windowed-run-{run}.json")),
            serde_json::to_vec_pretty(&serde_json::json!({
                "status": "pass",
                "lifs": [{"samples": [{
                    "pathconf_status": "pass",
                    "write_mib_s": 10.0,
                    "read_mib_s": 10.0,
                    "create_ms": 50.0,
                    "null_ms": 9.0,
                    "write_ms": 9.0
                }]}]
            }))
            .expect("gate run must serialize"),
        )
        .expect("gate run must be written");
    }
    let status = Command::new("python3")
        .arg(workspace_path(
            "tests/benchmarks/check-performance-baselines.py",
        ))
        .arg("--manifest")
        .arg(&manifest_path)
        .arg("--results-dir")
        .arg(&fixture_dir)
        .arg("--output")
        .arg(fixture_dir.join("gate.json"))
        .current_dir(workspace_path("."))
        .status()
        .expect("performance gate should start");
    assert_eq!(status.code(), Some(2));
    let gate: Value = serde_json::from_slice(
        &fs::read(fixture_dir.join("gate.json")).expect("gate report must be generated"),
    )
    .expect("gate report must be JSON");
    let violations = gate["environments"][0]["violations"]
        .as_array()
        .expect("violations must be an array");
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0]["metric"], "write_ms");
}

#[test]
fn gate_retests_only_a_numeric_failure_and_accepts_soft_jitter_as_a_warning() {
    let fixture_dir = std::env::temp_dir().join(format!(
        "nfsrs-soft-performance-gate-{}",
        std::process::id()
    ));
    let report_dir = fixture_dir.join("report");
    fs::create_dir_all(&fixture_dir).expect("temporary gate directory must be created");
    let baseline_path = fixture_dir.join("soft.json");
    fs::write(
        &baseline_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "status": "accepted",
            "capture_runs": 45,
            "capture_windows": 9,
            "endpoint": "127.0.0.1:/soft",
            "protocol": "4.1",
            "capabilities": {"pathconf": "pass"},
            "thresholds": {
                "throughput_regression_percent": 15,
                "p95_latency_regression_percent": 30,
                "metadata_p95_absolute_floor_ms": 10
            },
            "benchmarks": {"storage_path": {
                "write_mib_s": {"median": 10.0},
                "read_mib_s": {"median": 10.0},
                "write_ms": {"p95": 1.0, "window_p95": {"p95": 1.0}}
            }}
        }))
        .expect("baseline must serialize"),
    )
    .expect("baseline must be written");
    let manifest_path = fixture_dir.join("manifest.json");
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "minimum_capture_runs": 45,
            "minimum_capture_windows": 9,
            "soft_threshold_policy": {
                "throughput_hard_limit_factor": 0.9,
                "latency_hard_limit_factor": 1.1
            },
            "environments": [{
                "id": "soft",
                "endpoint": "127.0.0.1:/soft",
                "protocol": "4.1",
                "baseline": baseline_path
            }]
        }))
        .expect("manifest must serialize"),
    )
    .expect("manifest must be written");
    for run in 1..=4 {
        fs::write(
            fixture_dir.join(format!("soft-run-{run}.json")),
            serde_json::to_vec_pretty(&serde_json::json!({
                "status": "pass",
                "lifs": [{"summary": {
                    "write_median_mib_s": 5.0,
                    "read_median_mib_s": 5.0
                }, "samples": [{
                    "pathconf_status": "pass",
                    "write_mib_s": 5.0,
                    "read_mib_s": 5.0,
                    "write_ms": 2.0
                }]}]
            }))
            .expect("gate run must serialize"),
        )
        .expect("gate run must be written");
    }
    let gate_path = fixture_dir.join("gate.json");
    let gate_status = Command::new("python3")
        .arg(workspace_path(
            "tests/benchmarks/check-performance-baselines.py",
        ))
        .arg("--manifest")
        .arg(&manifest_path)
        .arg("--results-dir")
        .arg(&fixture_dir)
        .arg("--output")
        .arg(&gate_path)
        .current_dir(workspace_path("."))
        .status()
        .expect("performance gate should start");
    assert_eq!(gate_status.code(), Some(2));

    let supplemental_dirs: Vec<_> = (1..=3)
        .map(|round| fixture_dir.join(format!("supplemental-{round}")))
        .collect();
    for (round_index, supplemental_dir) in supplemental_dirs.iter().enumerate() {
        fs::create_dir_all(supplemental_dir).expect("supplemental directory must be created");
        let throughput = if round_index == 1 { 8.0 } else { 5.0 };
        for run in 1..=4 {
            fs::write(
                supplemental_dir.join(format!("soft-run-{run}.json")),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "status": "pass",
                    "lifs": [{"summary": {
                        "write_median_mib_s": 8.0,
                        "read_median_mib_s": 8.0
                    }, "samples": [{
                        "pathconf_status": "pass",
                        "write_mib_s": throughput,
                        "read_mib_s": throughput,
                        "write_ms": 1.4
                    }]}]
                }))
                .expect("supplemental run must serialize"),
            )
            .expect("supplemental run must be written");
        }
    }
    let mut gate_command = Command::new("python3");
    gate_command
        .arg(workspace_path(
            "tests/benchmarks/check-performance-baselines.py",
        ))
        .arg("--manifest")
        .arg(&manifest_path)
        .arg("--results-dir")
        .arg(&fixture_dir);
    for supplemental_dir in &supplemental_dirs {
        gate_command
            .arg("--supplemental-results-dir")
            .arg(supplemental_dir);
    }
    let gate_status = gate_command
        .arg("--output")
        .arg(&gate_path)
        .current_dir(workspace_path("."))
        .status()
        .expect("supplemental performance gate should start");
    assert_eq!(gate_status.code(), Some(0));
    let gate: Value = serde_json::from_slice(&fs::read(&gate_path).expect("gate JSON"))
        .expect("gate report must be JSON");
    assert_eq!(gate["status"], "pass_with_warnings");
    assert_eq!(gate["environments"][0]["status"], "warning");
    assert_eq!(gate["environments"][0]["initial_status"], "fail");
    assert_eq!(
        gate["environments"][0]["supplemental_tests"]
            .as_array()
            .expect("three supplemental rounds must be recorded")
            .len(),
        3
    );
    assert_eq!(gate["environments"][0]["warnings"][0]["hard_limit"], 8.5);
    assert_eq!(gate["environments"][0]["warnings"][0]["soft_limit"], 7.65);

    let mut report_command = Command::new("python3");
    report_command
        .arg(workspace_path(
            "tests/benchmarks/generate-baseline-report.py",
        ))
        .arg("--manifest")
        .arg(&manifest_path)
        .arg("--results-dir")
        .arg(&fixture_dir);
    for supplemental_dir in &supplemental_dirs {
        report_command
            .arg("--supplemental-results-dir")
            .arg(supplemental_dir);
    }
    let report_status = report_command
        .arg("--gate-result")
        .arg(&gate_path)
        .arg("--output-dir")
        .arg(&report_dir)
        .current_dir(workspace_path("."))
        .status()
        .expect("performance report should start");
    assert_eq!(report_status.code(), Some(0));
    for extension in ["json", "md", "html"] {
        let report =
            fs::read_to_string(report_dir.join(format!("performance-baselines.{extension}")))
                .expect("warning report must be generated");
        assert!(report.contains("warning"));
        assert!(report.contains("soft_limit"));
        assert!(report.to_lowercase().contains("supplemental"));
    }

    for run in 1..=4 {
        fs::write(
            supplemental_dirs[1].join(format!("soft-run-{run}.json")),
            serde_json::to_vec_pretty(&serde_json::json!({
                "status": "pass",
                "lifs": [{"samples": [{
                    "pathconf_status": "pass",
                    "write_mib_s": 5.0,
                    "read_mib_s": 5.0,
                    "write_ms": 2.0
                }]}]
            }))
            .expect("failing supplemental run must serialize"),
        )
        .expect("failing supplemental run must be written");
    }
    let mut all_fail_command = Command::new("python3");
    all_fail_command
        .arg(workspace_path(
            "tests/benchmarks/check-performance-baselines.py",
        ))
        .arg("--manifest")
        .arg(&manifest_path)
        .arg("--results-dir")
        .arg(&fixture_dir);
    for supplemental_dir in &supplemental_dirs {
        all_fail_command
            .arg("--supplemental-results-dir")
            .arg(supplemental_dir);
    }
    let all_fail_status = all_fail_command
        .arg("--output")
        .arg(&gate_path)
        .current_dir(workspace_path("."))
        .status()
        .expect("all-failing supplemental performance gate should start");
    assert_eq!(all_fail_status.code(), Some(2));
}

#[test]
fn scheduled_capture_and_candidate_release_gate_use_the_global_performance_lock() {
    let capture = fs::read_to_string(workspace_path(
        ".github/workflows/performance-baselines.yml",
    ))
    .expect("performance capture workflow must exist");
    let runner = fs::read_to_string(workspace_path(
        "tests/benchmarks/run-storage-benchmark-suite.sh",
    ))
    .expect("benchmark suite runner must exist");
    assert!(capture.contains("0 2,10,18 * * *"));
    assert!(capture.contains("NFS_RS_BENCHMARK_CAPTURE_RUNS: 5"));
    assert!(capture.contains("run-storage-benchmark-suite.sh capture"));
    assert!(
        fs::read_to_string(workspace_path(".github/workflows/release-validation.yml"))
            .expect("release workflow must exist")
            .contains("run-storage-benchmark-suite.sh gate")
    );
    assert!(runner.contains("check-performance-baselines.py"));
    assert!(runner.contains("gate-initial.json"));
    assert!(runner.contains("select(.supplemental_eligible)"));
    assert!(runner.contains("--supplemental-results-dir"));
    assert!(runner.contains("for round in 1 2 3"));
    assert!(runner.contains("run_environment \"$environment\" \"$template\""));
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
    assert!(builder.contains("window_p95"));
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
