use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

const MANIFEST: &str = "tests/nfs41-reliability-coverage.json";

fn workspace_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn load_manifest() -> Value {
    let bytes = fs::read(workspace_path(MANIFEST)).expect("coverage manifest must be readable");
    serde_json::from_slice(&bytes).expect("coverage manifest must be valid JSON")
}

#[test]
fn coverage_manifest_is_complete() {
    let manifest = load_manifest();
    assert_eq!(manifest["schema_version"], 1);
    let spec = manifest["spec"]
        .as_str()
        .expect("spec path must be a string");
    assert!(workspace_path(spec).is_file(), "spec path does not exist");
    assert_eq!(manifest["tracking_issue"], 10);

    let requirements = manifest["requirements"]
        .as_array()
        .expect("requirements must be an array");
    let requirement_ids = requirements
        .iter()
        .map(|value| value.as_str().expect("requirement ID must be a string"))
        .collect::<HashSet<_>>();
    assert_eq!(requirement_ids.len(), requirements.len());
    for number in 1..=18 {
        assert!(
            requirement_ids.contains(format!("R{number}").as_str()),
            "missing R{number}"
        );
    }

    let tests = manifest["tests"]
        .as_array()
        .expect("tests must be an array");
    let mut test_ids = HashSet::new();
    for entry in tests {
        let id = entry["id"].as_str().expect("test ID must be a string");
        assert!(test_ids.insert(id), "duplicate test ID {id}");
        let ci = entry["ci"].as_str().expect("CI mapping must be a string");
        let nightly = entry["nightly"]
            .as_str()
            .expect("nightly mapping must be a string");
        assert!(!ci.trim().is_empty(), "{id} has empty CI mapping");
        assert!(!nightly.trim().is_empty(), "{id} has empty nightly mapping");
        assert!(
            !nightly.contains("skip"),
            "{id} silently skips required nightly coverage"
        );
        assert_eq!(entry["required"], true, "{id} must remain required");
    }
    for number in 1..=25 {
        assert!(
            test_ids.contains(format!("T{number:02}").as_str()),
            "missing T{number:02}"
        );
    }
    assert_eq!(test_ids.len(), 25);
}

#[test]
fn production_code_has_no_unwrap_or_expect() {
    let src = workspace_path("src");
    let mut pending = vec![src];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path).expect("source directory must be readable") {
            let entry = entry.expect("source entry must be readable");
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.extension().and_then(|value| value.to_str()) != Some("rs") {
                continue;
            }
            let source = fs::read_to_string(&path).expect("Rust source must be UTF-8");
            let production = source
                .split("#[cfg(test)]")
                .next()
                .unwrap_or_default()
                .lines()
                .filter(|line| {
                    let trimmed = line.trim_start();
                    !trimmed.starts_with("///") && !trimmed.starts_with("//!")
                })
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                !production.contains(".unwrap()") && !production.contains(".expect("),
                "production unwrap/expect found in {}",
                path.display()
            );
        }
    }
}

#[test]
fn lab_capability_probe_is_read_only() {
    let path = workspace_path("tests/lab/capability-report.sh");
    let source = fs::read_to_string(&path).expect("capability probe must be readable");
    for forbidden in [
        "systemctl restart",
        "systemctl stop",
        "systemctl kill",
        "service restart",
        "iptables -",
        "nft add",
        "nft delete",
        "tc qdisc",
        "killall",
        "pkill",
    ] {
        assert!(
            !source.contains(forbidden),
            "read-only capability probe contains forbidden mutation: {forbidden}"
        );
    }
    assert!(source.contains("sudo -n -l"));
    assert!(source.contains("terrasync-lab-nfs-status"));
}

#[test]
fn kernel_nfsv40_e2e_is_safe_and_wired_into_lab_gates() {
    let runner = fs::read_to_string(workspace_path("tests/lab/run-kernel-v40-e2e.sh"))
        .expect("kernel NFSv4.0 runner must be readable");
    let helper = fs::read_to_string(workspace_path("tests/lab/admin/nfsrs-lab-kernel-v40-mount"))
        .expect("kernel NFSv4.0 privileged helper must be readable");
    let nightly = fs::read_to_string(workspace_path(".github/workflows/nightly.yml"))
        .expect("nightly workflow must be readable");
    let release = fs::read_to_string(workspace_path(".github/workflows/release-validation.yml"))
        .expect("release workflow must be readable");

    for required in [
        "vers=4.0",
        "findmnt",
        "trap cleanup EXIT INT TERM",
        "NFS_RS_LAB_KERNEL_V40_SERVER_A",
        "NFS_RS_LAB_KERNEL_V40_SERVER_B",
        "LAB_NFS41_EXPORT",
        "sha256sum",
        "sha256sum --check",
        "local_oracle",
        "concurrent-manifest.sha256",
        "peer_mount",
        "flock",
    ] {
        assert!(
            runner.contains(required),
            "kernel NFSv4.0 runner lacks {required}"
        );
    }
    assert!(!runner.contains("rm -rf"));
    assert!(!runner.contains("10.128.61.200"));
    assert!(!runner.contains("10.128.61.201"));
    for required in [
        "mount-source:10.10.1.12:/srv/nfs/v4",
        "mount-dest:10.10.1.13:/srv/nfs/v4",
        "vers=4.0",
        "validate_test_name",
    ] {
        assert!(
            helper.contains(required),
            "kernel mount helper lacks {required}"
        );
    }
    assert!(!helper.contains("rm -rf"));
    assert!(nightly.contains("tests/lab/run-kernel-v40-e2e.sh \"$RUN_ID\""));
    assert!(release.contains("tests/lab/run-kernel-v40-e2e.sh \"$RUN_ID\""));
}

#[test]
fn dxn_nfsv40_e2e_is_exact_and_wired_into_nightly() {
    let runner = fs::read_to_string(workspace_path("tests/lab/run-dxn-v40-e2e.sh"))
        .expect("DXN NFSv4.0 runner must be readable");
    let common = fs::read_to_string(workspace_path("tests/lab/common.sh"))
        .expect("lab defaults must be readable");
    let capability = fs::read_to_string(workspace_path("tests/lab/capability-report.sh"))
        .expect("capability report must be readable");
    let nightly = fs::read_to_string(workspace_path(".github/workflows/nightly.yml"))
        .expect("nightly workflow must be readable");

    for required in [
        "validate_run_id",
        "validate_ipv4",
        "validate_export_path",
        "version=4.0",
        "/dev/tcp/$1/2049",
        "nfs_v40_server_max_io_attributes",
        "nfs_v40_single_export_end_to_end",
        "nfs_v40_same_open_state_supports_concurrent_io",
        "NFS_RS_LAB_V40_RUN_ID",
    ] {
        assert!(runner.contains(required), "DXN runner lacks {required}");
    }
    assert!(common.contains("LAB_DXN_V40_DATA=\"${LAB_DXN_V40_DATA:-10.131.7.201}\""));
    assert!(common.contains("LAB_DXN_V40_EXPORT=\"${LAB_DXN_V40_EXPORT:-/jay_nfs}\""));
    assert!(capability.contains("dxn-nfsv40.txt"));
    assert!(capability.contains("BLOCKED_CAPABILITY(dxn-nfsv40)"));
    assert!(nightly.contains("DXN NFSv4.0 E2E"));
    assert!(nightly.contains("tests/lab/run-dxn-v40-e2e.sh \"$RUN_ID\""));
}

#[test]
fn nightly_uses_only_a_verified_preprovisioned_toolchain() {
    let workflow = fs::read_to_string(workspace_path(".github/workflows/nightly.yml"))
        .expect("nightly workflow must be readable");
    assert!(
        workflow
            .contains("/home/github-runner/.rustup/toolchains/1.95.0-x86_64-unknown-linux-gnu/bin")
    );
    assert!(workflow.contains("complete pre-provisioned Rust 1.95.0 toolchain not found"));
    assert!(workflow.contains("cache-bin: false"));
    assert!(workflow.contains("rustc --version | grep -q '^rustc 1\\.95\\.0 '"));
    assert!(!workflow.contains("rustup default"));
    assert!(!workflow.contains("dtolnay/rust-toolchain"));
    assert!(!workflow.contains("rustup toolchain install"));
}

#[test]
fn release_validation_uses_only_a_verified_preprovisioned_toolchain() {
    let workflow = include_str!("../.github/workflows/release-validation.yml");

    assert!(workflow.contains("Discover persisted Rust toolchain"));
    assert!(
        workflow
            .contains("/home/github-runner/.rustup/toolchains/1.95.0-x86_64-unknown-linux-gnu/bin")
    );
    assert!(workflow.contains("complete pre-provisioned Rust 1.95.0 toolchain not found"));
    assert!(workflow.contains("rustc --version | grep -q '^rustc 1\\.95\\.0 '"));
    assert!(workflow.contains("cache-bin: false"));
    assert_eq!(
        workflow
            .matches("Discover pre-provisioned Zig 0.13.0")
            .count(),
        1
    );
    assert_eq!(
        workflow
            .matches("test \"$(zig version)\" = \"0.13.0\"")
            .count(),
        1
    );
    assert!(!workflow.contains("goto-bus-stop/setup-zig"));
    assert!(!workflow.contains("aarch64-python-real-protocol"));
    assert!(!workflow.contains("runs-on: [self-hosted, linux, ARM64"));
    assert!(!workflow.contains("dtolnay/rust-toolchain"));
    assert!(!workflow.contains("rustup default"));
    assert!(!workflow.contains("rustup toolchain install"));
}

#[test]
fn nfsv40_experimental_release_contract_is_complete() {
    let cargo = fs::read_to_string(workspace_path("Cargo.toml")).expect("Cargo.toml readable");
    let readme = fs::read_to_string(workspace_path("README.md")).expect("README readable");
    let changelog = fs::read_to_string(workspace_path("CHANGELOG.md")).expect("changelog readable");
    let release = include_str!("../.github/workflows/release-validation.yml");
    let publisher = include_str!("../.github/workflows/release.yml");
    let nightly = include_str!("../.github/workflows/nightly.yml");
    let matrix = include_str!("lab/run-netapp-v40-release-matrix.sh");

    assert!(cargo.contains("version = \"0.5.3\""));
    for required in [
        "NFSv4.0 (experimental)",
        "AUTH_SYS",
        "RPCSEC_GSS",
        "version=4.0",
        "retain-delegations=true",
        "grace/reclaim",
    ] {
        assert!(readme.contains(required), "README lacks {required}");
    }
    assert!(changelog.contains("## [0.5.0]"));
    for command in [
        "cargo package --locked",
        "tests/lab/run-netapp-v40-performance.sh",
        "tests/lab/collect-nfsv40-release-evidence.sh",
    ] {
        assert!(
            release.contains(command) || nightly.contains(command) || matrix.contains(command),
            "release matrix lacks {command}"
        );
    }
    assert!(publisher.contains("publish-crate-artifact.py"));
    assert!(publisher.contains("actions/download-artifact@v5"));
    assert!(!publisher.contains("cargo publish"));
}

#[test]
fn nfsv40_release_evidence_is_typed_hashed_and_fail_closed() {
    let recorder = fs::read_to_string(workspace_path("tests/lab/record-nfsv40-evidence.sh"))
        .expect("NFSv4.0 evidence recorder readable");
    let collector = fs::read_to_string(workspace_path(
        "tests/lab/collect-nfsv40-release-evidence.sh",
    ))
    .expect("NFSv4.0 evidence collector readable");
    let matrix = fs::read_to_string(workspace_path("tests/lab/run-netapp-v40-release-matrix.sh"))
        .expect("NFSv4.0 release matrix readable");
    let release = include_str!("../.github/workflows/release-validation.yml");
    let nightly = include_str!("../.github/workflows/nightly.yml");

    for required in [
        "started_at_utc",
        "finished_at_utc",
        "outcome",
        "exit_code",
        "command",
        "sha256",
    ] {
        assert!(recorder.contains(required), "recorder lacks {required}");
    }
    for evidence in [
        "semantic",
        "callback-fault",
        "lease-fault",
        "performance",
        "cleanup",
        "grace-reclaim",
    ] {
        let invocation = format!("record {evidence} ");
        assert!(
            matrix.contains(&invocation),
            "release matrix does not record {evidence} evidence"
        );
        assert!(collector.contains(evidence), "collector lacks {evidence}");
    }
    assert!(release.contains("run-netapp-v40-release-matrix.sh"));
    assert!(nightly.contains("run-netapp-v40-release-matrix.sh"));
    assert!(collector.contains("missing required NFSv4.0 evidence"));
    assert!(collector.contains("stale NFSv4.0 performance report identity"));
    assert!(collector.contains("NFSv4.0 performance report topology mismatch"));
    assert!(collector.contains("SHA256SUMS"));
}

#[test]
fn nfsv40_performance_gate_covers_four_workload_quadrants() {
    let baseline: Value = serde_json::from_slice(
        &fs::read(workspace_path("tests/lab/nfsv40-performance-baseline.json"))
            .expect("NFSv4.0 performance baseline readable"),
    )
    .expect("NFSv4.0 performance baseline valid JSON");
    assert_eq!(baseline["schema_version"], 2);
    assert_eq!(baseline["thresholds"]["throughput_regression_percent"], 15);
    assert_eq!(baseline["thresholds"]["p95_latency_regression_percent"], 20);
    let names = baseline["workloads"]
        .as_array()
        .expect("workloads array")
        .iter()
        .map(|entry| entry["name"].as_str().expect("workload name"))
        .collect::<HashSet<_>>();
    assert_eq!(
        names,
        HashSet::from(["small-single", "small-multi", "large-single", "large-multi",])
    );
    let checker = fs::read_to_string(workspace_path("tests/lab/check-nfsv40-performance.py"))
        .expect("performance checker readable");
    for required in [
        "throughput_mib_s",
        "write_p95_latency_ms",
        "workload_p95_latency_ms",
        "peak_rss_kib",
        "liveness",
    ] {
        assert!(checker.contains(required), "checker lacks {required}");
    }
}

#[test]
fn nfsv40_release_matrix_records_performance_without_duplicate_gate() {
    let matrix = fs::read_to_string(workspace_path("tests/lab/run-netapp-v40-release-matrix.sh"))
        .expect("NFSv4.0 release matrix readable");
    let runner = fs::read_to_string(workspace_path("tests/lab/run-netapp-v40-performance.sh"))
        .expect("NFSv4.0 performance runner readable");

    assert!(matrix.contains("run-netapp-v40-performance.sh \"$run_id\" --observe-only"));
    assert!(runner.contains("nfs_v40_small_large_single_multi_performance"));
    assert!(runner.contains("if [[ \"$mode\" == \"gate\" ]]"));
    assert!(runner.contains("check-nfsv40-performance.py"));
}

#[test]
fn nfsv40_performance_gate_rejects_regressions() {
    let baseline_path = workspace_path("tests/lab/nfsv40-performance-baseline.json");
    let baseline: Value = serde_json::from_slice(&fs::read(&baseline_path).expect("baseline"))
        .expect("valid baseline");
    let temp = std::env::temp_dir().join(format!("nfsrs-perf-gate-{}", std::process::id()));
    fs::create_dir_all(&temp).expect("create temporary gate directory");
    let current_path = temp.join("current.json");
    let mut current = serde_json::json!({
        "liveness": "pass",
        "workloads": baseline["workloads"].clone(),
    });
    current["workloads"][0]["throughput_mib_s"] = serde_json::json!(0.0);
    current["workloads"][0]["workload_p95_latency_ms"] = serde_json::json!(f64::MAX);
    fs::write(
        &current_path,
        serde_json::to_vec(&current).expect("encode current report"),
    )
    .expect("write current report");
    let output = Command::new("python3")
        .arg(workspace_path("tests/lab/check-nfsv40-performance.py"))
        .arg(&baseline_path)
        .arg(&current_path)
        .output()
        .expect("run performance checker");
    assert!(
        !output.status.success(),
        "regressed performance was accepted"
    );
    let diagnostic = String::from_utf8(output.stderr).expect("UTF-8 performance diagnostic");
    for field in ["baseline=", "actual=", "limit=", "regression_percent="] {
        assert!(
            diagnostic.contains(field),
            "diagnostic lacks {field}: {diagnostic}"
        );
    }
    for metric in ["throughput_mib_s", "workload_p95_latency_ms"] {
        assert!(
            diagnostic.contains(metric),
            "diagnostic lacks {metric}: {diagnostic}"
        );
    }
    current["workloads"] = baseline["workloads"].clone();
    current["workloads"][0]["workload_p95_latency_ms"] = serde_json::json!(f64::MAX);
    fs::write(
        &current_path,
        serde_json::to_vec(&current).expect("encode workload latency regression"),
    )
    .expect("write workload latency regression");
    let status = Command::new("python3")
        .arg(workspace_path("tests/lab/check-nfsv40-performance.py"))
        .arg(&baseline_path)
        .arg(&current_path)
        .status()
        .expect("run workload latency checker");
    assert!(
        !status.success(),
        "regressed workload p95 latency was accepted"
    );
    fs::remove_file(current_path).expect("remove current report");
    fs::remove_dir(temp).expect("remove temporary gate directory");
}

#[test]
fn nfsv40_evidence_preserves_raw_performance_report_on_gate_failure() {
    let temp = std::env::temp_dir().join(format!("nfsrs-perf-evidence-{}", std::process::id()));
    let records = temp.join("records");
    let output = temp.join("output");
    let performance = temp.join("performance.json");
    fs::create_dir_all(&records).expect("create empty evidence records");
    fs::write(&performance, br#"{"liveness":"pass","workloads":[]}"#)
        .expect("write raw performance report");
    let status = Command::new("bash")
        .arg(workspace_path(
            "tests/lab/collect-nfsv40-release-evidence.sh",
        ))
        .arg("nightly-evidence-preservation")
        .arg(&output)
        .env("NFS_RS_LAB_V40_EVIDENCE_DIR", &records)
        .env("NFS_RS_LAB_V40_PERF_OUTPUT", &performance)
        .env("NFS_RS_LAB_V40_EVIDENCE_COMMIT", "test-commit")
        .status()
        .expect("run incomplete evidence collection");
    assert!(!status.success(), "incomplete evidence unexpectedly passed");
    assert_eq!(
        fs::read(output.join("performance-report.json")).expect("preserved performance report"),
        fs::read(&performance).expect("source performance report"),
    );
    fs::remove_dir_all(temp).expect("remove evidence test directory");
}

#[test]
fn nfs_fault_helper_is_allow_listed_and_run_scoped() {
    let source = fs::read_to_string(workspace_path("tests/lab/admin/terrasync-lab-nfs-fault"))
        .expect("NFS fault helper must be readable");
    assert!(source.contains("apply-session-fault)"));
    assert!(source.contains("apply-tcp-reset)"));
    assert!(source.contains("trigger-delegation-recall)"));
    assert!(source.contains("status)"));
    assert!(source.contains("restore)"));
    assert!(source.contains("^(nightly|release)-"));
    assert!(source.contains("flock --wait"));
    assert!(source.contains("nfs-server.service"));
    assert!(source.contains("10.10.1.11"));
    assert!(source.contains("ss -K sport = :2049"));
    for forbidden in [
        "eval ", "bash -c", "sh -c", "rm -rf", "iptables", "nft ", "reboot", "poweroff",
    ] {
        assert!(
            !source.contains(forbidden),
            "fault helper contains forbidden capability: {forbidden}"
        );
    }
}

#[test]
fn netapp_v40_lease_fault_is_destination_scoped_and_restored() {
    let helper = fs::read_to_string(workspace_path("tests/lab/admin/nfsrs-lab-v40-fault"))
        .expect("NFSv4.0 fault helper must be readable");
    let runner = fs::read_to_string(workspace_path(
        "tests/lab/run-netapp-v40-lease-fault-e2e.sh",
    ))
    .expect("NFSv4.0 lease fault runner must be readable");
    let workflow = fs::read_to_string(workspace_path(".github/workflows/nightly.yml"))
        .expect("nightly workflow must be readable");
    let matrix = fs::read_to_string(workspace_path("tests/lab/run-netapp-v40-release-matrix.sh"))
        .expect("NFSv4.0 release matrix must be readable");
    for required in [
        "10.131.9.11",
        "10.128.61.200",
        "10.128.61.201",
        "tcp dport 2049 drop",
        "^(nightly|release)-",
    ] {
        assert!(helper.contains(required), "fault helper lacks {required}");
    }
    assert!(helper.contains("output ip daddr \"$target_ip\" tcp dport 2049 drop"));
    assert!(helper.contains("input ip saddr \"$target_ip\" tcp flags"));
    assert!(runner.contains("trap cleanup EXIT INT TERM"));
    assert!(runner.contains("NFS_RS_LAB_V40_LIF_A"));
    assert!(runner.contains("NFS_RS_LAB_V40_LIF_B"));
    assert!(runner.contains("run_case \"$target_ip\" below"));
    assert!(runner.contains("run_case \"$target_ip\" above"));
    assert!(runner.contains("restore-any"));
    assert!(matrix.contains("run-netapp-v40-lease-fault-e2e.sh"));
    assert!(workflow.contains("Restore NetApp NFSv4.0 connectivity"));
    assert!(workflow.contains("verify-netapp-v40-cleanup.sh"));
}

#[test]
fn callback_fault_coverage_is_explicit_and_capability_honest() {
    let workflow = fs::read_to_string(workspace_path(".github/workflows/nightly.yml"))
        .expect("nightly workflow must be readable");
    let runner = fs::read_to_string(workspace_path("tests/lab/run-callback-replay-e2e.sh"))
        .expect("callback replay runner must be readable");
    let capabilities = fs::read_to_string(workspace_path("tests/lab/capability-report.sh"))
        .expect("capability report must be readable");

    assert!(workflow.contains("tests/lab/run-callback-replay-e2e.sh"));
    assert!(
        runner
            .contains("scripted_callback_reply_loss_replays_cached_body_and_executes_recall_once")
    );
    assert!(capabilities.contains("selective_callback_reply_loss=supported"));
    assert!(capabilities.contains("callback_retransmission=supported"));
    assert!(workflow.contains("tests/lab/run-real-callback-replay-e2e.sh"));
    let proxy = fs::read_to_string(workspace_path("tests/lab/rpc-callback-drop-proxy.py"))
        .expect("callback proxy must be readable");
    for evidence in [
        "callback-reply-dropped",
        "callback-retransmit-injected",
        "fore-call-held",
        "replay_forwarded",
    ] {
        assert!(proxy.contains(evidence), "proxy lacks {evidence} state");
    }
}

#[test]
fn netapp_pnfs_lab_contract_is_wired() {
    let common = fs::read_to_string(workspace_path("tests/lab/common.sh"))
        .expect("lab common config must be readable");
    let runner = fs::read_to_string(workspace_path("tests/lab/run-pnfs-e2e.sh"))
        .expect("pNFS runner must be readable");
    let compatibility = fs::read_to_string(workspace_path("tests/lab/run-netapp-v41-e2e.sh"))
        .expect("NetApp compatibility runner must be readable");
    let cleanup = fs::read_to_string(workspace_path("tests/lab/cleanup-pnfs-run.sh"))
        .expect("pNFS cleanup must be readable");
    let workflow = fs::read_to_string(workspace_path(".github/workflows/nightly.yml"))
        .expect("nightly workflow must be readable");
    let capability = fs::read_to_string(workspace_path("tests/lab/capability-report.sh"))
        .expect("capability report must be readable");
    let rust_test = fs::read_to_string(workspace_path("tests/lab_e2e.rs"))
        .expect("lab E2E test must be readable");
    let fault_runner = fs::read_to_string(workspace_path("tests/lab/run-pnfs-ds-reset-e2e.sh"))
        .expect("pNFS fault runner must be readable");
    let fault_helper = fs::read_to_string(workspace_path("tests/lab/admin/nfsrs-lab-pnfs-fault"))
        .expect("pNFS fault helper must be readable");
    let preflight_runner = fs::read_to_string(workspace_path(
        "tests/lab/run-pnfs-preflight-fallback-e2e.sh",
    ))
    .expect("pNFS preflight runner must be readable");
    let layoutcommit_runner = fs::read_to_string(workspace_path(
        "tests/lab/run-pnfs-layoutcommit-fault-e2e.sh",
    ))
    .expect("pNFS LAYOUTCOMMIT runner must be readable");
    let recall_runner =
        fs::read_to_string(workspace_path("tests/lab/run-pnfs-layout-recall-e2e.sh"))
            .expect("pNFS layout recall runner must be readable");
    let recall_proxy = fs::read_to_string(workspace_path(
        "tests/lab/rpc-layout-recall-inject-proxy.py",
    ))
    .expect("pNFS layout recall proxy must be readable");

    for value in [
        "LAB_PNFS_MDS_DATA",
        "LAB_PNFS_DS_DATA",
        "LAB_PNFS_PRIMARY_EXPORT",
        "LAB_PNFS_SECONDARY_EXPORT",
    ] {
        assert!(common.contains(value), "missing pNFS config key {value}");
    }
    assert!(runner.contains("validate_run_id"));
    assert!(runner.contains("validate_ipv4"));
    assert!(runner.contains("validate_export_path"));
    assert!(runner.contains("observed_connections > baseline_connections"));
    assert!(runner.contains("BLOCKED_CAPABILITY(netapp-pnfs-independent-ds)"));
    assert!(runner.contains("nfs_v41_pnfs_multifile_active_layout_refresh"));
    assert!(runner.contains("trap cleanup EXIT"));
    assert!(workflow.contains("NetApp NFSv4.1 pNFS WRITE E2E"));
    assert!(workflow.contains("NetApp NFSv4.1 compatibility E2E"));
    assert!(workflow.contains("tests/lab/run-netapp-v41-e2e.sh \"$RUN_ID\""));
    assert!(compatibility.contains("nfs_v3_and_v41_end_to_end"));
    assert!(compatibility.contains("version=4.1&noresvport=true"));
    assert!(workflow.contains("tests/lab/run-pnfs-e2e.sh \"$RUN_ID\""));
    assert!(workflow.contains("tests/lab/cleanup-pnfs-run.sh \"$RUN_ID\""));
    assert!(workflow.contains("Cleanup NetApp pNFS run"));
    assert!(cleanup.contains("validate_run_id"));
    assert!(cleanup.contains("nfs_v41_pnfs_cleanup_run"));
    assert!(capability.contains("BLOCKED_CAPABILITY(netapp-pnfs-data-lif)"));
    assert!(rust_test.contains("nfs_v41_pnfs_write_uses_independent_ds"));
    assert!(rust_test.contains("nfs_v41_pnfs_multifile_active_layout_refresh"));
    assert!(rust_test.contains("pnfs-active-refresh files=16"));
    assert!(rust_test.contains("pNFS full-payload checksum mismatch"));
    assert!(workflow.contains("NetApp pNFS DS reset uncertain-outcome E2E"));
    assert!(workflow.contains("tests/lab/run-pnfs-ds-reset-e2e.sh \"$RUN_ID\""));
    assert!(workflow.contains("Restore NetApp pNFS DS connectivity"));
    assert!(fault_runner.contains("trap cleanup EXIT"));
    assert!(fault_runner.contains("uncertain=1 mds-fallback=0 restored=1 checksum=ok"));
    assert!(fault_helper.contains("runner_ip=10.131.9.11"));
    assert!(fault_helper.contains("ds_ip=10.128.56.161"));
    assert!(fault_helper.contains("tcp dport 2049 drop"));
    assert!(!fault_helper.contains("eval "));
    assert!(rust_test.contains("nfs_v41_pnfs_ds_reset_returns_uncertain"));
    assert!(rust_test.contains("VerifyThenResume"));
    assert!(rust_test.contains("pNFS checkpoint recovery full-payload checksum mismatch"));
    assert!(workflow.contains("NetApp pNFS pre-WRITE DS failure fallback E2E"));
    assert!(workflow.contains("tests/lab/run-pnfs-preflight-fallback-e2e.sh \"$RUN_ID\""));
    assert!(preflight_runner.contains("trap cleanup EXIT"));
    assert!(preflight_runner.contains("ds_write_sent=0 mds_fallback=1"));
    assert!(preflight_runner.contains("maximum_connections > baseline_connections"));
    assert!(rust_test.contains("nfs_v41_pnfs_ds_unreachable_before_write_falls_back_to_mds"));
    assert!(rust_test.contains("pNFS preflight MDS fallback full-payload checksum mismatch"));
    assert!(workflow.contains("NetApp pNFS LAYOUTCOMMIT failure recovery E2E"));
    assert!(layoutcommit_runner.contains("trap cleanup EXIT"));
    assert!(layoutcommit_runner.contains("isolate-mds"));
    assert!(
        layoutcommit_runner.contains("dirty-retained=1 reopen-verify=1 restored=1 checksum=ok")
    );
    assert!(rust_test.contains("nfs_v41_pnfs_layoutcommit_failure_retains_dirty_range"));
    assert!(workflow.contains("NetApp pNFS layout recall during WRITE/CLOSE E2E"));
    assert!(recall_runner.contains("trap cleanup EXIT"));
    assert!(recall_runner.contains("rpc-layout-recall-inject-proxy.py"));
    assert!(recall_runner.contains("pnfs-layout-recall received="));
    assert!(recall_runner.contains("close-ordered=1 checksum=ok"));
    assert!(recall_proxy.contains("CB_LAYOUTRECALL"));
    assert!(recall_proxy.contains("layout-recall-reply-status="));
    assert!(rust_test.contains("nfs_v41_pnfs_layout_recall_during_write_and_close"));

    let pnfs_io = fs::read_to_string(workspace_path("src/nfs41/pnfs_io.rs"))
        .expect("pNFS I/O implementation must be readable");
    let layout = fs::read_to_string(workspace_path("src/nfs41/layout.rs"))
        .expect("pNFS layout implementation must be readable");
    for test in [
        "ds_batch_waits_for_success_when_failure_completes_first",
        "ds_batch_waits_for_failure_when_success_completes_first",
        "cancelling_ds_batch_drops_every_pending_write",
    ] {
        assert!(pnfs_io.contains(test), "missing partial T15 CI test {test}");
    }
    assert!(pnfs_io.contains("let completions = settle_ds_batch(futures).await"));
    assert!(pnfs_io.contains("ds_batch_diagnostic(&completions)"));
    assert!(pnfs_io.contains("attempted=true outcome=success"));
    assert!(pnfs_io.contains("attempted=true outcome=error"));
    assert!(pnfs_io.contains("RecoveryAction::VerifyThenResume"));
    assert!(layout.contains("PNFS_LAYOUT_REFRESH_INTERVAL"));
    assert!(pnfs_io.contains("refresh_layout_for_write"));

    for path in [
        common,
        runner,
        compatibility,
        cleanup,
        workflow,
        capability,
        rust_test,
        fault_runner,
        fault_helper,
        preflight_runner,
        layoutcommit_runner,
        recall_runner,
        recall_proxy,
    ] {
        assert!(
            !path.contains("Netapp1!"),
            "management credential leaked into repository"
        );
    }
}

#[test]
fn negotiated_channel_limit_contract_is_wired() {
    let workflow = fs::read_to_string(workspace_path(".github/workflows/nightly.yml"))
        .expect("nightly workflow must be readable");
    let runner = fs::read_to_string(workspace_path("tests/lab/run-channel-limits-e2e.sh"))
        .expect("channel limit runner must be readable");
    let rust_test = fs::read_to_string(workspace_path("tests/lab_e2e.rs"))
        .expect("lab E2E test must be readable");
    assert!(workflow.contains("NetApp NFSv4.1 negotiated channel limits E2E"));
    assert!(workflow.contains("tests/lab/run-channel-limits-e2e.sh \"$RUN_ID\""));
    assert!(runner.contains("validate_run_id"));
    assert!(runner.contains("nfs_v41_channel_limits_at_effective_bounds"));
    assert!(rust_test.contains("nfs41-channel-limits request="));
    assert!(rust_test.contains("effective_highest_slot_id"));
}
