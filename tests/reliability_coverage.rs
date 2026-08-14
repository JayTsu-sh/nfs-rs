use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

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
    for required in [
        "10.131.9.11",
        "10.128.61.200",
        "10.128.61.201",
        "tcp dport 2049 drop",
        "^(nightly|release)-",
    ] {
        assert!(helper.contains(required), "fault helper lacks {required}");
    }
    assert!(!helper.contains("input {"));
    assert!(runner.contains("trap cleanup EXIT INT TERM"));
    assert!(runner.contains("run_case below"));
    assert!(runner.contains("run_case above"));
    assert!(workflow.contains("run-netapp-v40-lease-fault-e2e.sh"));
    assert!(workflow.contains("Restore NetApp NFSv4.0 connectivity"));
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
