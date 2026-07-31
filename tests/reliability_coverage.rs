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
