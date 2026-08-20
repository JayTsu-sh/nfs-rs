use std::process::Command;

#[test]
fn validates_one_exact_protocol_environment_without_contacting_storage() {
    let output = Command::new(env!("CARGO_BIN_EXE_nfs-storage-benchmark"))
        .args([
            "--environment",
            "linux-source-v3",
            "--url",
            "nfs://10.10.1.12/srv/nfs/v3/ci/run?version=3&noresvport=true",
            "--validate-only",
        ])
        .output()
        .expect("benchmark program should start");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("configuration report must be JSON");
    assert_eq!(report["environment"], "linux-source-v3");
    assert_eq!(report["protocol"], "3");
    assert_eq!(report["status"], "configuration_valid");
}
