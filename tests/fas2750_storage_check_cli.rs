use std::process::Command;

#[test]
fn rejects_a_non_v40_url_before_contacting_storage() {
    let output = Command::new(env!("CARGO_BIN_EXE_fas2750-storage-check"))
        .args(["--url", "nfs://10.128.61.200/nfsrs_v40_test?version=4.1"])
        .output()
        .expect("diagnostic program should start");

    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).trim(),
        "--url must select exact NFSv4.0 with version=4.0"
    );
}

#[test]
fn requires_two_lifs_before_contacting_storage() {
    let output = Command::new(env!("CARGO_BIN_EXE_fas2750-storage-check"))
        .args(["--url", "nfs://10.128.61.200/nfsrs_v40_test?version=4.0"])
        .output()
        .expect("diagnostic program should start");

    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).trim(),
        "exactly two --url values are required"
    );
}
