use std::process::Command;

fn run(args: &[&str]) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_nfs-perf-compare"))
        .args(args)
        .output()
        .expect("harness should start");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("JSON report")
}

#[test]
fn smoke_runs_all_suites_against_a_local_directory() {
    let root = std::env::temp_dir().join(format!("perfcmp-cli-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let target = root.to_str().unwrap();
    let common = [
        "--target",
        target,
        "--io",
        "buffered",
        "--workdir",
        "w",
        "--smoke",
    ];

    let meta = run(&[&common[..], &["metadata"]].concat());
    assert_eq!(meta["backend"], "posix");
    assert_eq!(meta["harness"], "rust");
    assert_eq!(meta["results"].as_array().unwrap().len(), 9);

    let small = run(&[&common[..], &["data", "--size", "4k", "--qd", "1"]].concat());
    assert_eq!(small["results"][0]["name"], "write_ms");

    let large = run(&[&common[..], &["data", "--size", "40m", "--qd", "8"]].concat());
    assert_eq!(large["results"][1]["name"], "read");
    assert!(large["results"][1]["median"].as_f64().unwrap() > 0.0);
    assert_eq!(large["results"][2]["name"], "read_hot");

    let mc = run(&[
        &common[..],
        &[
            "multiclient",
            "--size",
            "40m",
            "--clients",
            "2",
            "--mode",
            "same",
        ],
    ]
    .concat());
    assert_eq!(mc["results"][0]["name"], "aggregate_read");
    assert_eq!(mc["results"][1]["samples"].as_array().unwrap().len(), 2);
    assert!(mc["peak_rss_kib"].as_u64().unwrap() > 0);

    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    std::fs::remove_dir_all(root).unwrap();
}
