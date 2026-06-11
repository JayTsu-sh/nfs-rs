// 一次性测试工具：把 NFS 目标端恢复成 "文件 a 挡住目录 a/b/c" 的脏状态，
// 用于验证 data-mover create_dir_all 的非目录冲突替换逻辑（含并发竞争收敛）。
//
// Run from inside nfs-rs/:
//   cargo run --example reset_dirty_a -- "nfs://10.128.61.201:/deep_sync_nfs?version=4.1"

use std::env;

use bytes::Bytes;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: reset_dirty_a <nfs-url>");
        std::process::exit(2);
    }
    let mount = nfs_rs::parse_url_and_mount(&args[1])
        .await
        .expect("mount failed");

    // 1. 清掉现有的 a 目录树（best-effort，逐层删）
    for f in ["a/b/c/d.txt"] {
        match mount.remove_path(f).await {
            Ok(_) => println!("removed file {f}"),
            Err(e) => println!("remove {f}: {e} (ignored)"),
        }
    }
    for d in ["a/b/c", "a/b", "a"] {
        match mount.rmdir_path(d).await {
            Ok(_) => println!("rmdir {d}"),
            Err(e) => println!("rmdir {d}: {e} (ignored)"),
        }
    }

    // 2. 重建脏状态：根下创建 9 字节的普通文件 "a"
    let root = mount.getfh().await;
    let obj = mount
        .create(root, "a", Some(0o644))
        .await
        .expect("create file a failed");
    let written = mount
        .write(obj.fh, 0, Bytes::from_static(b"b/c/d.txt"))
        .await
        .expect("write file a failed");
    println!("created dirty file 'a' ({written} bytes)");

    mount.umount().await.expect("umount failed");
}
