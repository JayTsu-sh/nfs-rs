use std::env;
use std::io;
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use nfs_rs::{Mount, NFSVersion, OPEN_BOTH, OPEN_READ, parse_url_and_mount};

const LAB_ENABLE_ENV: &str = "NFS_RS_LAB_E2E";
const LAB_URLS_ENV: &str = "NFS_RS_LAB_URLS";
const CASE_DIR: &str = "nfs-rs-e2e";
const ORIGINAL_FILE: &str = "nfs-rs-e2e/payload.bin";
const RENAMED_FILE: &str = "nfs-rs-e2e/renamed.bin";
const HARD_LINK: &str = "nfs-rs-e2e/payload.hardlink";
const SYMLINK: &str = "nfs-rs-e2e/payload.symlink";
const SESSION_WSIZE_FILE: &str = "nfs-rs-e2e/session-wsize.bin";
const NFS41_FORE_CHANNEL_MAX_REQUEST_SIZE: usize = 1024 * 1024;
const RECOVERY_DIR: &str = "nfs41-session-recovery";
const RECOVERY_FILE: &str = "nfs41-session-recovery/payload.bin";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn ensure(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message.into()).into())
    }
}

fn payload() -> Bytes {
    let bytes = (0..(256 * 1024 + 37))
        .map(|index| ((index * 31 + 17) % 251) as u8)
        .collect::<Vec<_>>();
    Bytes::from(bytes)
}

async fn write_all(mount: &dyn Mount, fh: Bytes, data: &Bytes) -> TestResult {
    let chunk_size = (mount.get_max_write_size() as usize).min(64 * 1024);
    write_all_with_chunk_size(mount, fh, data, chunk_size).await
}

async fn write_all_with_chunk_size(
    mount: &dyn Mount,
    fh: Bytes,
    data: &Bytes,
    chunk_size: usize,
) -> TestResult {
    ensure(chunk_size > 0, "server reported a zero maximum write size")?;

    let mut offset = 0usize;
    while offset < data.len() {
        let end = (offset + chunk_size).min(data.len());
        let written = mount
            .write(fh.clone(), offset as u64, data.slice(offset..end))
            .await? as usize;
        ensure(written > 0, format!("zero-byte write at offset {offset}"))?;
        ensure(
            written <= end - offset,
            format!("server over-reported write count {written} at offset {offset}"),
        )?;
        offset += written;
    }
    mount.commit(fh, 0, data.len() as u32).await?;
    Ok(())
}

async fn read_all(mount: &dyn Mount, fh: Bytes, expected_len: usize) -> TestResult<Bytes> {
    let chunk_size = (mount.get_max_read_size() as usize).min(64 * 1024);
    ensure(chunk_size > 0, "server reported a zero maximum read size")?;

    let mut output = Vec::with_capacity(expected_len);
    while output.len() < expected_len {
        let count = chunk_size.min(expected_len - output.len()) as u32;
        let chunk = mount.read(fh.clone(), output.len() as u64, count).await?;
        ensure(
            !chunk.is_empty(),
            format!("unexpected EOF at offset {}", output.len()),
        )?;
        output.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(output))
}

async fn recover_file_from_checkpoint(url: &str, expected: &Bytes) -> TestResult {
    let mount = parse_url_and_mount(url).await?;
    let result = async {
        let opened = mount.open_path(RECOVERY_FILE, OPEN_BOTH).await?;
        write_all(mount.as_ref(), opened.fh.clone(), expected).await?;
        mount.close(opened.fh).await?;
        let opened = mount.open_path(RECOVERY_FILE, OPEN_READ).await?;
        let actual = read_all(mount.as_ref(), opened.fh.clone(), expected.len()).await?;
        mount.close(opened.fh).await?;
        ensure(
            actual == *expected,
            "post-recovery checksum payload mismatch",
        )
    }
    .await;
    let _ = mount.umount().await;
    result
}

async fn cleanup_case(mount: &dyn Mount) {
    for path in [
        SYMLINK,
        HARD_LINK,
        RENAMED_FILE,
        ORIGINAL_FILE,
        SESSION_WSIZE_FILE,
    ] {
        let _ = mount.remove_path(path).await;
    }
    let _ = mount.rmdir_path(CASE_DIR).await;
}

async fn exercise_endpoint(url: &str) -> TestResult {
    let mount = parse_url_and_mount(url).await?;
    let expected_version = if url.contains("version=4.1") {
        NFSVersion::NFSv4p1
    } else {
        NFSVersion::NFSv3
    };
    ensure(
        mount.version() == expected_version,
        format!(
            "{url}: mounted {:?}, expected {expected_version:?}",
            mount.version()
        ),
    )?;

    cleanup_case(mount.as_ref()).await;
    let result = async {
        mount.null().await?;
        mount.fsinfo().await?;
        mount.fsstat().await?;
        mount.pathconf(mount.getfh().await).await?;

        mount.mkdir_path(CASE_DIR, 0o755).await?;
        let created = mount.create_path(ORIGINAL_FILE, Some(0o640)).await?;
        let expected = payload();
        write_all(mount.as_ref(), created.fh.clone(), &expected).await?;
        mount.close(created.fh).await?;

        let attr = mount.getattr_path(ORIGINAL_FILE).await?;
        ensure(
            attr.filesize == expected.len() as u64,
            format!(
                "{url}: size mismatch after write: {} != {}",
                attr.filesize,
                expected.len()
            ),
        )?;

        mount
            .setattr_path(
                ORIGINAL_FILE,
                false,
                Some(0o600),
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
        let attr = mount.getattr_path(ORIGINAL_FILE).await?;
        ensure(
            attr.file_mode & 0o777 == 0o600,
            format!("{url}: mode mismatch after setattr: {:o}", attr.file_mode),
        )?;

        let opened = mount.open_path(ORIGINAL_FILE, OPEN_READ).await?;
        let actual = read_all(mount.as_ref(), opened.fh.clone(), expected.len()).await?;
        mount.close(opened.fh).await?;
        ensure(
            actual == expected,
            format!("{url}: read data differs from written payload"),
        )?;

        if expected_version == NFSVersion::NFSv4p1 {
            let negotiated_wsize = mount.get_max_write_size() as usize;
            ensure(
                negotiated_wsize < NFS41_FORE_CHANNEL_MAX_REQUEST_SIZE,
                format!(
                    "{url}: effective wsize {negotiated_wsize} must be smaller than the \
                     {NFS41_FORE_CHANNEL_MAX_REQUEST_SIZE}-byte session request limit"
                ),
            )?;

            let expected = Bytes::from(
                (0..(negotiated_wsize + 37))
                    .map(|index| ((index * 31 + 17) % 251) as u8)
                    .collect::<Vec<_>>(),
            );
            let created = mount.create_path(SESSION_WSIZE_FILE, Some(0o640)).await?;
            write_all_with_chunk_size(
                mount.as_ref(),
                created.fh.clone(),
                &expected,
                negotiated_wsize,
            )
            .await?;
            mount.close(created.fh).await?;

            let opened = mount.open_path(SESSION_WSIZE_FILE, OPEN_READ).await?;
            let actual = read_all(mount.as_ref(), opened.fh.clone(), expected.len()).await?;
            mount.close(opened.fh).await?;
            ensure(
                actual == expected,
                format!("{url}: session-limited wsize round trip differs from written payload"),
            )?;
        }

        let names = mount
            .readdir_path(CASE_DIR)
            .await?
            .map_ok(|entry| entry.file_name)
            .try_collect::<Vec<_>>()
            .await?;
        ensure(
            names.iter().any(|name| name == "payload.bin"),
            format!("{url}: READDIR did not return payload.bin: {names:?}"),
        )?;

        let detailed = mount
            .readdirplus_path(CASE_DIR)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let payload_entry = detailed
            .iter()
            .find(|entry| entry.file_name == "payload.bin");
        ensure(
            payload_entry.is_some(),
            format!("{url}: READDIRPLUS did not return payload.bin"),
        )?;
        ensure(
            payload_entry
                .and_then(|entry| entry.attr.as_ref())
                .is_some(),
            format!("{url}: READDIRPLUS returned payload.bin without attributes"),
        )?;

        mount.rename_path(ORIGINAL_FILE, RENAMED_FILE).await?;
        mount.link_path(RENAMED_FILE, HARD_LINK).await?;
        let linked = mount.read_path(HARD_LINK, 0, expected.len() as u32).await?;
        ensure(
            linked == expected,
            format!("{url}: hard-link read differs from written payload"),
        )?;

        mount.symlink_path("renamed.bin", SYMLINK).await?;
        let target = mount.readlink_path(SYMLINK).await?;
        ensure(
            target == "renamed.bin",
            format!("{url}: symlink target mismatch: {target:?}"),
        )?;

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    cleanup_case(mount.as_ref()).await;
    let unmount_result = mount.umount().await;
    result?;
    unmount_result?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the Terrasync NFS integration lab"]
async fn nfs_v3_and_v41_end_to_end() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        format!("{LAB_ENABLE_ENV}=1 is required"),
    )?;
    let urls = env::var(LAB_URLS_ENV)
        .map_err(|_| io::Error::other(format!("{LAB_URLS_ENV} is required")))?;
    let urls = urls.split_ascii_whitespace().collect::<Vec<_>>();
    ensure(!urls.is_empty(), format!("{LAB_URLS_ENV} is empty"))?;

    for url in urls {
        eprintln!("running NFS lab E2E against {url}");
        tokio::time::timeout(Duration::from_secs(120), exercise_endpoint(url))
            .await
            .map_err(|_| io::Error::other(format!("{url}: E2E timed out")))??;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires an authorized Terrasync NFS session fault"]
async fn nfs_v41_session_fault_reopen_resume_checksum() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_FAULT_URL")?;
    let ready = env::var("NFS_RS_LAB_FAULT_READY_FILE")?;
    let completed = env::var("NFS_RS_LAB_FAULT_DONE_FILE")?;
    let mount = parse_url_and_mount(&url).await?;
    ensure(mount.version() == NFSVersion::NFSv4p1, "NFSv4.1 required")?;
    let _ = mount.remove_path(RECOVERY_FILE).await;
    let _ = mount.rmdir_path(RECOVERY_DIR).await;
    mount.mkdir_path(RECOVERY_DIR, 0o755).await?;
    let created = mount.create_path(RECOVERY_FILE, Some(0o600)).await?;
    let chunk = Bytes::from(vec![0x5a; 64 * 1024]);

    std::fs::write(&ready, b"ready")?;
    tokio::time::timeout(Duration::from_secs(90), async {
        while !std::path::Path::new(&completed).exists() {
            let writes = (0..64u64).map(|index| {
                mount.write(
                    created.fh.clone(),
                    index * chunk.len() as u64,
                    chunk.clone(),
                )
            });
            let _outcomes = futures::future::join_all(writes).await;
        }
    })
    .await
    .map_err(|_| io::Error::other("session fault did not complete"))?;
    let _ = mount.umount().await;

    // Consumer-verified recovery policy: remount, reopen, restart the file from
    // the last trusted checkpoint, then verify the complete payload.
    let expected = Bytes::from(
        (0..(4 * 1024 * 1024))
            .map(|index| ((index * 17 + 29) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        match recover_file_from_checkpoint(&url, &expected).await {
            Ok(()) => break,
            Err(error) if tokio::time::Instant::now() < deadline => {
                eprintln!("recovery attempt deferred: {error}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => return Err(error),
        }
    }
    let mount = parse_url_and_mount(&url).await?;
    mount.remove_path(RECOVERY_FILE).await?;
    mount.rmdir_path(RECOVERY_DIR).await?;
    mount.umount().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires an authorized Terrasync NFS TCP reset"]
async fn nfs_v41_tcp_reset_rebind_checksum() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_FAULT_URL")?;
    let ready = env::var("NFS_RS_LAB_FAULT_READY_FILE")?;
    let completed = env::var("NFS_RS_LAB_FAULT_DONE_FILE")?;
    let mount = parse_url_and_mount(&url).await?;
    ensure(mount.version() == NFSVersion::NFSv4p1, "NFSv4.1 required")?;
    let _ = mount.remove_path(RECOVERY_FILE).await;
    let _ = mount.rmdir_path(RECOVERY_DIR).await;
    mount.mkdir_path(RECOVERY_DIR, 0o755).await?;
    let created = mount.create_path(RECOVERY_FILE, Some(0o600)).await?;
    let chunk = Bytes::from(vec![0xa5; 64 * 1024]);

    std::fs::write(&ready, b"ready")?;
    tokio::time::timeout(Duration::from_secs(60), async {
        while !std::path::Path::new(&completed).exists() {
            let writes = (0..64u64).map(|index| {
                mount.write(
                    created.fh.clone(),
                    index * chunk.len() as u64,
                    chunk.clone(),
                )
            });
            let _outcomes = futures::future::join_all(writes).await;
        }
    })
    .await
    .map_err(|_| io::Error::other("TCP reset did not complete"))?;

    let expected = Bytes::from(
        (0..(4 * 1024 * 1024))
            .map(|index| ((index * 13 + 41) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    write_all(mount.as_ref(), created.fh.clone(), &expected).await?;
    mount.close(created.fh).await?;
    let opened = mount.open_path(RECOVERY_FILE, OPEN_READ).await?;
    let actual = read_all(mount.as_ref(), opened.fh.clone(), expected.len()).await?;
    mount.close(opened.fh).await?;
    ensure(actual == expected, "post-rebind checksum payload mismatch")?;
    mount.remove_path(RECOVERY_FILE).await?;
    mount.rmdir_path(RECOVERY_DIR).await?;
    mount.umount().await?;
    Ok(())
}
