use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::io;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{StreamExt, TryStreamExt};
use nfs_rs::{
    AceFlags, AceMask, AceType, Acl, Acl41Flags, Mount, MountLifecycleState, NFSVersion, NfsAce,
    NfsAcl41, NfsError, OPEN_BOTH, OPEN_READ, Time, parse_url_and_mount,
};

const LAB_ENABLE_ENV: &str = "NFS_RS_LAB_E2E";
const LAB_URLS_ENV: &str = "NFS_RS_LAB_URLS";
const LAB_V40_URLS_ENV: &str = "NFS_RS_LAB_V40_URLS";
const LAB_ACL_URLS_ENV: &str = "NFS_RS_LAB_ACL_URLS";
const LAB_ACL_SOURCE_URL_ENV: &str = "NFS_RS_LAB_ACL_SOURCE_URL";
const LAB_ACL_TARGET_URL_ENV: &str = "NFS_RS_LAB_ACL_TARGET_URL";
const LAB_ACL_LINUX_V40_URL_ENV: &str = "NFS_RS_LAB_ACL_LINUX_V40_URL";
const LAB_ACL_LINUX_V41_URL_ENV: &str = "NFS_RS_LAB_ACL_LINUX_V41_URL";
const LAB_ACL_FAS2750_V40_URL_ENV: &str = "NFS_RS_LAB_ACL_FAS2750_V40_URL";
const LAB_ACL_FAS2750_V41_URL_ENV: &str = "NFS_RS_LAB_ACL_FAS2750_V41_URL";
const CASE_DIR: &str = "nfs-rs-e2e";
const ORIGINAL_FILE: &str = "nfs-rs-e2e/payload.bin";
const RENAMED_FILE: &str = "nfs-rs-e2e/renamed.bin";
const HARD_LINK: &str = "nfs-rs-e2e/payload.hardlink";
const SYMLINK: &str = "nfs-rs-e2e/payload.symlink";
const SESSION_WSIZE_FILE: &str = "nfs-rs-e2e/session-wsize.bin";
const NFS41_FORE_CHANNEL_MAX_REQUEST_SIZE: usize = 1024 * 1024;
const RECOVERY_DIR: &str = "nfs41-session-recovery";
const RECOVERY_FILE: &str = "nfs41-session-recovery/payload.bin";
const CALLBACK_FILE: &str = "callback-recall.bin";
const PNFS_PAYLOAD_SIZE: usize = 8 * 1024 * 1024 + 37;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy)]
enum AclObjectKind {
    File,
    Directory,
    InheritedFile,
    InheritedDirectory,
}

#[tokio::test]
#[ignore = "requires an NFSv4.0 server for raw MAXREAD/MAXWRITE observation"]
async fn nfs_v40_server_max_io_attributes() -> TestResult {
    for configured_url in env::var(LAB_V40_URLS_ENV)?.split(',') {
        let mut url = url::Url::parse(configured_url)?;
        url.query_pairs_mut()
            .append_pair("rsize", &u32::MAX.to_string())
            .append_pair("wsize", &u32::MAX.to_string());
        let mount = parse_url_and_mount(url.as_str()).await?;
        let fsinfo = mount.fsinfo().await?;
        println!(
            "{} MAXREAD={} MAXWRITE={}",
            url.host_str().unwrap_or("unknown"),
            fsinfo.rtmax,
            fsinfo.wtmax
        );
        mount.umount().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a writable single-export NFSv4.0 fixture"]
async fn nfs_v40_single_export_end_to_end() -> TestResult {
    let urls = env::var(LAB_V40_URLS_ENV)?;
    let urls = urls
        .split(',')
        .filter(|url| !url.is_empty())
        .collect::<Vec<_>>();
    ensure(urls.len() == 1, "single-export validation requires one URL")?;
    let url = urls[0];
    ensure(
        url.contains("version=4.0"),
        format!("not an exact v4.0 URL: {url}"),
    )?;

    let run_id = env::var("NFS_RS_LAB_V40_RUN_ID")
        .unwrap_or_else(|_| format!("single-export-{}", std::process::id()));
    let original = format!("nfs-rs-{run_id}-payload.bin");
    let renamed = format!("nfs-rs-{run_id}-renamed.bin");
    let hardlink = format!("nfs-rs-{run_id}-hardlink.bin");
    let symlink = format!("nfs-rs-{run_id}-symlink");
    let payload = Bytes::from(
        (0..(4 * 1024 * 1024 + 37))
            .map(|index| ((index * 17 + 29) % 251) as u8)
            .collect::<Vec<_>>(),
    );

    let mount = parse_url_and_mount(url).await?;
    ensure(
        mount.version() == NFSVersion::NFSv4p0,
        "single-export fixture negotiated the wrong protocol",
    )?;
    mount.null().await?;
    ensure(!mount.getfh().await.is_empty(), "empty export filehandle")?;
    for residual in [&original, &renamed, &hardlink, &symlink] {
        let _ = mount.remove_path(residual).await;
    }

    let result: TestResult = async {
        let created = mount.create_path(&original, Some(0o600)).await?;
        let mut written = 0usize;
        while written < payload.len() {
            let end = (written + mount.get_max_write_size() as usize).min(payload.len());
            let count = mount
                .write_stable(
                    created.fh.clone(),
                    written as u64,
                    payload.slice(written..end),
                )
                .await? as usize;
            ensure(count != 0 && count <= end - written, "invalid WRITE count")?;
            written += count;
        }
        mount
            .commit(created.fh.clone(), 0, payload.len() as u32)
            .await?;
        mount.close(created.fh).await?;

        let reopened = mount.open_path(&original, OPEN_READ).await?;
        ensure(
            mount.getattr(reopened.fh.clone()).await?.filesize == payload.len() as u64,
            "single-export GETATTR size mismatch",
        )?;
        ensure(
            read_all(mount.as_ref(), reopened.fh.clone(), payload.len()).await? == payload,
            "single-export payload checksum mismatch",
        )?;
        mount.close(reopened.fh).await?;

        mount.rename_path(&original, &renamed).await?;
        let renamed_object = mount.lookup_path(&renamed).await?;
        let linked = mount.link_path(&renamed, &hardlink).await?;
        ensure(
            linked.nlink >= 2 && mount.lookup_path(&hardlink).await?.fh == renamed_object.fh,
            "single-export hard-link identity mismatch",
        )?;
        let symbolic = mount.symlink_path(&renamed, &symlink).await?;
        ensure(
            mount.readlink(symbolic.fh).await? == renamed,
            "single-export symbolic-link target mismatch",
        )?;
        let names = mount
            .readdir(mount.getfh().await)
            .await
            .map_ok(|entry| entry.file_name)
            .try_collect::<Vec<_>>()
            .await?;
        for expected in [&renamed, &hardlink, &symlink] {
            ensure(
                names.contains(expected),
                format!("single-export READDIR omitted {expected}"),
            )?;
        }
        Ok(())
    }
    .await;

    let mut cleanup_error = None;
    for path in [&symlink, &hardlink, &renamed, &original] {
        if let Err(error) = mount.remove_path(path).await
            && !error.is_not_found()
        {
            cleanup_error.get_or_insert_with(|| Box::new(error) as _);
        }
    }
    if let Err(error) = mount.umount().await {
        cleanup_error.get_or_insert_with(|| Box::new(error) as _);
    }
    result?;
    if let Some(error) = cleanup_error {
        return Err(error);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the NetApp NFSv4.0 reference fixture"]
async fn nfs_v40_mount_null_and_traversal_on_both_lifs() -> TestResult {
    let urls = env::var(LAB_V40_URLS_ENV)?;
    let urls = urls
        .split(',')
        .filter(|url| !url.is_empty())
        .collect::<Vec<_>>();
    ensure(
        urls.len() == 2,
        "NFSv4.0 validation requires exactly two LIF URLs",
    )?;
    for url in urls {
        ensure(
            url.contains("version=4.0"),
            format!("not an exact v4.0 URL: {url}"),
        )?;
        let mount = parse_url_and_mount(url).await?;
        ensure(
            mount.version() == NFSVersion::NFSv4p0,
            "wrong selected protocol",
        )?;
        ensure(
            !mount.getfh().await.is_empty(),
            "export traversal returned an empty filehandle",
        )?;
        mount.null().await?;
        mount.umount().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires writable real NFSv4.0 ACL fixtures"]
async fn nfs_v40_file_and_directory_acl_primitives_cover_children() -> TestResult {
    let urls = env::var(LAB_ACL_URLS_ENV).or_else(|_| env::var(LAB_V40_URLS_ENV))?;
    let urls = urls
        .split(',')
        .filter(|url| !url.is_empty())
        .collect::<Vec<_>>();
    ensure(!urls.is_empty(), "NFSv4.0 ACL validation requires URLs")?;

    let owner_directory_mask = AceMask::READ_DATA
        | AceMask::WRITE_DATA
        | AceMask::APPEND_DATA
        | AceMask::EXECUTE
        | AceMask::DELETE_CHILD
        | AceMask::READ_ATTRIBUTES
        | AceMask::WRITE_ATTRIBUTES
        | AceMask::READ_NAMED_ATTRS
        | AceMask::WRITE_NAMED_ATTRS
        | AceMask::READ_ACL
        | AceMask::WRITE_ACL
        | AceMask::WRITE_OWNER
        | AceMask::SYNCHRONIZE;
    let owner_file_mask = AceMask::READ_DATA
        | AceMask::WRITE_DATA
        | AceMask::APPEND_DATA
        | AceMask::READ_ATTRIBUTES
        | AceMask::WRITE_ATTRIBUTES
        | AceMask::READ_NAMED_ATTRS
        | AceMask::WRITE_NAMED_ATTRS
        | AceMask::READ_ACL
        | AceMask::WRITE_ACL
        | AceMask::WRITE_OWNER
        | AceMask::SYNCHRONIZE;
    let read_directory_mask = AceMask::READ_DATA
        | AceMask::EXECUTE
        | AceMask::READ_ATTRIBUTES
        | AceMask::READ_NAMED_ATTRS
        | AceMask::READ_ACL
        | AceMask::SYNCHRONIZE;
    let read_file_mask = AceMask::READ_DATA
        | AceMask::READ_ATTRIBUTES
        | AceMask::READ_NAMED_ATTRS
        | AceMask::READ_ACL
        | AceMask::SYNCHRONIZE;

    for (index, url) in urls.into_iter().enumerate() {
        ensure(
            url.contains("version=4.0"),
            format!("not an exact v4.0 URL: {url}"),
        )?;
        let mount = parse_url_and_mount(url).await?;
        let root = format!("nfs-rs-v40-acl-primitives-{index}");
        let root_file = format!("{root}/root-file.txt");
        let child_directory = format!("{root}/child");
        let child_file = format!("{child_directory}/child-file.txt");

        let _ = mount.remove_path(&child_file).await;
        let _ = mount.rmdir_path(&child_directory).await;
        let _ = mount.remove_path(&root_file).await;
        let _ = mount.rmdir_path(&root).await;

        let result: TestResult = async {
            let root_object = mount.mkdir_path(&root, 0o700).await?;
            let root_acl = Acl {
                aces: vec![
                    NfsAce {
                        ace_type: AceType::AccessAllowed,
                        flags: AceFlags(0),
                        access_mask: AceMask(owner_directory_mask),
                        who: "OWNER@".to_string(),
                    },
                    NfsAce {
                        ace_type: AceType::AccessAllowed,
                        flags: AceFlags(AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT),
                        access_mask: AceMask(read_directory_mask),
                        who: "EVERYONE@".to_string(),
                    },
                ],
            };
            mount.setacl(root_object.fh.clone(), &root_acl).await?;
            let accepted_root_acl = mount.getacl(root_object.fh.clone()).await?;
            ensure(
                !accepted_root_acl.aces.is_empty(),
                format!("NFSv4.0 directory returned an empty ACL through {url}"),
            )?;
            mount
                .setacl(root_object.fh.clone(), &accepted_root_acl)
                .await?;
            ensure(
                mount.getacl(root_object.fh.clone()).await? == accepted_root_acl,
                format!("NFSv4.0 directory ACL did not stabilize through {url}"),
            )?;

            let root_file_object = mount.create_path(&root_file, Some(0o600)).await?;
            mount.close(root_file_object.fh.clone()).await?;
            let root_file_acl = Acl {
                aces: vec![
                    NfsAce {
                        ace_type: AceType::AccessAllowed,
                        flags: AceFlags(0),
                        access_mask: AceMask(owner_file_mask),
                        who: "OWNER@".to_string(),
                    },
                    NfsAce {
                        ace_type: AceType::AccessAllowed,
                        flags: AceFlags(0),
                        access_mask: AceMask(read_file_mask),
                        who: "EVERYONE@".to_string(),
                    },
                ],
            };
            mount
                .setacl(root_file_object.fh.clone(), &root_file_acl)
                .await?;
            let accepted_root_file_acl = mount.getacl(root_file_object.fh.clone()).await?;
            ensure(
                !accepted_root_file_acl.aces.is_empty(),
                format!("NFSv4.0 file returned an empty ACL through {url}"),
            )?;
            mount
                .setacl(root_file_object.fh.clone(), &accepted_root_file_acl)
                .await?;
            ensure(
                mount.getacl(root_file_object.fh.clone()).await? == accepted_root_file_acl,
                format!("NFSv4.0 file ACL did not stabilize through {url}"),
            )?;

            let child_directory_object = mount.mkdir_path(&child_directory, 0o700).await?;
            let inherited_child_acl = mount.getacl(child_directory_object.fh.clone()).await?;
            ensure(
                inherited_child_acl
                    .aces
                    .iter()
                    .any(|ace| ace.who == "EVERYONE@"),
                format!("NFSv4.0 child directory did not inherit an EVERYONE@ ACE through {url}"),
            )?;
            let child_directory_acl = Acl {
                aces: vec![NfsAce {
                    ace_type: AceType::AccessAllowed,
                    flags: AceFlags(AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT),
                    access_mask: AceMask(owner_directory_mask),
                    who: "OWNER@".to_string(),
                }],
            };
            mount
                .setacl(child_directory_object.fh.clone(), &child_directory_acl)
                .await?;
            let accepted_child_directory_acl =
                mount.getacl(child_directory_object.fh.clone()).await?;
            ensure(
                accepted_child_directory_acl != inherited_child_acl,
                format!("NFSv4.0 child-directory ACL did not change after SET through {url}"),
            )?;
            mount
                .setacl(
                    child_directory_object.fh.clone(),
                    &accepted_child_directory_acl,
                )
                .await?;
            ensure(
                mount.getacl(child_directory_object.fh.clone()).await?
                    == accepted_child_directory_acl,
                format!("NFSv4.0 child-directory ACL did not stabilize through {url}"),
            )?;

            let child_file_object = mount.create_path(&child_file, Some(0o600)).await?;
            mount.close(child_file_object.fh.clone()).await?;
            let inherited_file_acl = mount.getacl(child_file_object.fh.clone()).await?;
            ensure(
                !inherited_file_acl.aces.is_empty(),
                format!("NFSv4.0 child file returned an empty inherited ACL through {url}"),
            )?;
            let child_file_acl = Acl {
                aces: vec![NfsAce {
                    ace_type: AceType::AccessAllowed,
                    flags: AceFlags(0),
                    access_mask: AceMask(read_file_mask),
                    who: "EVERYONE@".to_string(),
                }],
            };
            mount
                .setacl(child_file_object.fh.clone(), &child_file_acl)
                .await?;
            let accepted_child_file_acl = mount.getacl(child_file_object.fh.clone()).await?;
            ensure(
                accepted_child_file_acl != inherited_file_acl,
                format!("NFSv4.0 child-file ACL did not change after SET through {url}"),
            )?;
            mount
                .setacl(child_file_object.fh.clone(), &accepted_child_file_acl)
                .await?;
            ensure(
                mount.getacl(child_file_object.fh.clone()).await? == accepted_child_file_acl,
                format!("NFSv4.0 child-file ACL did not stabilize through {url}"),
            )?;
            Ok(())
        }
        .await;

        let mut cleanup_error = None;
        if let Err(error) = mount.remove_path(&child_file).await
            && !error.is_not_found()
        {
            cleanup_error.get_or_insert_with(|| Box::new(error) as _);
        }
        if let Err(error) = mount.rmdir_path(&child_directory).await
            && !error.is_not_found()
        {
            cleanup_error.get_or_insert_with(|| Box::new(error) as _);
        }
        if let Err(error) = mount.remove_path(&root_file).await
            && !error.is_not_found()
        {
            cleanup_error.get_or_insert_with(|| Box::new(error) as _);
        }
        if let Err(error) = mount.rmdir_path(&root).await
            && !error.is_not_found()
        {
            cleanup_error.get_or_insert_with(|| Box::new(error) as _);
        }
        if let Err(error) = mount.umount().await {
            cleanup_error.get_or_insert_with(|| Box::new(error) as _);
        }
        result?;
        if let Some(error) = cleanup_error {
            return Err(error);
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires writable Linux knfsd source and FAS2750 target fixtures"]
async fn nfs_v40_linux_acls_migrate_to_fas2750() -> TestResult {
    let source_url = env::var(LAB_ACL_SOURCE_URL_ENV)?;
    let target_url = env::var(LAB_ACL_TARGET_URL_ENV)?;
    for (role, url) in [("source", &source_url), ("target", &target_url)] {
        ensure(
            url.contains("version=4.0"),
            format!("ACL migration {role} is not an exact NFSv4.0 URL: {url}"),
        )?;
    }

    let source = parse_url_and_mount(&source_url).await?;
    let target = parse_url_and_mount(&target_url).await?;
    ensure(
        source.version() == NFSVersion::NFSv4p0 && target.version() == NFSVersion::NFSv4p0,
        "ACL migration fixtures negotiated the wrong protocol",
    )?;

    let case = format!("nfs-rs-acl-migration-{}", std::process::id());
    let child_directory = format!("{case}/child");
    let child_file = format!("{child_directory}/payload.bin");
    for mount in [&source, &target] {
        let _ = mount.remove_path(&child_file).await;
        let _ = mount.rmdir_path(&child_directory).await;
        let _ = mount.rmdir_path(&case).await;
    }

    let result: TestResult = async {
        let source_root = source.mkdir_path(&case, 0o700).await?;
        let source_root_acl = Acl {
            aces: vec![
                NfsAce {
                    ace_type: AceType::AccessAllowed,
                    flags: AceFlags(0),
                    access_mask: AceMask(
                        AceMask::READ_DATA
                            | AceMask::WRITE_DATA
                            | AceMask::APPEND_DATA
                            | AceMask::EXECUTE
                            | AceMask::DELETE_CHILD
                            | AceMask::READ_ATTRIBUTES
                            | AceMask::WRITE_ATTRIBUTES
                            | AceMask::READ_NAMED_ATTRS
                            | AceMask::WRITE_NAMED_ATTRS
                            | AceMask::READ_ACL
                            | AceMask::WRITE_ACL
                            | AceMask::WRITE_OWNER
                            | AceMask::SYNCHRONIZE,
                    ),
                    who: "OWNER@".to_string(),
                },
                NfsAce {
                    ace_type: AceType::AccessAllowed,
                    flags: AceFlags(AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT),
                    access_mask: AceMask(
                        AceMask::READ_DATA
                            | AceMask::EXECUTE
                            | AceMask::READ_ATTRIBUTES
                            | AceMask::READ_NAMED_ATTRS
                            | AceMask::READ_ACL
                            | AceMask::SYNCHRONIZE,
                    ),
                    who: "EVERYONE@".to_string(),
                },
            ],
        };
        source
            .setacl(source_root.fh.clone(), &source_root_acl)
            .await?;
        let source_directory = source.mkdir_path(&child_directory, 0o750).await?;
        let source_file = source.create_path(&child_file, Some(0o640)).await?;
        source.close(source_file.fh.clone()).await?;

        // The Linux server's readback, including its normalization, is the
        // authoritative source snapshot used by this migration test.
        let source_directory_acl = source.getacl(source_directory.fh.clone()).await?;
        let source_file_acl = source.getacl(source_file.fh.clone()).await?;
        ensure(
            !source_directory_acl.aces.is_empty() && !source_file_acl.aces.is_empty(),
            "Linux source returned an empty ACL",
        )?;

        target.mkdir_path(&case, 0o700).await?;
        let target_directory = target.mkdir_path(&child_directory, 0o700).await?;
        let target_file = target.create_path(&child_file, Some(0o600)).await?;
        target.close(target_file.fh.clone()).await?;

        // Existing objects migrate file ACLs first and directory ACLs last so
        // final directory inheritance cannot affect creation of this tree.
        target
            .setacl(target_file.fh.clone(), &source_file_acl)
            .await?;
        let target_file_acl = target.getacl(target_file.fh.clone()).await?;
        ensure_acl_structural_fidelity(
            &source_file_acl,
            &target_file_acl,
            "Linux-to-FAS2750 file",
        )?;
        target
            .setacl(target_directory.fh.clone(), &source_directory_acl)
            .await?;
        let target_directory_acl = target.getacl(target_directory.fh.clone()).await?;
        ensure_acl_structural_fidelity(
            &source_directory_acl,
            &target_directory_acl,
            "Linux-to-FAS2750 directory",
        )?;
        Ok(())
    }
    .await;

    let mut cleanup_error = None;
    for mount in [&target, &source] {
        if let Err(error) = mount.remove_path(&child_file).await
            && !error.is_not_found()
        {
            cleanup_error.get_or_insert_with(|| Box::new(error) as _);
        }
        if let Err(error) = mount.rmdir_path(&child_directory).await
            && !error.is_not_found()
        {
            cleanup_error.get_or_insert_with(|| Box::new(error) as _);
        }
        if let Err(error) = mount.rmdir_path(&case).await
            && !error.is_not_found()
        {
            cleanup_error.get_or_insert_with(|| Box::new(error) as _);
        }
    }
    if let Err(error) = source.umount().await {
        cleanup_error.get_or_insert_with(|| Box::new(error) as _);
    }
    if let Err(error) = target.umount().await {
        cleanup_error.get_or_insert_with(|| Box::new(error) as _);
    }
    result?;
    if let Some(error) = cleanup_error {
        return Err(error);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires writable Linux knfsd and FAS2750 NFSv4.0/NFSv4.1 fixtures"]
async fn nfsv4_acl_migration_covers_protocol_storage_matrix() -> TestResult {
    let endpoints = [
        (
            "linux-v40",
            env::var(LAB_ACL_LINUX_V40_URL_ENV)?,
            NFSVersion::NFSv4p0,
        ),
        (
            "linux-v41",
            env::var(LAB_ACL_LINUX_V41_URL_ENV)?,
            NFSVersion::NFSv4p1,
        ),
        (
            "fas2750-v40",
            env::var(LAB_ACL_FAS2750_V40_URL_ENV)?,
            NFSVersion::NFSv4p0,
        ),
        (
            "fas2750-v41",
            env::var(LAB_ACL_FAS2750_V41_URL_ENV)?,
            NFSVersion::NFSv4p1,
        ),
    ];

    for (source_index, (source_label, source_url, source_version)) in endpoints.iter().enumerate() {
        for (target_index, (target_label, target_url, target_version)) in
            endpoints.iter().enumerate()
        {
            let label = format!("{source_label}-to-{target_label}");
            let case_index = source_index * endpoints.len() + target_index;
            ensure(
                source_url.contains(match source_version {
                    NFSVersion::NFSv4p0 => "version=4.0",
                    NFSVersion::NFSv4p1 => "version=4.1",
                    _ => unreachable!(),
                }) && target_url.contains(match target_version {
                    NFSVersion::NFSv4p0 => "version=4.0",
                    NFSVersion::NFSv4p1 => "version=4.1",
                    _ => unreachable!(),
                }),
                format!("ACL matrix URL/version mismatch for {label}"),
            )?;
            let source = parse_url_and_mount(source_url).await?;
            let target = parse_url_and_mount(target_url).await?;
            ensure(
                source.version() == *source_version && target.version() == *target_version,
                format!("ACL matrix case {label} negotiated the wrong protocol version"),
            )?;

            let suffix = format!("{}-{case_index}", std::process::id());
            let source_root = format!("nfs-rs-acl-cross-version-source-{suffix}");
            let source_directory = format!("{source_root}/child");
            let source_file = format!("{source_directory}/payload.bin");
            let source_inherited_directory = format!("{source_directory}/inherited-child");
            let source_inherited_file = format!("{source_inherited_directory}/inherited.bin");
            let target_root = format!("nfs-rs-acl-cross-version-target-{suffix}");
            let target_directory = format!("{target_root}/child");
            let target_file = format!("{target_directory}/payload.bin");
            let target_inherited_directory = format!("{target_directory}/inherited-child");
            let target_inherited_file = format!("{target_inherited_directory}/inherited.bin");

            for (mount, inherited_file, inherited_directory, file, directory, root) in [
                (
                    &source,
                    &source_inherited_file,
                    &source_inherited_directory,
                    &source_file,
                    &source_directory,
                    &source_root,
                ),
                (
                    &target,
                    &target_inherited_file,
                    &target_inherited_directory,
                    &target_file,
                    &target_directory,
                    &target_root,
                ),
            ] {
                let _ = mount.remove_path(inherited_file).await;
                let _ = mount.rmdir_path(inherited_directory).await;
                let _ = mount.remove_path(file).await;
                let _ = mount.rmdir_path(directory).await;
                let _ = mount.rmdir_path(root).await;
            }

            let result: TestResult = async {
                let source_root_object = source.mkdir_path(&source_root, 0o700).await?;
                let inheritable_acl = Acl {
                    aces: vec![
                        NfsAce {
                            ace_type: AceType::AccessAllowed,
                            flags: AceFlags(0),
                            access_mask: AceMask(
                                AceMask::READ_DATA
                                    | AceMask::WRITE_DATA
                                    | AceMask::APPEND_DATA
                                    | AceMask::EXECUTE
                                    | AceMask::DELETE_CHILD
                                    | AceMask::READ_ATTRIBUTES
                                    | AceMask::WRITE_ATTRIBUTES
                                    | AceMask::READ_NAMED_ATTRS
                                    | AceMask::WRITE_NAMED_ATTRS
                                    | AceMask::READ_ACL
                                    | AceMask::WRITE_ACL
                                    | AceMask::WRITE_OWNER
                                    | AceMask::SYNCHRONIZE,
                            ),
                            who: "OWNER@".to_string(),
                        },
                        NfsAce {
                            ace_type: AceType::AccessAllowed,
                            flags: AceFlags(AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT),
                            access_mask: AceMask(
                                AceMask::READ_DATA
                                    | AceMask::EXECUTE
                                    | AceMask::READ_ATTRIBUTES
                                    | AceMask::READ_NAMED_ATTRS
                                    | AceMask::READ_ACL
                                    | AceMask::SYNCHRONIZE,
                            ),
                            who: "EVERYONE@".to_string(),
                        },
                    ],
                };
                source
                    .setacl(source_root_object.fh.clone(), &inheritable_acl)
                    .await?;
                let source_directory_object = source.mkdir_path(&source_directory, 0o750).await?;
                let source_file_object = source.create_path(&source_file, Some(0o640)).await?;
                source.close(source_file_object.fh.clone()).await?;
                let directory_snapshot = source.getacl(source_directory_object.fh.clone()).await?;
                let file_snapshot = source.getacl(source_file_object.fh.clone()).await?;
                ensure(
                    !directory_snapshot.aces.is_empty() && !file_snapshot.aces.is_empty(),
                    format!("ACL matrix source returned an empty ACL for {label}"),
                )?;

                target.mkdir_path(&target_root, 0o700).await?;
                let target_directory_object = target.mkdir_path(&target_directory, 0o700).await?;
                let target_file_object = target.create_path(&target_file, Some(0o600)).await?;
                target.close(target_file_object.fh.clone()).await?;

                target
                    .setacl(target_file_object.fh.clone(), &file_snapshot)
                    .await?;
                let accepted_file_acl = target.getacl(target_file_object.fh.clone()).await?;
                let file_fidelity = classify_acl_fidelity(
                    source_label,
                    target_label,
                    AclObjectKind::File,
                    &file_snapshot,
                    &accepted_file_acl,
                )
                .map_err(io::Error::other)?;
                if file_fidelity != "EXACT" {
                        target.setacl(target_file_object.fh.clone(), &accepted_file_acl).await?;
                        let stable = target.getacl(target_file_object.fh.clone()).await?;
                        ensure_acl_structural_fidelity(
                            &accepted_file_acl,
                            &stable,
                            &format!("ACL matrix normalized file {label}"),
                        )?;
                }
                target
                    .setacl(target_directory_object.fh.clone(), &directory_snapshot)
                    .await?;
                let accepted_directory_acl = target
                    .getacl(target_directory_object.fh.clone())
                    .await?;
                let directory_fidelity = classify_acl_fidelity(
                    source_label,
                    target_label,
                    AclObjectKind::Directory,
                    &directory_snapshot,
                    &accepted_directory_acl,
                )
                .map_err(io::Error::other)?;
                if directory_fidelity != "EXACT" {
                        target
                            .setacl(
                                target_directory_object.fh.clone(),
                                &accepted_directory_acl,
                            )
                            .await?;
                        let stable = target
                            .getacl(target_directory_object.fh.clone())
                            .await?;
                        ensure_acl_structural_fidelity(
                            &accepted_directory_acl,
                            &stable,
                            &format!("ACL matrix normalized directory {label}"),
                    )?;
                }

                let source_inherited_directory_object = source
                    .mkdir_path(&source_inherited_directory, 0o700)
                    .await?;
                let source_inherited_file_object = source
                    .create_path(&source_inherited_file, Some(0o600))
                    .await?;
                source
                    .close(source_inherited_file_object.fh.clone())
                    .await?;
                let target_inherited_directory_object = target
                    .mkdir_path(&target_inherited_directory, 0o700)
                    .await?;
                let target_inherited_file_object = target
                    .create_path(&target_inherited_file, Some(0o600))
                    .await?;
                target
                    .close(target_inherited_file_object.fh.clone())
                    .await?;
                let inherited_directory_fidelity = classify_acl_fidelity(
                    source_label,
                    target_label,
                    AclObjectKind::InheritedDirectory,
                    &source
                        .getacl(source_inherited_directory_object.fh.clone())
                        .await?,
                    &target
                        .getacl(target_inherited_directory_object.fh.clone())
                        .await?,
                )
                .map_err(io::Error::other)?;
                let inherited_file_fidelity = classify_acl_fidelity(
                    source_label,
                    target_label,
                    AclObjectKind::InheritedFile,
                    &source
                        .getacl(source_inherited_file_object.fh.clone())
                        .await?,
                    &target
                        .getacl(target_inherited_file_object.fh.clone())
                        .await?,
                )
                .map_err(io::Error::other)?;
                println!(
                    "NFSV4_ACL_MIGRATION_MATRIX=PASS case={label} source={:?} target={:?} file_fidelity={file_fidelity} directory_fidelity={directory_fidelity} inherited_file_fidelity={inherited_file_fidelity} inherited_directory_fidelity={inherited_directory_fidelity}",
                    source.version(),
                    target.version(),
                );
                Ok(())
            }
            .await;

            let mut cleanup_error = None;
            for (mount, inherited_file, inherited_directory, file, directory, root) in [
                (
                    &target,
                    &target_inherited_file,
                    &target_inherited_directory,
                    &target_file,
                    &target_directory,
                    &target_root,
                ),
                (
                    &source,
                    &source_inherited_file,
                    &source_inherited_directory,
                    &source_file,
                    &source_directory,
                    &source_root,
                ),
            ] {
                if let Err(error) = mount.remove_path(inherited_file).await
                    && !error.is_not_found()
                {
                    cleanup_error.get_or_insert_with(|| Box::new(error) as _);
                }
                if let Err(error) = mount.rmdir_path(inherited_directory).await
                    && !error.is_not_found()
                {
                    cleanup_error.get_or_insert_with(|| Box::new(error) as _);
                }
                if let Err(error) = mount.remove_path(file).await
                    && !error.is_not_found()
                {
                    cleanup_error.get_or_insert_with(|| Box::new(error) as _);
                }
                if let Err(error) = mount.rmdir_path(directory).await
                    && !error.is_not_found()
                {
                    cleanup_error.get_or_insert_with(|| Box::new(error) as _);
                }
                if let Err(error) = mount.rmdir_path(root).await
                    && !error.is_not_found()
                {
                    cleanup_error.get_or_insert_with(|| Box::new(error) as _);
                }
            }
            if let Err(error) = source.umount().await {
                cleanup_error.get_or_insert_with(|| Box::new(error) as _);
            }
            if let Err(error) = target.umount().await {
                cleanup_error.get_or_insert_with(|| Box::new(error) as _);
            }
            result?;
            if let Some(error) = cleanup_error {
                return Err(error);
            }
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires writable Linux knfsd and FAS2750 NFSv4.1 fixtures"]
async fn nfs_v41_dacl_and_sacl_round_trip_on_linux_and_fas2750() -> TestResult {
    let urls = env::var("NFS_RS_LAB_ACL_V41_URLS")?;
    for (index, url) in urls.split(',').filter(|url| !url.is_empty()).enumerate() {
        ensure(
            url.contains("version=4.1"),
            format!("DACL/SACL fixture is not exact NFSv4.1: {url}"),
        )?;
        let mount = parse_url_and_mount(url).await?;
        let directory = format!("nfs-rs-v41-acl41-{index}");
        let filename = format!("{directory}/file");
        let _ = mount.rmdir_path(&directory).await;
        let directory_object = mount.mkdir_path(&directory, 0o700).await?;
        let file_object = mount.create_path(&filename, Some(0o600)).await?;
        let result = async {
            for (kind, fh) in [
                ("directory", directory_object.fh.clone()),
                ("file", file_object.fh.clone()),
            ] {
                match mount.getdacl(fh.clone()).await {
                    Ok(dacl) => {
                        let replacement = NfsAcl41 {
                            flags: Acl41Flags(dacl.flags.0 ^ Acl41Flags::PROTECTED),
                            aces: dacl.aces.clone(),
                        };
                        mount.setdacl(fh.clone(), &replacement).await?;
                        let replacement_readback = mount.getdacl(fh.clone()).await;
                        let restore = async {
                            mount.setdacl(fh.clone(), &dacl).await?;
                            ensure(
                                mount.getdacl(fh.clone()).await? == dacl,
                                format!("{kind} DACL restore did not stabilize through {url}"),
                            )
                        }
                        .await;
                        ensure(
                            replacement_readback? == replacement,
                            format!("{kind} DACL full replacement failed through {url}"),
                        )?;
                        restore?;
                        println!("NFSV41_DACL=SUPPORTED kind={kind} url={url}");
                    }
                    Err(NfsError::Unsupported(_)) => {
                        ensure(
                            matches!(
                                mount.setdacl(fh.clone(), &NfsAcl41::default()).await,
                                Err(NfsError::Unsupported(_))
                            ),
                            format!(
                                "{kind} DACL SET did not preserve unsupported capability through {url}"
                            ),
                        )?;
                        println!("NFSV41_DACL=UNSUPPORTED kind={kind} url={url}");
                    }
                    Err(error) => return Err(error.into()),
                }
                match mount.getsacl(fh.clone()).await {
                    Ok(sacl) => {
                        let replacement = NfsAcl41 {
                            flags: Acl41Flags(sacl.flags.0 ^ Acl41Flags::DEFAULTED),
                            aces: sacl.aces.clone(),
                        };
                        mount.setsacl(fh.clone(), &replacement).await?;
                        let replacement_readback = mount.getsacl(fh.clone()).await;
                        let restore = async {
                            mount.setsacl(fh.clone(), &sacl).await?;
                            ensure(
                                mount.getsacl(fh.clone()).await? == sacl,
                                format!("{kind} SACL restore did not stabilize through {url}"),
                            )
                        }
                        .await;
                        ensure(
                            replacement_readback? == replacement,
                            format!("{kind} SACL full replacement failed through {url}"),
                        )?;
                        restore?;
                        println!("NFSV41_SACL=SUPPORTED kind={kind} url={url}");
                    }
                    Err(NfsError::Unsupported(_)) => {
                        ensure(
                            matches!(
                                mount.setsacl(fh, &NfsAcl41::default()).await,
                                Err(NfsError::Unsupported(_))
                            ),
                            format!(
                                "{kind} SACL SET did not preserve unsupported capability through {url}"
                            ),
                        )?;
                        println!("NFSV41_SACL=UNSUPPORTED kind={kind} url={url}");
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }
        .await;
        let close = mount.close(file_object.fh).await;
        let remove = mount.remove_path(&filename).await;
        let cleanup = mount.rmdir_path(&directory).await;
        let unmount = mount.umount().await;
        result?;
        close?;
        remove?;
        cleanup?;
        unmount?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the writable NetApp NFSv4.0 delegation fixture"]
async fn nfs_v40_delegation_recall_across_both_lifs() -> TestResult {
    let urls = env::var(LAB_V40_URLS_ENV)?
        .split(',')
        .filter(|url| !url.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    ensure(
        urls.len() == 2,
        "NFSv4.0 delegation validation requires exactly two LIF URLs",
    )?;
    let filename = env::var("NFS_RS_LAB_V40_DELEGATION_FILE")
        .unwrap_or_else(|_| "nfs-rs-v40-delegation.bin".to_string());
    let mut granted = 0usize;

    for primary in 0..urls.len() {
        let retention_url = format!("{}&retain-delegations=true", urls[primary]);
        let contender_url = &urls[1 - primary];
        let mount = parse_url_and_mount(&retention_url).await?;
        let contender = parse_url_and_mount(contender_url).await?;
        ensure(
            mount.capabilities().delegation_retention,
            "NFSv4.0 delegation retention was not enabled",
        )?;
        ensure(
            mount.health().callback_healthy == Some(true),
            format!("callback listener is not healthy: {:?}", mount.health()),
        )?;
        let _ = contender.remove_path(&filename).await;
        let created = mount.create_path(&filename, Some(0o600)).await?;
        mount.close(created.fh).await?;
        let delegated = mount.open_path(&filename, OPEN_BOTH).await?;
        let granted_here = mount.callback_stats().await.grants_received > 0;
        let conflicting = contender.open_path(&filename, OPEN_BOTH).await?;

        let outcome = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let stats = mount.callback_stats().await;
                if stats.recalls_received > 0 {
                    break stats;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        match outcome {
            Ok(_) => {
                granted += 1;
                let stats = tokio::time::timeout(Duration::from_secs(30), async {
                    loop {
                        let stats = mount.callback_stats().await;
                        if stats.returns_completed + stats.returns_failed > 0 {
                            break stats;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .map_err(|_| io::Error::other("delegation return did not settle"))?;
                ensure(
                    stats.returns_completed > 0 && stats.returns_failed == 0,
                    format!("delegation return failed through {retention_url}: {stats:?}"),
                )?;
                println!("NFS40_DELEGATION_OUTCOME=GRANTED_RECALLED_RETURNED url={retention_url}");
            }
            Err(_) if granted_here => {
                return Err(io::Error::other(format!(
                    "NFS40_DELEGATION_OUTCOME=GRANTED_CALLBACK_FAILED url={retention_url}"
                ))
                .into());
            }
            Err(_) => println!("NFS40_DELEGATION_OUTCOME=SKIP_NOT_GRANTED url={retention_url}"),
        }

        let expected = Bytes::from_static(b"nfs-rs-v40-delegation-checksum");
        write_all(mount.as_ref(), delegated.fh.clone(), &expected).await?;
        mount.close(delegated.fh).await?;
        contender.close(conflicting.fh).await?;
        let reopened = contender.open_path(&filename, OPEN_READ).await?;
        let actual = read_all(contender.as_ref(), reopened.fh.clone(), expected.len()).await?;
        contender.close(reopened.fh).await?;
        ensure(actual == expected, "post-delegation checksum mismatch")?;
        contender.remove_path(&filename).await?;
        mount.umount().await?;
        contender.umount().await?;
    }

    if granted == 0 {
        println!("NFS40_DELEGATION_SUMMARY=SKIP_NOT_GRANTED");
    } else {
        println!("NFS40_DELEGATION_SUMMARY=PASS_GRANTED count={granted}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a runner-scoped inbound NFSv4.0 callback fault"]
async fn nfs_v40_unreachable_callback_preserves_base_io() -> TestResult {
    let urls = env::var(LAB_V40_URLS_ENV)?
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>();
    ensure(urls.len() == 2, "callback fault test requires two LIF URLs")?;
    let primary = env::var("NFS_RS_LAB_V40_FAULT_PRIMARY")?.parse::<usize>()?;
    ensure(primary < 2, "invalid callback fault primary index")?;
    let mount = parse_url_and_mount(&format!("{}&retain-delegations=true", urls[primary])).await?;
    let contender = parse_url_and_mount(&urls[1 - primary]).await?;
    let filename = format!("nfs-rs-v40-callback-fault-{primary}.bin");
    let _ = contender.remove_path(&filename).await;
    let created = mount.create_path(&filename, Some(0o600)).await?;
    mount.close(created.fh).await?;
    let delegated = mount.open_path(&filename, OPEN_BOTH).await?;
    if let (Ok(armed), Ok(applied)) = (
        env::var("NFS_RS_LAB_V40_CALLBACK_FAULT_ARMED"),
        env::var("NFS_RS_LAB_V40_CALLBACK_FAULT_APPLIED"),
    ) {
        std::fs::write(armed, b"delegation outcome observable")?;
        tokio::time::timeout(Duration::from_secs(45), async {
            while !std::path::Path::new(&applied).exists() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| io::Error::other("callback fault was not applied"))?;
    }
    if mount.callback_stats().await.grants_received == 0 {
        println!(
            "NFS40_CALLBACK_FAULT_OUTCOME=SKIP_NOT_GRANTED url={}",
            urls[primary]
        );
    } else {
        ensure(
            mount.callback_stats().await.recalls_received == 0,
            "callback fault admitted an inbound recall",
        )?;
        println!(
            "NFS40_CALLBACK_FAULT_OUTCOME=GRANTED_CALLBACK_UNREACHABLE url={}",
            urls[primary]
        );
        if let Ok(trigger) = env::var("NFS_RS_LAB_V40_CALLBACK_FAULT_TRIGGER") {
            std::fs::write(trigger, b"trigger recall")?;
            let conflicting = tokio::time::timeout(
                Duration::from_secs(90),
                contender.open_path(&filename, OPEN_BOTH),
            )
            .await
            .map_err(|_| {
                io::Error::other("conflicting OPEN did not recover after callback fault")
            })??;
            contender.close(conflicting.fh).await?;
            ensure(
                mount.callback_stats().await.recalls_received > 0,
                "restored callback did not deliver the pending recall",
            )?;
        }
    }
    let expected = Bytes::from_static(b"ordinary-io-survives-callback-loss");
    write_all(mount.as_ref(), delegated.fh.clone(), &expected).await?;
    mount.close(delegated.fh).await?;
    if let (Ok(ready), Ok(restored)) = (
        env::var("NFS_RS_LAB_V40_CALLBACK_FAULT_READY"),
        env::var("NFS_RS_LAB_V40_CALLBACK_FAULT_RESTORED"),
    ) {
        std::fs::write(ready, b"callback fault observed")?;
        tokio::time::timeout(Duration::from_secs(45), async {
            while !std::path::Path::new(&restored).exists() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| io::Error::other("callback fault was not restored"))?;
    }
    let reopened = contender.open_path(&filename, OPEN_READ).await?;
    let actual = read_all(contender.as_ref(), reopened.fh.clone(), expected.len()).await?;
    contender.close(reopened.fh).await?;
    ensure(actual == expected, "callback loss corrupted ordinary I/O")?;
    contender.remove_path(&filename).await?;
    contender.umount().await?;
    mount.umount().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the writable NetApp NFSv4.0 reference fixture"]
async fn nfs_v40_open_io_commit_close_on_both_lifs() -> TestResult {
    let urls = env::var(LAB_V40_URLS_ENV)?;
    let small_file =
        env::var("NFS_RS_LAB_V40_SMALL_FILE").unwrap_or_else(|_| "nfs-rs-small.bin".to_string());
    let large_file =
        env::var("NFS_RS_LAB_V40_LARGE_FILE").unwrap_or_else(|_| "nfs-rs-large.bin".to_string());
    let small = Bytes::from(
        (0..64)
            .map(|index| ((index * 13 + 7) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let large = Bytes::from(
        (0..(4 * 1024 * 1024 + 37))
            .map(|index| ((index * 17 + 29) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    for url in urls.split(',').filter(|url| !url.is_empty()) {
        let mount = parse_url_and_mount(url).await?;
        let fsinfo = mount.fsinfo().await?;
        ensure(
            fsinfo.rtmax > 0 && fsinfo.wtmax > 0,
            format!("NFSv4.0 FSINFO returned zero I/O limits through {url}"),
        )?;
        let fsstat = mount.fsstat().await?;
        ensure(
            fsstat.tbytes >= fsstat.fbytes && fsstat.fbytes >= fsstat.abytes,
            format!("NFSv4.0 FSSTAT byte counters are inconsistent through {url}"),
        )?;
        match mount.pathconf(mount.getfh().await).await {
            Ok(pathconf) => ensure(
                pathconf.name_max > 0 && pathconf.linkmax > 0,
                format!("NFSv4.0 PATHCONF returned zero limits through {url}"),
            )?,
            Err(NfsError::Unsupported(_)) => {}
            Err(error) => return Err(error.into()),
        }
        let namespace_dir = "nfs-rs-v40-namespace-dir";
        let _ = mount.rmdir_path(namespace_dir).await;
        let created_dir = mount.mkdir_path(namespace_dir, 0o700).await?;
        ensure(
            created_dir
                .attr
                .as_ref()
                .is_some_and(|attr| attr.file_mode == 0o700),
            format!("NFSv4.0 MKDIR mode mismatch through {url}"),
        )?;
        ensure(
            mount.lookup_path(namespace_dir).await?.fh == created_dir.fh,
            format!("NFSv4.0 MKDIR lookup mismatch through {url}"),
        )?;
        mount.rmdir_path(namespace_dir).await?;
        ensure(
            mount.lookup_path(namespace_dir).await.is_err(),
            format!("NFSv4.0 RMDIR left a residual directory through {url}"),
        )?;
        let namespace_file = "nfs-rs-v40-created.bin";
        let _ = mount.remove_path(namespace_file).await;
        let created_file = mount.create_path(namespace_file, Some(0o600)).await?;
        ensure(
            created_file
                .attr
                .as_ref()
                .is_some_and(|attr| attr.file_mode == 0o600),
            format!("NFSv4.0 CREATE mode mismatch through {url}"),
        )?;
        let timestamp = Time {
            seconds: 1_700_000_000,
            nseconds: 123_000_000,
        };
        mount
            .setattr(
                created_file.fh.clone(),
                None,
                None,
                Some(0),
                Some(0),
                None,
                Some(timestamp),
                Some(timestamp),
            )
            .await?;
        let metadata = mount.getattr(created_file.fh.clone()).await?;
        ensure(
            metadata.uid == 0
                && metadata.gid == 0
                && metadata.atime == timestamp
                && metadata.mtime == timestamp,
            format!("NFSv4.0 owner/timestamp SETATTR mismatch through {url}: {metadata:?}"),
        )?;
        let acl_support = mount.aclsupport(created_file.fh.clone()).await?;
        ensure(
            acl_support.supports(nfs_rs::AclSupport::ALLOW),
            format!("NFSv4.0 ACLSUPPORT omitted ALLOW through {url}"),
        )?;
        match mount.getacl(created_file.fh.clone()).await {
            Ok(original_acl) => match mount.setacl(created_file.fh.clone(), &original_acl).await {
                Ok(()) => ensure(
                    mount.getacl(created_file.fh.clone()).await? == original_acl,
                    format!("NFSv4.0 ACL round trip mismatch through {url}"),
                )?,
                Err(NfsError::Unsupported(_)) => ensure(
                    mount.capabilities().acl
                        && mount
                            .aclsupport(created_file.fh.clone())
                            .await?
                            .supports(nfs_rs::AclSupport::ALLOW),
                    format!(
                        "NFSv4.0 SETACL rejection incorrectly disabled readable ACL capability through {url}"
                    ),
                )?,
                Err(error) => return Err(error.into()),
            },
            Err(NfsError::Unsupported(_)) => ensure(
                mount.capabilities().acl
                    && mount
                        .aclsupport(created_file.fh.clone())
                        .await?
                        .supports(nfs_rs::AclSupport::ALLOW),
                format!("NFSv4.0 GETACL rejection incorrectly changed ACLSUPPORT through {url}"),
            )?,
            Err(error) => return Err(error.into()),
        }
        ensure(
            !mount.capabilities().named_attributes
                && matches!(
                    mount.listxattr(created_file.fh.clone()).await,
                    Err(NfsError::Unsupported(_))
                ),
            format!("NFSv4.0 named attributes were not explicitly unsupported through {url}"),
        )?;
        let names = mount
            .readdir(mount.getfh().await)
            .await
            .map_ok(|entry| entry.file_name)
            .try_collect::<Vec<_>>()
            .await?;
        ensure(
            names.iter().any(|name| name == namespace_file),
            format!("NFSv4.0 READDIR omitted {namespace_file} through {url}"),
        )?;
        let entries = mount
            .readdirplus(mount.getfh().await)
            .await
            .try_collect::<Vec<_>>()
            .await?;
        ensure(
            entries.iter().any(|entry| {
                entry.file_name == namespace_file
                    && entry.attr.is_some()
                    && !entry.handle.is_empty()
            }),
            format!("NFSv4.0 READDIRPLUS omitted detailed {namespace_file} through {url}"),
        )?;
        let created_payload = Bytes::from_static(b"created-through-common-mount-api");
        ensure(
            mount
                .write_stable(created_file.fh.clone(), 0, created_payload.clone())
                .await?
                == created_payload.len() as u32,
            format!("NFSv4.0 CREATE write count mismatch through {url}"),
        )?;
        mount.close(created_file.fh).await?;
        let reopened = mount.open_path(namespace_file, OPEN_READ).await?;
        ensure(
            mount
                .read(reopened.fh.clone(), 0, created_payload.len() as u32)
                .await?
                == created_payload,
            format!("NFSv4.0 CREATE payload mismatch through {url}"),
        )?;
        mount.close(reopened.fh).await?;
        let renamed_file = "nfs-rs-v40-renamed.bin";
        let hardlink_file = "nfs-rs-v40-hardlink.bin";
        let symlink_file = "nfs-rs-v40-symlink";
        for residual in [renamed_file, hardlink_file, symlink_file] {
            let _ = mount.remove_path(residual).await;
        }
        mount.rename_path(namespace_file, renamed_file).await?;
        ensure(
            mount.lookup_path(namespace_file).await.is_err(),
            format!("NFSv4.0 RENAME left the source through {url}"),
        )?;
        let renamed = mount.lookup_path(renamed_file).await?;
        let linked_attr = mount.link_path(renamed_file, hardlink_file).await?;
        ensure(
            linked_attr.nlink >= 2 && mount.lookup_path(hardlink_file).await?.fh == renamed.fh,
            format!("NFSv4.0 LINK identity mismatch through {url}"),
        )?;
        let symbolic = mount.symlink_path(renamed_file, symlink_file).await?;
        ensure(
            mount.readlink(symbolic.fh).await? == renamed_file,
            format!("NFSv4.0 READLINK target mismatch through {url}"),
        )?;
        mount.remove_path(symlink_file).await?;
        mount.remove_path(hardlink_file).await?;
        mount.remove_path(renamed_file).await?;
        ensure(
            mount.lookup_path(renamed_file).await.is_err(),
            format!("NFSv4.0 REMOVE left a residual file through {url}"),
        )?;
        let small_open = mount.open_path_stateful(&small_file, OPEN_BOTH).await?;
        let large_open = mount.open_path_stateful(&large_file, OPEN_BOTH).await?;
        let (small_lock, large_lock) = tokio::try_join!(
            mount.lock_open_stateful(&small_open, 2, 0, 1),
            mount.lock_open_stateful(&large_open, 2, 0, 1),
        )?;
        mount.unlock_stateful(small_lock).await?;
        mount.unlock_stateful(large_lock).await?;
        mount.close_stateful(small_open).await?;
        mount.close_stateful(large_open).await?;
        for (file, expected) in [(&small_file, &small), (&large_file, &large)] {
            let opened = mount.open_path_stateful(file, OPEN_BOTH).await?;
            let fh = opened.object.fh.clone();
            let lock = mount
                .lock_stateful(fh.clone(), 2, 0, expected.len() as u64)
                .await?;
            let conflict = mount
                .lock_test(fh.clone(), 2, 0, expected.len() as u64)
                .await;
            ensure(
                matches!(conflict, Err(NfsError::LockDenied { .. })),
                format!("NFSv4.0 LOCKT did not report the held range through {url}: {conflict:?}"),
            )?;
            mount.unlock_stateful(lock).await?;
            mount
                .lock_test(fh.clone(), 2, 0, expected.len() as u64)
                .await?;
            let attr = mount.getattr(fh.clone()).await?;
            ensure(
                attr.filesize == expected.len() as u64,
                format!("NFSv4.0 GETATTR size mismatch for {file} through {url}"),
            )?;
            let granted = mount.access(fh.clone(), 0x01).await?;
            ensure(
                granted & 0x01 != 0,
                format!("NFSv4.0 ACCESS denied fixture read through {url}"),
            )?;
            let original_mode = attr.file_mode;
            let test_mode = if original_mode & 0o100 != 0 {
                original_mode & !0o100
            } else {
                original_mode | 0o100
            };
            mount
                .setattr(
                    fh.clone(),
                    None,
                    Some(test_mode),
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await?;
            ensure(
                mount.getattr(fh.clone()).await?.file_mode == test_mode,
                format!("NFSv4.0 SETATTR mode mismatch through {url}"),
            )?;
            mount
                .setattr(
                    fh.clone(),
                    None,
                    Some(original_mode),
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await?;
            let original = read_all(mount.as_ref(), fh.clone(), expected.len()).await?;
            ensure(
                original.len() == expected.len(),
                format!("NFSv4.0 fixture {file} has the wrong bounded size"),
            )?;
            let mut written = 0usize;
            while written < expected.len() {
                let end = (written + mount.get_max_write_size() as usize).min(expected.len());
                let count = mount
                    .write_stable(fh.clone(), written as u64, expected.slice(written..end))
                    .await? as usize;
                ensure(
                    count != 0 && count <= end - written,
                    "invalid NFSv4.0 WRITE count",
                )?;
                written += count;
            }
            mount.commit(fh.clone(), 0, written as u32).await?;
            let actual = read_all(mount.as_ref(), fh, expected.len()).await?;
            ensure(
                actual == *expected,
                format!("NFSv4.0 payload mismatch through {url}"),
            )?;
            let mut restored = 0usize;
            while restored < original.len() {
                let end = (restored + mount.get_max_write_size() as usize).min(original.len());
                restored += mount
                    .write_stable(
                        opened.object.fh.clone(),
                        restored as u64,
                        original.slice(restored..end),
                    )
                    .await? as usize;
            }
            mount
                .commit(opened.object.fh.clone(), 0, restored as u32)
                .await?;
            mount.close_stateful(opened).await?;
        }
        mount.umount().await?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct V40PerfWorkload {
    name: &'static str,
    file_count: usize,
    payload_size: usize,
    tasks: usize,
}

const V40_PERF_WORKLOADS: [V40PerfWorkload; 4] = [
    V40PerfWorkload {
        name: "small-single",
        file_count: 128,
        payload_size: 4 * 1024,
        tasks: 1,
    },
    V40PerfWorkload {
        name: "small-multi",
        file_count: 128,
        payload_size: 4 * 1024,
        tasks: 4,
    },
    V40PerfWorkload {
        name: "large-single",
        file_count: 4,
        payload_size: 16 * 1024 * 1024,
        tasks: 1,
    },
    V40PerfWorkload {
        name: "large-multi",
        file_count: 4,
        payload_size: 16 * 1024 * 1024,
        tasks: 4,
    },
];

async fn run_v40_performance_task(
    url: String,
    run_id: String,
    workload: V40PerfWorkload,
    task: usize,
) -> TestResult<(u64, Vec<f64>, Vec<f64>, u32, u32)> {
    let mount = parse_url_and_mount(&url).await?;
    let negotiated_read_size = mount.get_max_read_size();
    let negotiated_write_size = mount.get_max_write_size();
    let write_chunk_size = negotiated_io_chunk_size(negotiated_write_size)?;
    let payload = Bytes::from(
        (0..workload.payload_size)
            .map(|index| ((index * 17 + task * 13 + 29) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let mut write_latencies = Vec::new();
    let mut workload_latencies = Vec::new();
    let mut transferred = 0u64;
    for file in 0..workload.file_count {
        let workload_started = Instant::now();
        let name = format!("nfsrs-perf-{run_id}-{}-{task}-{file}.bin", workload.name);
        let _ = mount.remove_path(&name).await;
        let created = mount.create_path(&name, Some(0o600)).await?;
        let started = Instant::now();
        if workload.payload_size <= write_chunk_size {
            write_all_with_chunk_size(
                mount.as_ref(),
                created.fh.clone(),
                &payload,
                write_chunk_size,
            )
            .await?;
            write_latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        } else {
            for offset in (0..payload.len()).step_by(write_chunk_size) {
                let end = (offset + write_chunk_size).min(payload.len());
                let chunk = payload.slice(offset..end);
                let chunk_started = Instant::now();
                let written = mount
                    .write_stable(created.fh.clone(), offset as u64, chunk.clone())
                    .await?;
                ensure(written as usize == chunk.len(), "short performance write")?;
                write_latencies.push(chunk_started.elapsed().as_secs_f64() * 1_000.0);
            }
        }
        mount
            .commit(created.fh.clone(), 0, payload.len() as u32)
            .await?;
        mount.close(created.fh).await?;
        let opened = mount.open_path(&name, OPEN_READ).await?;
        let actual = read_all(mount.as_ref(), opened.fh.clone(), payload.len()).await?;
        mount.close(opened.fh).await?;
        ensure(actual == payload, "performance payload checksum mismatch")?;
        mount.remove_path(&name).await?;
        workload_latencies.push(workload_started.elapsed().as_secs_f64() * 1_000.0);
        transferred += payload.len() as u64;
    }
    mount.umount().await?;
    Ok((
        transferred,
        write_latencies,
        workload_latencies,
        negotiated_read_size,
        negotiated_write_size,
    ))
}

fn peak_rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmHWM:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .unwrap_or(0)
}

fn percentile_95(samples: &mut [f64]) -> f64 {
    samples.sort_by(f64::total_cmp);
    let index = ((samples.len() as f64 * 0.95).ceil() as usize)
        .saturating_sub(1)
        .min(samples.len().saturating_sub(1));
    samples[index]
}

async fn measure_v40_performance_workload(
    urls: &[String],
    run_id: &str,
    workload: V40PerfWorkload,
) -> TestResult<serde_json::Value> {
    let mut throughput_samples = Vec::new();
    let mut write_p95_samples = Vec::new();
    let mut workload_p95_samples = Vec::new();
    let mut bytes_per_sample = 0u64;
    let mut negotiated_read_sizes = BTreeSet::new();
    let mut negotiated_write_sizes = BTreeSet::new();
    for _ in 0..3 {
        let started = Instant::now();
        let mut joins = tokio::task::JoinSet::new();
        for task in 0..workload.tasks {
            joins.spawn(run_v40_performance_task(
                urls[task % urls.len()].clone(),
                run_id.to_string(),
                workload,
                task,
            ));
        }
        let mut write_latencies = Vec::new();
        let mut workload_latencies = Vec::new();
        bytes_per_sample = 0;
        while let Some(joined) = joins.join_next().await {
            let (task_bytes, mut task_write, mut task_workload, task_read_size, task_write_size) =
                match joined {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => {
                        joins.abort_all();
                        while joins.join_next().await.is_some() {}
                        return Err(error);
                    }
                    Err(error) => {
                        joins.abort_all();
                        while joins.join_next().await.is_some() {}
                        return Err(error.into());
                    }
                };
            bytes_per_sample += task_bytes;
            write_latencies.append(&mut task_write);
            workload_latencies.append(&mut task_workload);
            negotiated_read_sizes.insert(task_read_size);
            negotiated_write_sizes.insert(task_write_size);
        }
        throughput_samples
            .push(bytes_per_sample as f64 / 1_048_576.0 / started.elapsed().as_secs_f64());
        write_p95_samples.push(percentile_95(&mut write_latencies));
        workload_p95_samples.push(percentile_95(&mut workload_latencies));
    }
    throughput_samples.sort_by(f64::total_cmp);
    write_p95_samples.sort_by(f64::total_cmp);
    workload_p95_samples.sort_by(f64::total_cmp);
    Ok(serde_json::json!({
        "name": workload.name,
        "throughput_mib_s": throughput_samples[1],
        "write_p95_latency_ms": write_p95_samples[1],
        "workload_p95_latency_ms": workload_p95_samples[1],
        "peak_rss_kib": peak_rss_kib(),
        "bytes_per_sample": bytes_per_sample,
        "samples": 3,
        "tasks": workload.tasks,
        "negotiated_read_sizes": negotiated_read_sizes,
        "negotiated_write_sizes": negotiated_write_sizes,
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "internal child of the NFSv4.0 performance matrix"]
async fn nfs_v40_performance_workload_child() -> TestResult {
    let selected = env::var("NFS_RS_LAB_V40_PERF_WORKLOAD")?;
    let workload = V40_PERF_WORKLOADS
        .iter()
        .copied()
        .find(|workload| workload.name == selected)
        .ok_or_else(|| io::Error::other("unknown NFSv4.0 performance workload"))?;
    let urls = env::var(LAB_V40_URLS_ENV)?
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>();
    ensure(urls.len() == 2, "performance matrix requires both FAS LIFs")?;
    let result =
        measure_v40_performance_workload(&urls, &env::var("NFS_RS_LAB_V40_PERF_RUN_ID")?, workload)
            .await?;
    std::fs::write(
        env::var("NFS_RS_LAB_V40_PERF_CHILD_OUTPUT")?,
        serde_json::to_vec_pretty(&result)?,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires the writable NetApp NFSv4.0 performance fixture"]
async fn nfs_v40_small_large_single_multi_performance() -> TestResult {
    let urls = env::var(LAB_V40_URLS_ENV)?
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>();
    ensure(urls.len() == 2, "performance matrix requires both FAS LIFs")?;
    let run_id = env::var("NFS_RS_LAB_V40_PERF_RUN_ID")?;
    let output = env::var("NFS_RS_LAB_V40_PERF_OUTPUT")?;
    let lifs = urls
        .iter()
        .map(|url| {
            url::Url::parse(url)?
                .host_str()
                .map(str::to_string)
                .ok_or_else(|| io::Error::other("NFSv4.0 performance URL lacks host").into())
        })
        .collect::<TestResult<Vec<_>>>()?;
    let mut results: Vec<serde_json::Value> = Vec::new();
    let executable = env::current_exe()?;
    for workload in V40_PERF_WORKLOADS {
        let child_output = env::temp_dir().join(format!(
            "nfsrs-v40-perf-{}-{}-{}.json",
            std::process::id(),
            run_id,
            workload.name
        ));
        let status = Command::new(&executable)
            .arg("nfs_v40_performance_workload_child")
            .arg("--ignored")
            .arg("--exact")
            .arg("--nocapture")
            .env("NFS_RS_LAB_V40_PERF_WORKLOAD", workload.name)
            .env("NFS_RS_LAB_V40_PERF_CHILD_OUTPUT", &child_output)
            .status()?;
        ensure(
            status.success(),
            format!("{} workload child failed", workload.name),
        )?;
        results.push(serde_json::from_slice(&std::fs::read(&child_output)?)?);
        std::fs::remove_file(child_output)?;
    }
    let report = serde_json::json!({
        "schema_version": 2,
        "run_id": run_id,
        "commit": env::var("NFS_RS_LAB_V40_PERF_COMMIT")?,
        "lifs": lifs,
        "protocol": "4.0",
        "liveness": "pass",
        "workloads": results,
    });
    std::fs::write(output, serde_json::to_vec_pretty(&report)?)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires writable NFSv4.0 reference fixtures"]
async fn nfs_v40_same_open_state_supports_concurrent_io() -> TestResult {
    let urls = env::var(LAB_V40_URLS_ENV)?
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>();
    ensure(!urls.is_empty(), "concurrent I/O validation requires a URL")?;
    let run_id = env::var("NFS_RS_LAB_V40_PERF_RUN_ID")
        .unwrap_or_else(|_| format!("e2e-{}", std::process::id()));

    for (server, url) in urls.into_iter().enumerate() {
        let mount = Arc::<dyn Mount>::from(parse_url_and_mount(&url).await?);
        let chunk_size =
            negotiated_io_chunk_size(mount.get_max_read_size().min(mount.get_max_write_size()))?;
        let chunk_count = 8usize;
        let payload = Bytes::from(
            (0..chunk_size * chunk_count)
                .map(|index| ((index * 31 + server * 17 + 11) % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let name = format!("nfsrs-concurrent-io-{run_id}-{server}.bin");
        let _ = mount.remove_path(&name).await;
        let created = mount.create_path(&name, Some(0o600)).await?;

        let mut writes = tokio::task::JoinSet::new();
        for chunk in 0..chunk_count {
            let mount = Arc::clone(&mount);
            let fh = created.fh.clone();
            let data = payload.slice(chunk * chunk_size..(chunk + 1) * chunk_size);
            writes.spawn(async move {
                let written = mount
                    .write_stable(fh, (chunk * chunk_size) as u64, data)
                    .await?;
                ensure(
                    written as usize == chunk_size,
                    format!("short concurrent WRITE for chunk {chunk}"),
                )
            });
        }
        while let Some(write) = writes.join_next().await {
            write??;
        }
        mount
            .commit(created.fh.clone(), 0, payload.len() as u32)
            .await?;
        mount.close(created.fh).await?;

        let opened = mount.open_path(&name, OPEN_READ).await?;
        let mut reads = tokio::task::JoinSet::new();
        for chunk in 0..chunk_count {
            let mount = Arc::clone(&mount);
            let fh = opened.fh.clone();
            reads.spawn(async move {
                let base = chunk * chunk_size;
                let mut data = Vec::with_capacity(chunk_size);
                while data.len() < chunk_size {
                    let part = mount
                        .read(
                            fh.clone(),
                            (base + data.len()) as u64,
                            (chunk_size - data.len()) as u32,
                        )
                        .await?;
                    ensure(
                        !part.is_empty(),
                        format!("unexpected EOF in concurrent READ chunk {chunk}"),
                    )?;
                    data.extend_from_slice(&part);
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((chunk, Bytes::from(data)))
            });
        }
        let mut actual = vec![Bytes::new(); chunk_count];
        while let Some(read) = reads.join_next().await {
            let (chunk, data) = read??;
            actual[chunk] = data;
        }
        mount.close(opened.fh).await?;
        ensure(
            actual.iter().enumerate().all(|(chunk, data)| {
                *data == payload.slice(chunk * chunk_size..(chunk + 1) * chunk_size)
            }),
            format!("concurrent I/O payload mismatch on server {server}"),
        )?;
        mount.remove_path(&name).await?;
        mount.umount().await?;
    }
    Ok(())
}

async fn measure_data_mover_same_file_sample(
    url: &str,
    run_id: &str,
    sample: usize,
    payload_mib: usize,
    read_depth: usize,
    write_depth: usize,
) -> TestResult<(f64, f64, u32, u32, usize, BTreeMap<usize, usize>, usize)> {
    let mount = Arc::<dyn Mount>::from(parse_url_and_mount(url).await?);
    let negotiated_read_size = mount.get_max_read_size();
    let negotiated_write_size = mount.get_max_write_size();
    let chunk_size = negotiated_io_chunk_size(negotiated_read_size.min(negotiated_write_size))?;
    let chunk_count = (payload_mib * 1024 * 1024 / chunk_size).max(1);
    let payload = Bytes::from(
        (0..chunk_size * chunk_count)
            .map(|index| ((index * 37 + sample * 19 + 7) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let name = format!("nfsrs-data-mover-perf-{run_id}-{read_depth}-{write_depth}-{sample}.bin");
    let _ = mount.remove_path(&name).await;

    let write_started = Instant::now();
    let created = mount.create_path(&name, Some(0o600)).await?;
    let mut writes = FuturesUnordered::new();
    for chunk in 0..chunk_count {
        let mount = Arc::clone(&mount);
        let fh = created.fh.clone();
        let data = payload.slice(chunk * chunk_size..(chunk + 1) * chunk_size);
        writes.push(async move {
            let written = mount
                .write_stable(fh, (chunk * chunk_size) as u64, data)
                .await?;
            ensure(
                written as usize == chunk_size,
                format!("short data-mover WRITE for chunk {chunk}"),
            )
        });
        if writes.len() >= write_depth {
            writes.next().await.expect("non-empty write window")?;
        }
    }
    while let Some(write) = writes.next().await {
        write?;
    }
    mount.close(created.fh).await?;
    let write_seconds = write_started.elapsed().as_secs_f64();

    let read_started = Instant::now();
    let opened = mount.open_path(&name, OPEN_READ).await?;
    let mut reads = FuturesOrdered::new();
    let mut next_chunk = 0usize;
    let mut verified_bytes = 0usize;
    let mut read_response_lengths = BTreeMap::new();
    let mut non_eof_short_reads = 0usize;
    loop {
        while reads.len() < read_depth && next_chunk < chunk_count {
            let mount = Arc::clone(&mount);
            let fh = opened.fh.clone();
            let chunk = next_chunk;
            reads.push_back(async move {
                let data = mount
                    .read(fh, (chunk * chunk_size) as u64, chunk_size as u32)
                    .await;
                (chunk, data)
            });
            next_chunk += 1;
        }
        let Some(read) = reads.next().await else {
            break;
        };
        let (chunk, first) = read;
        let range_start = chunk * chunk_size;
        let mut range_bytes = 0usize;
        let mut part = first?;
        loop {
            ensure(!part.is_empty(), "unexpected EOF in data-mover READ range")?;
            let requested = chunk_size - range_bytes;
            *read_response_lengths.entry(part.len()).or_insert(0) += 1;
            if part.len() < requested && range_start + range_bytes + part.len() < payload.len() {
                non_eof_short_reads += 1;
            }
            let part_end = range_start + range_bytes + part.len();
            ensure(
                part == payload.slice(range_start + range_bytes..part_end),
                "data-mover performance payload mismatch",
            )?;
            range_bytes += part.len();
            verified_bytes += part.len();
            if range_bytes >= chunk_size {
                break;
            }
            part = mount
                .read(
                    opened.fh.clone(),
                    (range_start + range_bytes) as u64,
                    (chunk_size - range_bytes) as u32,
                )
                .await?;
        }
    }
    mount.close(opened.fh).await?;
    let read_seconds = read_started.elapsed().as_secs_f64();
    ensure(
        verified_bytes == payload.len(),
        format!(
            "data-mover READ verified {verified_bytes} of {} bytes",
            payload.len()
        ),
    )?;
    mount.remove_path(&name).await?;
    mount.umount().await?;

    let mib = payload.len() as f64 / 1_048_576.0;
    Ok((
        mib / write_seconds,
        mib / read_seconds,
        negotiated_read_size,
        negotiated_write_size,
        chunk_size,
        read_response_lengths,
        non_eof_short_reads,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a writable NFSv4.0 performance fixture"]
async fn nfs_v40_data_mover_same_file_concurrency_performance() -> TestResult {
    let url = env::var(LAB_V40_URLS_ENV)?
        .split(',')
        .next()
        .filter(|url| !url.is_empty())
        .ok_or_else(|| io::Error::other("data-mover performance requires a URL"))?
        .to_string();
    let run_id = env::var("NFS_RS_LAB_V40_PERF_RUN_ID")?;
    let output = env::var("NFS_RS_LAB_V40_PERF_OUTPUT")?;
    let payload_mib = env::var("NFS_RS_LAB_V40_DATA_MOVER_PAYLOAD_MIB")
        .map_or(Ok(16), |value| value.parse::<usize>())?;
    let sample_count = env::var("NFS_RS_LAB_V40_DATA_MOVER_SAMPLES")
        .map_or(Ok(3), |value| value.parse::<usize>())?;
    ensure(payload_mib > 0, "data-mover payload must be non-zero")?;
    ensure(
        sample_count > 0 && sample_count % 2 == 1,
        "data-mover sample count must be a non-zero odd number",
    )?;
    let mut modes = Vec::new();
    let mut negotiated_read_sizes = BTreeSet::new();
    let mut negotiated_write_sizes = BTreeSet::new();
    let mut effective_block_sizes = BTreeSet::new();
    for (name, read_depth, write_depth) in [("sequential", 1, 1), ("data-mover", 4, 8)] {
        let mut write_samples = Vec::new();
        let mut read_samples = Vec::new();
        let mut read_response_lengths = BTreeMap::new();
        let mut non_eof_short_reads = 0usize;
        for sample in 0..sample_count {
            let (
                write,
                read,
                negotiated_read,
                negotiated_write,
                effective_block,
                sample_response_lengths,
                sample_short_reads,
            ) = measure_data_mover_same_file_sample(
                &url,
                &run_id,
                sample,
                payload_mib,
                read_depth,
                write_depth,
            )
            .await?;
            write_samples.push(write);
            read_samples.push(read);
            negotiated_read_sizes.insert(negotiated_read);
            negotiated_write_sizes.insert(negotiated_write);
            effective_block_sizes.insert(effective_block);
            for (length, count) in sample_response_lengths {
                *read_response_lengths.entry(length).or_insert(0) += count;
            }
            non_eof_short_reads += sample_short_reads;
        }
        write_samples.sort_by(f64::total_cmp);
        read_samples.sort_by(f64::total_cmp);
        modes.push(serde_json::json!({
            "name": name,
            "read_depth": read_depth,
            "write_depth": write_depth,
            "write_mib_s": write_samples[sample_count / 2],
            "read_mib_s": read_samples[sample_count / 2],
            "combined_mib_s": 2.0 * payload_mib as f64
                / (payload_mib as f64 / write_samples[sample_count / 2]
                    + payload_mib as f64 / read_samples[sample_count / 2]),
            "samples": sample_count,
            "read_response_length_histogram": read_response_lengths,
            "non_eof_short_reads": non_eof_short_reads,
        }));
    }
    std::fs::write(
        output,
        serde_json::to_vec_pretty(&serde_json::json!({
            "run_id": run_id,
            "url": url,
            "payload_mib": payload_mib,
            "negotiated_read_sizes": negotiated_read_sizes,
            "negotiated_write_sizes": negotiated_write_sizes,
            "effective_block_sizes": effective_block_sizes,
            "modes": modes,
            "peak_rss_kib": peak_rss_kib(),
        }))?,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an authorized runner-side NetApp NFSv4.0 partition"]
async fn nfs_v40_destination_partition_respects_lease_generation() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_V40_FAULT_URL")?;
    let mode = env::var("NFS_RS_LAB_V40_FAULT_MODE")?;
    let ready = env::var("NFS_RS_LAB_V40_FAULT_READY_FILE")?;
    let applied = env::var("NFS_RS_LAB_V40_FAULT_APPLIED_FILE")?;
    let restored = env::var("NFS_RS_LAB_V40_FAULT_RESTORED_FILE")?;
    let observed = env::var("NFS_RS_LAB_V40_FAULT_OBSERVED_FILE")?;
    let mount = parse_url_and_mount(&url).await?;
    ensure(mount.version() == NFSVersion::NFSv4p0, "NFSv4.0 required")?;
    let initial = mount.health();
    let lease_seconds = initial
        .lease_seconds
        .ok_or_else(|| io::Error::other("NFSv4.0 lease time is not observable"))?;
    ensure(lease_seconds > 0, "NFSv4.0 lease time is zero")?;
    let opened = mount
        .open_path_stateful("nfs-rs-small.bin", OPEN_READ)
        .await?;
    std::fs::write(&ready, lease_seconds.to_string())?;
    wait_for_lab_file(&applied, Duration::from_secs(30)).await?;

    match mode.as_str() {
        "below" => {
            wait_for_lab_file(
                &restored,
                Duration::from_secs(u64::from(lease_seconds) + 60),
            )
            .await?;
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let health = mount.health();
                    if health.lifecycle == MountLifecycleState::Ready
                        && health.lease_renewals > initial.lease_renewals
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .map_err(|_| io::Error::other("below-lease partition did not renew after restore"))?;
            ensure(
                mount.health().generation == initial.generation,
                "below-lease reconnect invalidated the generation",
            )?;
            mount.close_stateful(opened).await?;
        }
        "above" => {
            tokio::time::timeout(Duration::from_secs(u64::from(lease_seconds) + 60), async {
                loop {
                    if mount.health().lifecycle == MountLifecycleState::LostState {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .map_err(|_| io::Error::other("above-lease partition did not lose state"))?;
            ensure(
                mount.health().generation > initial.generation,
                "above-lease partition retained the old generation",
            )?;
            let error = mount.close_stateful(opened).await.unwrap_err();
            ensure(
                error
                    .operation_outcome()
                    .is_some_and(|outcome| outcome.recovery == nfs_rs::RecoveryAction::Reopen),
                format!("stale generation did not require reopen: {error}"),
            )?;
            std::fs::write(&observed, b"lost-state-old-token-rejected")?;
            wait_for_lab_file(&restored, Duration::from_secs(30)).await?;
        }
        _ => return Err(io::Error::other("invalid NFSv4.0 fault mode").into()),
    }
    mount.umount().await?;
    Ok(())
}

fn ensure(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message.into()).into())
    }
}

fn ensure_acl_structural_fidelity(source: &Acl, target: &Acl, context: &str) -> TestResult {
    if let Some(difference) = acl_structural_difference(source, target) {
        return ensure(false, format!("{context} {difference}"));
    }
    Ok(())
}

fn acl_structural_difference(source: &Acl, target: &Acl) -> Option<String> {
    if source.aces.len() != target.aces.len() {
        return Some(format!(
            "ACL ACE count differs: source={} target={}",
            source.aces.len(),
            target.aces.len()
        ));
    }
    for (index, (source_ace, target_ace)) in source.aces.iter().zip(&target.aces).enumerate() {
        if source_ace.ace_type != target_ace.ace_type {
            return Some(format!("ACE {index} type differs"));
        }
        if source_ace.flags != target_ace.flags {
            return Some(format!("ACE {index} flags differ"));
        }
        if source_ace.access_mask != target_ace.access_mask {
            return Some(format!("ACE {index} access mask differs"));
        }
        if source_ace.who != target_ace.who {
            return Some(format!("ACE {index} identity differs"));
        }
    }
    None
}

fn classify_acl_fidelity(
    source_label: &str,
    target_label: &str,
    kind: AclObjectKind,
    source: &Acl,
    target: &Acl,
) -> Result<String, String> {
    if acl_structural_difference(source, target).is_none() {
        return Ok("EXACT".to_string());
    }
    if source_label.starts_with("fas2750-")
        && target_label.starts_with("linux-")
        && source == &known_fas_to_linux_source(kind)
        && target == &known_linux_from_fas_target(kind)
    {
        let description = match kind {
            AclObjectKind::File | AclObjectKind::InheritedFile => {
                "FAS2750 file ACL to Linux mode-derived ACL"
            }
            AclObjectKind::Directory | AclObjectKind::InheritedDirectory => {
                "FAS2750 inheritable ACL to Linux access/default ACLs"
            }
        };
        return Ok(format!("NORMALIZED({description})"));
    }
    if source_label.starts_with("linux-")
        && target_label.starts_with("fas2750-")
        && source == &known_linux_normalized_acl(kind)
        && target == &known_fas_acl(kind)
    {
        let description = match kind {
            AclObjectKind::File | AclObjectKind::InheritedFile => {
                "Linux mode-derived ACL to FAS2750 file ACL"
            }
            AclObjectKind::Directory | AclObjectKind::InheritedDirectory => {
                "Linux access/default ACLs to FAS2750 inheritable ACL"
            }
        };
        return Ok(format!("NORMALIZED({description})"));
    }
    Err(format!(
        "unlisted ACL normalization from {source_label} to {target_label}: {}; source={source:?}; target={target:?}",
        acl_structural_difference(source, target)
            .unwrap_or_else(|| "unknown structural difference".to_string())
    ))
}

fn known_fas_acl(kind: AclObjectKind) -> Acl {
    match kind {
        AclObjectKind::File => acl_from_literals(&[
            (0, 1_179_784, "EVERYONE@"),
            (0, 1_966_495, "OWNER@"),
            (AceFlags::IDENTIFIER_GROUP, 1_179_785, "GROUP@"),
        ]),
        AclObjectKind::Directory => acl_from_literals(&[
            (
                AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT,
                1_179_784,
                "EVERYONE@",
            ),
            (0, 1_966_591, "OWNER@"),
            (AceFlags::IDENTIFIER_GROUP, 1_179_817, "GROUP@"),
        ]),
        AclObjectKind::InheritedFile => acl_from_literals(&[
            (0, 1_966_495, "OWNER@"),
            (0, 1_179_776, "GROUP@"),
            (0, 1_179_776, "EVERYONE@"),
        ]),
        AclObjectKind::InheritedDirectory => acl_from_literals(&[
            (
                AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT,
                1_966_591,
                "OWNER@",
            ),
            (
                AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT,
                1_179_776,
                "GROUP@",
            ),
            (
                AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT,
                1_179_776,
                "EVERYONE@",
            ),
        ]),
    }
}

fn known_fas_to_linux_source(kind: AclObjectKind) -> Acl {
    match kind {
        AclObjectKind::InheritedFile => acl_from_literals(&[
            (0, 1_179_784, "EVERYONE@"),
            (0, 1_966_495, "OWNER@"),
            (AceFlags::IDENTIFIER_GROUP, 1_179_776, "GROUP@"),
        ]),
        AclObjectKind::InheritedDirectory => acl_from_literals(&[
            (
                AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT,
                1_179_784,
                "EVERYONE@",
            ),
            (0, 1_966_591, "OWNER@"),
            (AceFlags::IDENTIFIER_GROUP, 1_179_776, "GROUP@"),
        ]),
        _ => known_fas_acl(kind),
    }
}

fn known_linux_from_fas_target(kind: AclObjectKind) -> Acl {
    match kind {
        AclObjectKind::InheritedDirectory => {
            let inherited =
                AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT | AceFlags::INHERIT_ONLY;
            acl_from_literals(&[
                (0, 1_442_279, "OWNER@"),
                (0, 1_179_776, "GROUP@"),
                (0, 1_179_776, "EVERYONE@"),
                (inherited, 1_442_279, "OWNER@"),
                (inherited, 1_179_809, "GROUP@"),
                (inherited, 1_179_776, "EVERYONE@"),
            ])
        }
        _ => known_linux_normalized_acl(kind),
    }
}

fn known_linux_normalized_acl(kind: AclObjectKind) -> Acl {
    match kind {
        AclObjectKind::File => acl_from_literals(&[
            (0, 1_442_183, "OWNER@"),
            (0, 1_179_777, "GROUP@"),
            (0, 1_179_776, "EVERYONE@"),
        ]),
        AclObjectKind::Directory => {
            let inherited =
                AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT | AceFlags::INHERIT_ONLY;
            acl_from_literals(&[
                (0, 1_442_279, "OWNER@"),
                (0, 1_179_809, "GROUP@"),
                (0, 1_179_776, "EVERYONE@"),
                (inherited, 1_442_279, "OWNER@"),
                (inherited, 1_179_809, "GROUP@"),
                (inherited, 1_179_776, "EVERYONE@"),
            ])
        }
        AclObjectKind::InheritedFile => acl_from_literals(&[
            (0, 1_442_183, "OWNER@"),
            (0, 1_179_776, "GROUP@"),
            (0, 1_179_776, "EVERYONE@"),
        ]),
        AclObjectKind::InheritedDirectory => {
            let inherited =
                AceFlags::FILE_INHERIT | AceFlags::DIRECTORY_INHERIT | AceFlags::INHERIT_ONLY;
            acl_from_literals(&[
                (0, 1_442_279, "OWNER@"),
                (0, 1_179_776, "GROUP@"),
                (0, 1_179_776, "EVERYONE@"),
                (inherited, 1_442_279, "OWNER@"),
                (inherited, 1_179_809, "GROUP@"),
                (inherited, 1_179_809, "EVERYONE@"),
            ])
        }
    }
}

fn acl_from_literals(entries: &[(u32, u32, &str)]) -> Acl {
    Acl {
        aces: entries
            .iter()
            .map(|(flags, mask, who)| NfsAce {
                ace_type: AceType::AccessAllowed,
                flags: AceFlags(*flags),
                access_mask: AceMask(*mask),
                who: (*who).to_string(),
            })
            .collect(),
    }
}

#[test]
fn acl_migration_rejects_unlisted_server_normalization() {
    let source = Acl {
        aces: vec![NfsAce {
            ace_type: AceType::AccessAllowed,
            flags: AceFlags(0),
            access_mask: AceMask(AceMask::READ_DATA),
            who: "EVERYONE@".to_string(),
        }],
    };
    let target = Acl {
        aces: vec![NfsAce {
            ace_type: AceType::AccessAllowed,
            flags: AceFlags(0),
            access_mask: AceMask(AceMask::WRITE_DATA),
            who: "EVERYONE@".to_string(),
        }],
    };

    assert!(
        classify_acl_fidelity(
            "fas2750-v40",
            "linux-v40",
            AclObjectKind::File,
            &source,
            &target
        )
        .is_err()
    );
}

fn payload() -> Bytes {
    let bytes = (0..(256 * 1024 + 37))
        .map(|index| ((index * 31 + 17) % 251) as u8)
        .collect::<Vec<_>>();
    Bytes::from(bytes)
}

fn validated_pnfs_run_id() -> TestResult<String> {
    let run_id = env::var("NFS_RS_LAB_PNFS_RUN_ID")?;
    ensure(
        run_id
            .strip_prefix("nightly-")
            .or_else(|| run_id.strip_prefix("release-"))
            .is_some_and(|suffix| {
                !suffix.is_empty()
                    && suffix.len() <= 80
                    && suffix
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
            }),
        "unsafe pNFS run ID",
    )?;
    Ok(run_id)
}

fn pnfs_payload_size() -> TestResult<usize> {
    match env::var("NFS_RS_LAB_PNFS_PAYLOAD_SIZE") {
        Ok(value) => {
            let size = value.parse::<usize>()?;
            ensure(size > 0, "pNFS payload size must be greater than zero")?;
            Ok(size)
        }
        Err(env::VarError::NotPresent) => Ok(PNFS_PAYLOAD_SIZE),
        Err(error) => Err(error.into()),
    }
}

fn pnfs_pattern(offset: usize, length: usize) -> Bytes {
    Bytes::from(
        (offset..offset + length)
            .map(|index| ((index * 29 + 43) % 251) as u8)
            .collect::<Vec<_>>(),
    )
}

async fn cleanup_pnfs_case(mount: &dyn Mount, case_dir: &str, file: &str) {
    let _ = mount.remove_path(file).await;
    let _ = mount.rmdir_path(case_dir).await;
}

async fn wait_for_lab_file(path: &str, timeout: Duration) -> TestResult {
    tokio::time::timeout(timeout, async {
        while !std::path::Path::new(path).exists() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| io::Error::other(format!("timed out waiting for lab signal {path}")))?;
    Ok(())
}

async fn recover_pnfs_file_from_checkpoint(
    url: &str,
    case_dir: &str,
    file: &str,
    expected: &Bytes,
) -> TestResult {
    let mount = parse_url_and_mount(url).await?;
    cleanup_pnfs_case(mount.as_ref(), case_dir, file).await;
    let result = async {
        mount.mkdir_path(case_dir, 0o700).await?;
        let created = mount.create_path(file, Some(0o600)).await?;
        write_all(mount.as_ref(), created.fh.clone(), expected).await?;
        mount.close(created.fh).await?;

        let opened = mount.open_path(file, OPEN_READ).await?;
        let actual = read_all(mount.as_ref(), opened.fh.clone(), expected.len()).await?;
        mount.close(opened.fh).await?;
        ensure(
            actual == *expected,
            "pNFS checkpoint recovery full-payload checksum mismatch",
        )
    }
    .await;

    cleanup_pnfs_case(mount.as_ref(), case_dir, file).await;
    let unmount_result = mount.umount().await;
    result?;
    unmount_result?;
    Ok(())
}

async fn write_all(mount: &dyn Mount, fh: Bytes, data: &Bytes) -> TestResult {
    let chunk_size = negotiated_io_chunk_size(mount.get_max_write_size())?;
    write_all_with_chunk_size(mount, fh, data, chunk_size).await
}

fn negotiated_io_chunk_size(server_max: u32) -> TestResult<usize> {
    ensure(server_max > 0, "server reported a zero maximum I/O size")?;
    Ok(server_max as usize)
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
            .write_stable(fh.clone(), offset as u64, data.slice(offset..end))
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
    let chunk_size = negotiated_io_chunk_size(mount.get_max_read_size())?;

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

#[test]
fn lab_io_chunk_uses_the_server_negotiated_limit_by_default() -> TestResult {
    ensure(
        negotiated_io_chunk_size(1024 * 1024)? == 1024 * 1024,
        "default lab I/O chunk did not use the negotiated server limit",
    )
}

#[test]
fn lab_io_chunk_rejects_a_zero_server_limit() {
    assert!(negotiated_io_chunk_size(0).is_err());
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
    let capabilities = mount.capabilities();
    let health = mount.health();
    ensure(
        health.lifecycle == MountLifecycleState::Ready,
        format!("{url}: newly mounted client is not ready: {health:?}"),
    )?;
    let callback_stats = mount.callback_stats().await;
    if expected_version == NFSVersion::NFSv4p1 {
        ensure(
            capabilities.locks && capabilities.callbacks && capabilities.session_diagnostics,
            format!("{url}: NFSv4.1 common capabilities are incomplete: {capabilities:?}"),
        )?;
        ensure(
            health.lease_healthy == Some(true) && health.callback_healthy.is_none(),
            format!("{url}: NFSv4.1 health is incomplete: {health:?}"),
        )?;
    } else {
        ensure(
            capabilities == Default::default(),
            format!("{url}: NFSv3 advertised stateful capabilities: {capabilities:?}"),
        )?;
        ensure(
            callback_stats == Default::default(),
            format!("{url}: NFSv3 reported callback activity: {callback_stats:?}"),
        )?;
    }

    cleanup_case(mount.as_ref()).await;
    let result = async {
        mount.null().await?;
        mount.fsinfo().await?;
        mount.fsstat().await?;
        mount.pathconf(mount.getfh().await).await?;

        mount.mkdir_path(CASE_DIR, 0o755).await?;
        let created = mount.create_path(ORIGINAL_FILE, Some(0o640)).await?;
        let expected = payload();
        if expected_version == NFSVersion::NFSv4p1 {
            ensure(
                capabilities.acl,
                format!("{url}: server negotiation omitted the NFSv4 ACL capability"),
            )?;
            let acl_support = mount.aclsupport(created.fh.clone()).await?;
            ensure(
                acl_support.supports(nfs_rs::AclSupport::ALLOW),
                format!("{url}: ACLSUPPORT omitted ALLOW"),
            )?;
            let original_acl = mount.getacl(created.fh.clone()).await?;
            mount.setacl(created.fh.clone(), &original_acl).await?;
            ensure(
                mount.getacl(created.fh.clone()).await? == original_acl,
                format!("{url}: NFSv4.1 ACL round trip mismatch"),
            )?;
            let lock = mount
                .lock_stateful(created.fh.clone(), 2, 0, expected.len() as u64)
                .await?;
            mount.unlock_stateful(lock).await?;
        }
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

        let opened = mount.open_path_stateful(ORIGINAL_FILE, OPEN_READ).await?;
        let actual = read_all(mount.as_ref(), opened.object.fh.clone(), expected.len()).await?;
        mount.close_stateful(opened).await?;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the NetApp NFSv4.1 file-layout pNFS lab"]
async fn nfs_v41_pnfs_write_uses_independent_ds() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_PNFS_URL")?;
    let run_id = validated_pnfs_run_id()?;
    let ready = env::var("NFS_RS_LAB_PNFS_READY_FILE")?;
    let completed = env::var("NFS_RS_LAB_PNFS_DONE_FILE")?;
    let case_dir = format!("nfs-rs-pnfs-{run_id}");
    let file = format!("{case_dir}/payload.bin");
    let mount = parse_url_and_mount(&url).await?;
    ensure(mount.version() == NFSVersion::NFSv4p1, "NFSv4.1 required")?;
    cleanup_pnfs_case(mount.as_ref(), &case_dir, &file).await;

    let result = async {
        mount.mkdir_path(&case_dir, 0o700).await?;
        let created = mount.create_path(&file, Some(0o600)).await?;
        let expected = Bytes::from(
            (0..pnfs_payload_size()?)
                .map(|index| ((index * 29 + 43) % 251) as u8)
                .collect::<Vec<_>>(),
        );
        write_all(mount.as_ref(), created.fh.clone(), &expected).await?;
        std::fs::write(&ready, b"pnfs-write-complete")?;
        tokio::time::timeout(Duration::from_secs(60), async {
            while !std::path::Path::new(&completed).exists() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| io::Error::other("independent pNFS DS connection was not observed"))?;
        mount.close(created.fh).await?;
        let opened = mount.open_path(&file, OPEN_READ).await?;
        let actual = read_all(mount.as_ref(), opened.fh.clone(), expected.len()).await?;
        mount.close(opened.fh).await?;
        ensure(actual == expected, "pNFS full-payload checksum mismatch")
    }
    .await;

    cleanup_pnfs_case(mount.as_ref(), &case_dir, &file).await;
    let unmount_result = mount.umount().await;
    result?;
    unmount_result?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a NetApp multi-node pNFS FlexGroup lab"]
async fn nfs_v41_pnfs_multifile_active_layout_refresh() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_PNFS_URL")?;
    let run_id = validated_pnfs_run_id()?;
    let case_dir = format!("nfs-rs-pnfs-{run_id}");
    let mount = parse_url_and_mount(&url).await?;
    let _ = mount.rmdir_path(&case_dir).await;

    let result = async {
        mount.mkdir_path(&case_dir, 0o700).await?;
        for index in 0..16usize {
            let file = format!("{case_dir}/refresh-{index:02}.bin");
            let created = mount.create_path(&file, Some(0o600)).await?;
            let head = pnfs_pattern(index * 64 * 1024, 64 * 1024);
            let tail = pnfs_pattern((index + 32) * 64 * 1024, 64 * 1024);
            mount
                .write_stable(created.fh.clone(), 0, head.clone())
                .await?;
            mount
                .write_stable(created.fh.clone(), 1024 * 1024 * 1024, tail.clone())
                .await?;
            mount.close(created.fh).await?;

            let opened = mount.open_path(&file, OPEN_READ).await?;
            ensure(
                mount.read(opened.fh.clone(), 0, head.len() as u32).await? == head,
                format!("active refresh head mismatch for file {index}"),
            )?;
            ensure(
                mount
                    .read(opened.fh.clone(), 1024 * 1024 * 1024, tail.len() as u32)
                    .await?
                    == tail,
                format!("active refresh tail mismatch for file {index}"),
            )?;
            mount.close(opened.fh).await?;
            mount.remove_path(&file).await?;
        }
        eprintln!("pnfs-active-refresh files=16 sparse-boundary=1GiB checksum=ok");
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    let _ = mount.rmdir_path(&case_dir).await;
    let unmount_result = mount.umount().await;
    result?;
    unmount_result?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the NetApp NFSv4.1 file-layout pNFS lab"]
async fn nfs_v41_pnfs_cleanup_run() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_PNFS_URL")?;
    let run_id = validated_pnfs_run_id()?;
    let case_dir = format!("nfs-rs-pnfs-{run_id}");
    let file = format!("{case_dir}/payload.bin");
    let mount = parse_url_and_mount(&url).await?;
    cleanup_pnfs_case(mount.as_ref(), &case_dir, &file).await;
    mount.umount().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a real NFSv4.1 server for negotiated channel limits"]
async fn nfs_v41_channel_limits_at_effective_bounds() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_CHANNEL_URL")?;
    let mount = parse_url_and_mount(&url).await?;
    ensure(mount.version() == NFSVersion::NFSv4p1, "NFSv4.1 required")?;
    let limits = mount
        .nfs41_channel_limits()
        .await
        .ok_or_else(|| io::Error::other("NFSv4.1 channel limits unavailable"))?;
    ensure(limits.max_request_size > 0, "zero ca_maxrequestsize")?;
    ensure(limits.max_response_size > 0, "zero ca_maxresponsesize")?;
    ensure(
        limits.max_cached_response_size > 0,
        "zero ca_maxresponsesize_cached",
    )?;
    ensure(limits.max_operations > 0, "zero ca_maxoperations")?;
    ensure(
        (1..=64).contains(&limits.max_requests),
        "ca_maxrequests outside offered range",
    )?;
    ensure(
        limits.effective_highest_slot_id < limits.max_requests,
        "effective highest slot exceeds negotiated table",
    )?;

    let concurrency = limits.effective_highest_slot_id as usize + 1;
    futures::future::try_join_all((0..concurrency).map(|_| mount.fsstat())).await?;
    eprintln!(
        "nfs41-channel-limits request={} response={} cached={} operations={} requests={} effective_slots={} concurrency={} status=ok",
        limits.max_request_size,
        limits.max_response_size,
        limits.max_cached_response_size,
        limits.max_operations,
        limits.max_requests,
        limits.effective_highest_slot_id + 1,
        concurrency,
    );
    mount.umount().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an authorized NetApp pNFS DS reset"]
async fn nfs_v41_pnfs_ds_reset_returns_uncertain() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_PNFS_URL")?;
    let run_id = validated_pnfs_run_id()?;
    let ready = env::var("NFS_RS_LAB_PNFS_READY_FILE")?;
    let applied = env::var("NFS_RS_LAB_PNFS_FAULT_APPLIED_FILE")?;
    let uncertain = env::var("NFS_RS_LAB_PNFS_UNCERTAIN_FILE")?;
    let restored = env::var("NFS_RS_LAB_PNFS_RESTORED_FILE")?;
    let case_dir = format!("nfs-rs-pnfs-{run_id}");
    let file = format!("{case_dir}/payload.bin");
    let mount = parse_url_and_mount(&url).await?;
    cleanup_pnfs_case(mount.as_ref(), &case_dir, &file).await;

    let result = async {
        mount.mkdir_path(&case_dir, 0o700).await?;
        let created = mount.create_path(&file, Some(0o600)).await?;
        let seed = Bytes::from(vec![0x3c; 512 * 1024]);
        write_all(mount.as_ref(), created.fh.clone(), &seed).await?;
        std::fs::write(&ready, b"pnfs-ds-session-ready")?;
        wait_for_lab_file(&applied, Duration::from_secs(120)).await?;

        let fault_payload = Bytes::from(vec![0xc3; 64 * 1024]);
        let error = mount
            .write_stable(created.fh.clone(), seed.len() as u64, fault_payload)
            .await
            .expect_err("DS reset after send must not fall back to a successful MDS WRITE");
        let outcome = error
            .operation_outcome()
            .ok_or_else(|| io::Error::other(format!("missing uncertain outcome: {error}")))?;
        ensure(
            outcome.outcome == nfs_rs::OperationOutcome::Uncertain,
            format!("unexpected pNFS fault outcome: {:?}", outcome.outcome),
        )?;
        ensure(
            outcome.recovery == nfs_rs::RecoveryAction::VerifyThenResume,
            format!("unexpected pNFS recovery action: {:?}", outcome.recovery),
        )?;
        std::fs::write(&uncertain, b"uncertain-no-mds-fallback")?;
        wait_for_lab_file(&restored, Duration::from_secs(120)).await?;
        let _ = mount.umount().await;

        let expected = Bytes::from(
            (0..(4 * 1024 * 1024 + 37))
                .map(|index| ((index * 19 + 61) % 251) as u8)
                .collect::<Vec<_>>(),
        );
        recover_pnfs_file_from_checkpoint(&url, &case_dir, &file, &expected).await
    }
    .await;

    let cleanup_mount = parse_url_and_mount(&url).await;
    if let Ok(cleanup_mount) = cleanup_mount {
        cleanup_pnfs_case(cleanup_mount.as_ref(), &case_dir, &file).await;
        let _ = cleanup_mount.umount().await;
    }
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an authorized NetApp pNFS DS preflight fault"]
async fn nfs_v41_pnfs_ds_unreachable_before_write_falls_back_to_mds() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_PNFS_URL")?;
    let run_id = validated_pnfs_run_id()?;
    let ready = env::var("NFS_RS_LAB_PNFS_READY_FILE")?;
    let applied = env::var("NFS_RS_LAB_PNFS_FAULT_APPLIED_FILE")?;
    let completed = env::var("NFS_RS_LAB_PNFS_DONE_FILE")?;
    let case_dir = format!("nfs-rs-pnfs-{run_id}");
    let file = format!("{case_dir}/preflight-fallback.bin");
    let mount = parse_url_and_mount(&url).await?;
    cleanup_pnfs_case(mount.as_ref(), &case_dir, &file).await;

    let result = async {
        mount.mkdir_path(&case_dir, 0o700).await?;
        let created = mount.create_path(&file, Some(0o600)).await?;
        std::fs::write(&ready, b"file-open-no-write-sent")?;
        wait_for_lab_file(&applied, Duration::from_secs(120)).await?;

        let expected = Bytes::from(
            (0..(1024 * 1024 + 37))
                .map(|index| ((index * 23 + 71) % 251) as u8)
                .collect::<Vec<_>>(),
        );
        write_all(mount.as_ref(), created.fh.clone(), &expected).await?;
        mount.close(created.fh).await?;
        let opened = mount.open_path(&file, OPEN_READ).await?;
        let actual = read_all(mount.as_ref(), opened.fh.clone(), expected.len()).await?;
        mount.close(opened.fh).await?;
        ensure(
            actual == expected,
            "pNFS preflight MDS fallback full-payload checksum mismatch",
        )?;
        std::fs::write(&completed, b"mds-fallback-checksum-ok")?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    cleanup_pnfs_case(mount.as_ref(), &case_dir, &file).await;
    let unmount_result = mount.umount().await;
    result?;
    unmount_result?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an authorized NetApp pNFS MDS LAYOUTCOMMIT fault"]
async fn nfs_v41_pnfs_layoutcommit_failure_retains_dirty_range() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_PNFS_URL")?;
    let run_id = validated_pnfs_run_id()?;
    let ready = env::var("NFS_RS_LAB_PNFS_READY_FILE")?;
    let applied = env::var("NFS_RS_LAB_PNFS_FAULT_APPLIED_FILE")?;
    let uncertain = env::var("NFS_RS_LAB_PNFS_UNCERTAIN_FILE")?;
    let restored = env::var("NFS_RS_LAB_PNFS_RESTORED_FILE")?;
    let case_dir = format!("nfs-rs-pnfs-{run_id}");
    let file = format!("{case_dir}/layoutcommit-retry.bin");
    let mount = parse_url_and_mount(&url).await?;
    cleanup_pnfs_case(mount.as_ref(), &case_dir, &file).await;

    let result = async {
        mount.mkdir_path(&case_dir, 0o700).await?;
        let created = mount.create_path(&file, Some(0o600)).await?;
        let expected = Bytes::from(
            (0..(1024 * 1024 + 37))
                .map(|index| ((index * 29 + 43) % 251) as u8)
                .collect::<Vec<_>>(),
        );
        write_all(mount.as_ref(), created.fh.clone(), &expected).await?;
        std::fs::write(&ready, b"ds-write-complete-layoutcommit-pending")?;
        wait_for_lab_file(&applied, Duration::from_secs(120)).await?;

        let error = mount
            .close(created.fh.clone())
            .await
            .expect_err("lost LAYOUTCOMMIT reply must fail CLOSE");
        let outcome = error
            .operation_outcome()
            .ok_or_else(|| io::Error::other(format!("missing uncertain outcome: {error}")))?;
        ensure(
            outcome.outcome == nfs_rs::OperationOutcome::Uncertain,
            format!("unexpected LAYOUTCOMMIT outcome: {:?}", outcome.outcome),
        )?;
        std::fs::write(&uncertain, b"layoutcommit-uncertain-dirty-retained")?;
        wait_for_lab_file(&restored, Duration::from_secs(120)).await?;

        // A full MDS isolation can invalidate the old connection/session. Follow
        // VerifyThenResume by reopening on a fresh mount and verifying the
        // authoritative file rather than retrying on a fenced slot table.
        let verify_mount = parse_url_and_mount(&url).await?;
        let opened = verify_mount.open_path(&file, OPEN_READ).await?;
        let actual = read_all(verify_mount.as_ref(), opened.fh.clone(), expected.len()).await?;
        verify_mount.close(opened.fh).await?;
        ensure(
            actual == expected,
            "pNFS LAYOUTCOMMIT recovery full-payload checksum mismatch",
        )?;
        cleanup_pnfs_case(verify_mount.as_ref(), &case_dir, &file).await;
        verify_mount.umount().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    cleanup_pnfs_case(mount.as_ref(), &case_dir, &file).await;
    // The original session was deliberately fenced by the MDS fault. Its
    // ordered LAYOUTRETURN may correctly remain uncertain; authoritative
    // recovery was verified above on a fresh mount.
    let _ = mount.umount().await;
    result?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a real NetApp CB_LAYOUTRECALL trigger"]
async fn nfs_v41_pnfs_layout_recall_during_write_and_close() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_PNFS_URL")?;
    let run_id = validated_pnfs_run_id()?;
    let ready = env::var("NFS_RS_LAB_PNFS_READY_FILE")?;
    let applied = env::var("NFS_RS_LAB_PNFS_FAULT_APPLIED_FILE")?;
    let case_dir = format!("nfs-rs-pnfs-{run_id}-recall");
    let file = format!("{case_dir}/recall-race.bin");
    let mount = parse_url_and_mount(&url).await?;
    let baseline_callbacks = mount
        .nfs41_callback_stats()
        .await
        .ok_or_else(|| io::Error::other("NFSv4.1 callback stats unavailable"))?;
    cleanup_pnfs_case(mount.as_ref(), &case_dir, &file).await;

    let result = async {
        mount.mkdir_path(&case_dir, 0o700).await?;
        let created = mount.create_path(&file, Some(0o600)).await?;
        let expected = Bytes::from(
            (0..(64 * 1024 * 1024 + 37))
                .map(|index| ((index * 31 + 89) % 251) as u8)
                .collect::<Vec<_>>(),
        );
        for (chunk_index, chunk_start) in (0..expected.len()).step_by(512 * 1024).enumerate() {
            let chunk_end = (chunk_start + 512 * 1024).min(expected.len());
            mount
                .write_stable(
                    created.fh.clone(),
                    chunk_start as u64,
                    expected.slice(chunk_start..chunk_end),
                )
                .await?;
            if chunk_index == 0 {
                std::fs::write(&ready, b"pnfs-write-active")?;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        wait_for_lab_file(&applied, Duration::from_secs(120)).await?;
        let callback_stats = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let stats = mount.nfs41_callback_stats().await.unwrap_or_default();
                if stats.layout_recalls_received > baseline_callbacks.layout_recalls_received
                    && stats.layout_returns_completed > baseline_callbacks.layout_returns_completed
                {
                    break stats;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| io::Error::other("ONTAP did not complete CB_LAYOUTRECALL"))?;
        mount.close(created.fh).await?;

        let opened = mount.open_path(&file, OPEN_READ).await?;
        let actual = read_all(mount.as_ref(), opened.fh.clone(), expected.len()).await?;
        mount.close(opened.fh).await?;
        ensure(
            actual == expected,
            "pNFS recall/write/close full-payload checksum mismatch",
        )?;
        eprintln!(
            "pnfs-layout-recall received={} returned={} status=ok",
            callback_stats.layout_recalls_received - baseline_callbacks.layout_recalls_received,
            callback_stats.layout_returns_completed - baseline_callbacks.layout_returns_completed,
        );
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    cleanup_pnfs_case(mount.as_ref(), &case_dir, &file).await;
    let unmount_result = mount.umount().await;
    result?;
    unmount_result?;
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
                mount.write_stable(
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
    let stage = env::var("NFS_RS_LAB_FAULT_STAGE_FILE")?;
    let acknowledged = env::var("NFS_RS_LAB_FAULT_ACK_FILE")?;
    let mount = parse_url_and_mount(&url).await?;
    ensure(mount.version() == NFSVersion::NFSv4p1, "NFSv4.1 required")?;
    let _ = mount.remove_path(RECOVERY_FILE).await;
    let _ = mount.rmdir_path(RECOVERY_DIR).await;
    mount.mkdir_path(RECOVERY_DIR, 0o755).await?;
    let created = mount.create_path(RECOVERY_FILE, Some(0o600)).await?;
    let chunk = Bytes::from(vec![0xa5; 64 * 1024]);

    std::fs::write(&ready, b"ready")?;
    let mut acknowledged_stage = 0u8;
    tokio::time::timeout(Duration::from_secs(120), async {
        while !std::path::Path::new(&completed).exists() {
            let writes = (0..64u64).map(|index| {
                mount.write_stable(
                    created.fh.clone(),
                    index * chunk.len() as u64,
                    chunk.clone(),
                )
            });
            let outcomes = futures::future::join_all(writes).await;
            let requested_stage = std::fs::read_to_string(&stage)
                .ok()
                .and_then(|value| value.trim().parse::<u8>().ok())
                .unwrap_or(0);
            if requested_stage > acknowledged_stage && outcomes.iter().any(Result::is_ok) {
                std::fs::write(&acknowledged, requested_stage.to_string())?;
                acknowledged_stage = requested_stage;
            }
        }
        Ok::<(), io::Error>(())
    })
    .await
    .map_err(|_| io::Error::other("TCP resets did not complete"))??;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real knfsd delegation recall and callback reply-loss proxy"]
async fn nfs_v41_real_callback_reply_loss_checksum() -> TestResult {
    ensure(
        env::var(LAB_ENABLE_ENV).as_deref() == Ok("1"),
        "lab disabled",
    )?;
    let url = env::var("NFS_RS_LAB_FAULT_URL")?;
    let ready = env::var("NFS_RS_LAB_FAULT_READY_FILE")?;
    let completed = env::var("NFS_RS_LAB_FAULT_DONE_FILE")?;
    let mount = parse_url_and_mount(&url).await?;
    ensure(mount.version() == NFSVersion::NFSv4p1, "NFSv4.1 required")?;
    let _ = mount.remove_path(CALLBACK_FILE).await;
    let created = mount.create_path(CALLBACK_FILE, Some(0o600)).await?;
    mount.close(created.fh).await?;
    let delegated = mount.open_path(CALLBACK_FILE, OPEN_BOTH).await?;
    std::fs::write(&ready, b"delegation-open")?;
    tokio::time::timeout(Duration::from_secs(120), async {
        while !std::path::Path::new(&completed).exists() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| io::Error::other("callback retransmission was not observed"))?;

    let expected = payload();
    write_all(mount.as_ref(), delegated.fh.clone(), &expected).await?;
    mount.close(delegated.fh).await?;
    let opened = mount.open_path(CALLBACK_FILE, OPEN_READ).await?;
    let actual = read_all(mount.as_ref(), opened.fh.clone(), expected.len()).await?;
    mount.close(opened.fh).await?;
    ensure(actual == expected, "post-callback checksum mismatch")?;
    mount.remove_path(CALLBACK_FILE).await?;
    mount.umount().await?;
    Ok(())
}
