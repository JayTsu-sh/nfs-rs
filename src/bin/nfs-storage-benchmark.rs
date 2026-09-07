use std::env;
use std::process::ExitCode;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use nfs_rs::{NFSVersion, OPEN_READ, PathconfSupport, parse_url_and_mount};
use serde_json::json;
use thiserror::Error;

type AnyResult<T = ()> = Result<T, BenchmarkError>;

#[derive(Debug, Error)]
enum BenchmarkError {
    #[error("{0}")]
    Configuration(&'static str),
    #[error(transparent)]
    Nfs(#[from] nfs_rs::NfsError),
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Clock(#[from] std::time::SystemTimeError),
}

impl From<&'static str> for BenchmarkError {
    fn from(message: &'static str) -> Self {
        Self::Configuration(message)
    }
}

#[derive(Debug)]
struct Config {
    environment: String,
    protocol: String,
    run_id: String,
    window_id: String,
    commit: String,
    urls: Vec<String>,
    samples: usize,
    payload_mib: usize,
    max_metadata_p95_ms: Option<f64>,
    max_commit_p95_ms: Option<f64>,
    min_write_mib_s: Option<f64>,
    min_read_mib_s: Option<f64>,
    validate_only: bool,
}

#[derive(Debug)]
struct Sample {
    null_ms: f64,
    fsinfo_ms: f64,
    fsstat_ms: f64,
    mkdir_ms: f64,
    create_ms: f64,
    lookup_ms: f64,
    getattr_ms: f64,
    access_ms: f64,
    pathconf_ms: Option<f64>,
    pathconf_status: String,
    write_ms: f64,
    commit_ms: f64,
    close_ms: f64,
    open_ms: f64,
    read_ms: f64,
    rename_ms: f64,
    link_ms: f64,
    symlink_ms: f64,
    readlink_ms: f64,
    readdir_ms: f64,
    remove_ms: f64,
    rmdir_ms: f64,
    write_mib_s: f64,
    read_mib_s: f64,
}

#[tokio::main]
async fn main() -> ExitCode {
    let fas_mode = env::args()
        .next()
        .is_some_and(|name| name.ends_with("fas2750-storage-check"));
    match run(fas_mode).await {
        Ok(healthy) if healthy => ExitCode::SUCCESS,
        Ok(_) => ExitCode::from(2),
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(fas_mode: bool) -> AnyResult<bool> {
    let config = parse_config(env::args().skip(1), fas_mode)?;
    if config.validate_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "environment": config.environment,
                "protocol": config.protocol,
                "urls": config.urls,
                "status": "configuration_valid"
            }))?
        );
        return Ok(true);
    }
    let payload_len = config.payload_mib * 1024 * 1024;
    let payload = Bytes::from(
        (0..payload_len)
            .map(|index| ((index * 17 + 29) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let run_id = format!("{}-{}", std::process::id(), unix_seconds()?);
    let mut lif_reports = Vec::new();
    let mut healthy = true;

    for url in &config.urls {
        let mount_started = Instant::now();
        let mount = parse_url_and_mount(url).await?;
        let expected_version = match config.protocol.as_str() {
            "3" => NFSVersion::NFSv3,
            "4.0" => NFSVersion::NFSv4p0,
            "4.1" => NFSVersion::NFSv4p1,
            _ => return Err("unsupported protocol".into()),
        };
        if mount.version() != expected_version {
            return Err("server negotiated an unexpected NFS protocol".into());
        }
        let mount_ms = millis(mount_started);
        let host = url::Url::parse(url)?
            .host_str()
            .ok_or("NFS URL has no host")?
            .to_string();
        let max_read = mount.get_max_read_size();
        let max_write = mount.get_max_write_size();
        let mut samples = Vec::new();

        for sample_index in 0..config.samples {
            let directory = format!("nfsrs-storage-check-{run_id}-{sample_index}");
            let name = format!("{directory}/payload.bin");
            let renamed = format!("{directory}/renamed.bin");
            let hardlink = format!("{directory}/payload.hardlink");
            let symlink = format!("{directory}/payload.symlink");
            for stale in [&name, &renamed, &hardlink, &symlink] {
                let _ = mount.remove_path(stale).await;
            }
            let _ = mount.rmdir_path(&directory).await;

            let sample_result: AnyResult<Sample> = async {
                let started = Instant::now();
                mount.null().await?;
                let null_ms = millis(started);
                let started = Instant::now();
                mount.fsinfo().await?;
                let fsinfo_ms = millis(started);
                let started = Instant::now();
                mount.fsstat().await?;
                let fsstat_ms = millis(started);
                let started = Instant::now();
                let directory_obj = mount.mkdir_path(&directory, 0o700).await?;
                let mkdir_ms = millis(started);

                let started = Instant::now();
                let created = mount.create_path(&name, Some(0o600)).await?;
                let create_ms = millis(started);

                let started = Instant::now();
                mount
                    .lookup(directory_obj.fh.clone(), "payload.bin")
                    .await?;
                let lookup_ms = millis(started);
                let started = Instant::now();
                mount.getattr(created.fh.clone()).await?;
                let getattr_ms = millis(started);
                let started = Instant::now();
                mount.access(created.fh.clone(), 1).await?;
                let access_ms = millis(started);
                let started = Instant::now();
                let pathconf = mount.pathconf_with_support(created.fh.clone()).await?;
                let pathconf_ms = Some(millis(started));
                let pathconf_status = pathconf_status(pathconf.available);

                let started = Instant::now();
                let mut offset = 0usize;
                while offset < payload.len() {
                    let end = (offset + max_write as usize).min(payload.len());
                    let written = mount
                        .write_stable(
                            created.fh.clone(),
                            offset as u64,
                            payload.slice(offset..end),
                        )
                        .await? as usize;
                    if written == 0 || written > end - offset {
                        return Err("invalid NFS WRITE count".into());
                    }
                    offset += written;
                }
                let write_ms = millis(started);

                let started = Instant::now();
                mount
                    .commit(created.fh.clone(), 0, payload.len() as u32)
                    .await?;
                let commit_ms = millis(started);
                let started = Instant::now();
                mount.close(created.fh).await?;
                let close_ms = millis(started);

                let started = Instant::now();
                let opened = mount.open_path(&name, OPEN_READ).await?;
                let open_ms = millis(started);
                let started = Instant::now();
                let mut actual = BytesMut::with_capacity(payload.len());
                let mut read_offset = 0usize;
                while read_offset < payload.len() {
                    let requested = (payload.len() - read_offset).min(max_read as usize) as u32;
                    let part = mount
                        .read(opened.fh.clone(), read_offset as u64, requested)
                        .await?;
                    if part.is_empty() {
                        return Err("unexpected EOF while verifying payload".into());
                    }
                    read_offset += part.len();
                    actual.extend_from_slice(&part);
                }
                let read_ms = millis(started);
                mount.close(opened.fh).await?;
                if actual.as_ref() != payload.as_ref() {
                    return Err("read-back data integrity mismatch".into());
                }

                let started = Instant::now();
                mount.rename_path(&name, &renamed).await?;
                let rename_ms = millis(started);
                let started = Instant::now();
                mount.link_path(&renamed, &hardlink).await?;
                let link_ms = millis(started);
                let started = Instant::now();
                let symlink_obj = mount.symlink_path("renamed.bin", &symlink).await?;
                let symlink_ms = millis(started);
                let started = Instant::now();
                let target = mount.readlink(symlink_obj.fh).await?;
                let readlink_ms = millis(started);
                if target != "renamed.bin" {
                    return Err("readlink target mismatch".into());
                }
                let started = Instant::now();
                let mut entries = mount.readdir(directory_obj.fh).await;
                while entries.try_next().await?.is_some() {}
                let readdir_ms = millis(started);

                mount.remove_path(&hardlink).await?;
                mount.remove_path(&symlink).await?;
                let started = Instant::now();
                mount.remove_path(&renamed).await?;
                let remove_ms = millis(started);
                let started = Instant::now();
                mount.rmdir_path(&directory).await?;
                let rmdir_ms = millis(started);
                Ok(Sample {
                    null_ms,
                    fsinfo_ms,
                    fsstat_ms,
                    mkdir_ms,
                    create_ms,
                    lookup_ms,
                    getattr_ms,
                    access_ms,
                    pathconf_ms,
                    pathconf_status,
                    write_ms,
                    commit_ms,
                    close_ms,
                    open_ms,
                    read_ms,
                    rename_ms,
                    link_ms,
                    symlink_ms,
                    readlink_ms,
                    readdir_ms,
                    remove_ms,
                    rmdir_ms,
                    write_mib_s: config.payload_mib as f64 / (write_ms / 1000.0),
                    read_mib_s: config.payload_mib as f64 / (read_ms / 1000.0),
                })
            }
            .await;
            match sample_result {
                Ok(sample) => samples.push(sample),
                Err(error) => {
                    cleanup_sample(
                        mount.as_ref(),
                        &directory,
                        [&name, &renamed, &hardlink, &symlink],
                    )
                    .await;
                    let _ = mount.umount().await;
                    return Err(error);
                }
            }
        }
        let started = Instant::now();
        mount.umount().await?;
        let umount_ms = millis(started);

        let create_p95 = p95(samples.iter().map(|sample| sample.create_ms));
        let commit_p95 = p95(samples.iter().map(|sample| sample.commit_ms));
        let remove_p95 = p95(samples.iter().map(|sample| sample.remove_ms));
        let write_median = median(samples.iter().map(|sample| sample.write_mib_s));
        let read_median = median(samples.iter().map(|sample| sample.read_mib_s));
        let lif_healthy = config
            .max_metadata_p95_ms
            .is_none_or(|limit| create_p95 <= limit && remove_p95 <= limit)
            && config
                .max_commit_p95_ms
                .is_none_or(|limit| commit_p95 <= limit)
            && config
                .min_write_mib_s
                .is_none_or(|limit| write_median >= limit)
            && config
                .min_read_mib_s
                .is_none_or(|limit| read_median >= limit);
        healthy &= lif_healthy;
        lif_reports.push(json!({
            "host": host,
            "mount_ms": mount_ms,
            "umount_ms": umount_ms,
            "max_read": max_read,
            "max_write": max_write,
            "healthy": lif_healthy,
            "summary": {
                "create_p95_ms": create_p95,
                "commit_p95_ms": commit_p95,
                "remove_p95_ms": remove_p95,
                "write_median_mib_s": write_median,
                "read_median_mib_s": read_median,
            },
            "samples": samples.iter().map(|sample| json!({
                "null_ms": sample.null_ms,
                "fsinfo_ms": sample.fsinfo_ms,
                "fsstat_ms": sample.fsstat_ms,
                "mkdir_ms": sample.mkdir_ms,
                "create_ms": sample.create_ms,
                "lookup_ms": sample.lookup_ms,
                "getattr_ms": sample.getattr_ms,
                "access_ms": sample.access_ms,
                "pathconf_ms": sample.pathconf_ms,
                "pathconf_status": sample.pathconf_status,
                "write_ms": sample.write_ms,
                "commit_ms": sample.commit_ms,
                "close_ms": sample.close_ms,
                "open_ms": sample.open_ms,
                "read_ms": sample.read_ms,
                "rename_ms": sample.rename_ms,
                "link_ms": sample.link_ms,
                "symlink_ms": sample.symlink_ms,
                "readlink_ms": sample.readlink_ms,
                "readdir_ms": sample.readdir_ms,
                "remove_ms": sample.remove_ms,
                "rmdir_ms": sample.rmdir_ms,
                "write_mib_s": sample.write_mib_s,
                "read_mib_s": sample.read_mib_s,
                "data_integrity": "pass",
            })).collect::<Vec<_>>(),
        }));
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
        "schema_version": 2,
        "environment": config.environment,
        "run_id": config.run_id,
        "window_id": config.window_id,
        "commit": config.commit,
        "runner": env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
        "captured_at_unix": unix_seconds()?,
        "protocol": config.protocol,
            "payload_mib": config.payload_mib,
            "sample_count": config.samples,
            "status": if healthy { "pass" } else { "fail" },
            "thresholds": {
                "max_metadata_p95_ms": config.max_metadata_p95_ms,
                "max_commit_p95_ms": config.max_commit_p95_ms,
                "min_write_mib_s": config.min_write_mib_s,
                "min_read_mib_s": config.min_read_mib_s,
            },
            "lifs": lif_reports,
        }))?
    );
    Ok(healthy)
}

fn pathconf_status(support: PathconfSupport) -> String {
    let missing = [
        ("linkmax", support.linkmax),
        ("name_max", support.name_max),
        ("no_trunc", support.no_trunc),
        ("chown_restricted", support.chown_restricted),
        ("case_insensitive", support.case_insensitive),
        ("case_preserving", support.case_preserving),
    ]
    .into_iter()
    .filter_map(|(name, available)| (!available).then_some(name))
    .collect::<Vec<_>>();

    if missing.is_empty() {
        "pass".to_string()
    } else {
        format!("pass_with_defaults: {}", missing.join(","))
    }
}

fn parse_config(
    mut args: impl Iterator<Item = String>,
    fas_mode: bool,
) -> Result<Config, &'static str> {
    let mut config = Config {
        environment: if fas_mode {
            "fas2750-v40".to_string()
        } else {
            String::new()
        },
        protocol: String::new(),
        run_id: format!("manual-{}", std::process::id()),
        window_id: "manual".to_string(),
        commit: "unknown".to_string(),
        urls: Vec::new(),
        samples: 5,
        payload_mib: 16,
        max_metadata_p95_ms: None,
        max_commit_p95_ms: None,
        min_write_mib_s: None,
        min_read_mib_s: None,
        validate_only: false,
    };
    while let Some(argument) = args.next() {
        if argument == "--validate-only" && !fas_mode {
            config.validate_only = true;
            continue;
        }
        let value = args.next().ok_or("option value is required")?;
        match argument.as_str() {
            "--environment" if !fas_mode => config.environment = value,
            "--run-id" if !fas_mode => config.run_id = value,
            "--window-id" if !fas_mode => config.window_id = value,
            "--commit" if !fas_mode => config.commit = value,
            "--url" => {
                let parsed = url::Url::parse(&value).map_err(|_| "--url is not a valid URL")?;
                let versions = parsed
                    .query_pairs()
                    .filter(|(name, _)| name == "version")
                    .map(|(_, value)| value.into_owned())
                    .collect::<Vec<_>>();
                if parsed.scheme() != "nfs"
                    || versions.len() != 1
                    || (fas_mode && versions[0] != "4.0")
                    || (!fas_mode && !matches!(versions[0].as_str(), "3" | "4.0" | "4.1"))
                {
                    return Err(if fas_mode {
                        "--url must select exact NFSv4.0 with version=4.0"
                    } else {
                        "--url must select one exact supported NFS version"
                    });
                }
                if config.protocol.is_empty() {
                    config.protocol.clone_from(&versions[0]);
                } else if config.protocol != versions[0] {
                    return Err("all --url values must use the same protocol");
                }
                config.urls.push(value);
            }
            "--samples" => config.samples = positive_usize(&value)?,
            "--payload-mib" => config.payload_mib = positive_usize(&value)?,
            "--max-metadata-p95-ms" => config.max_metadata_p95_ms = Some(positive_f64(&value)?),
            "--max-commit-p95-ms" => config.max_commit_p95_ms = Some(positive_f64(&value)?),
            "--min-write-mib-s" => config.min_write_mib_s = Some(positive_f64(&value)?),
            "--min-read-mib-s" => config.min_read_mib_s = Some(positive_f64(&value)?),
            _ => return Err("unknown option"),
        }
    }
    if fas_mode && config.urls.len() != 2 {
        return Err("exactly two --url values are required");
    }
    if !fas_mode && config.urls.len() != 1 {
        return Err("exactly one --url value is required");
    }
    if config.environment.is_empty()
        || !config
            .environment
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err("--environment must be a lowercase identifier");
    }
    if !config
        .run_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err("--run-id contains unsafe characters");
    }
    if !config
        .window_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err("--window-id contains unsafe characters");
    }
    Ok(config)
}

fn positive_usize(value: &str) -> Result<usize, &'static str> {
    value
        .parse()
        .ok()
        .filter(|value| *value > 0)
        .ok_or("value must be a positive integer")
}

fn positive_f64(value: &str) -> Result<f64, &'static str> {
    value
        .parse()
        .ok()
        .filter(|value: &f64| value.is_finite() && *value > 0.0)
        .ok_or("threshold must be a positive number")
}

fn millis(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn percentile(mut samples: Vec<f64>, percentile: f64) -> f64 {
    samples.sort_by(f64::total_cmp);
    let index = ((samples.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(samples.len() - 1);
    samples[index]
}

fn p95(samples: impl Iterator<Item = f64>) -> f64 {
    percentile(samples.collect(), 0.95)
}

fn median(samples: impl Iterator<Item = f64>) -> f64 {
    percentile(samples.collect(), 0.5)
}

fn unix_seconds() -> AnyResult<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs())
}

async fn cleanup_sample<'a>(
    mount: &dyn nfs_rs::Mount,
    directory: &str,
    paths: impl IntoIterator<Item = &'a String>,
) {
    for path in paths {
        let _ = mount.remove_path(path).await;
    }
    let _ = mount.rmdir_path(directory).await;
}
