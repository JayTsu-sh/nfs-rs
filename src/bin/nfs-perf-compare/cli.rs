use std::collections::HashMap;
use std::path::PathBuf;

use thiserror::Error;

pub const CHUNK: u64 = 1024 * 1024;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Usage(String),
}

#[derive(Debug, Clone)]
pub enum Target {
    Nfs(String),
    Posix(PathBuf),
}

impl Target {
    pub fn as_arg(&self) -> String {
        match self {
            Target::Nfs(url) => url.clone(),
            Target::Posix(path) => path.to_string_lossy().into_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    Direct,
    Buffered,
}

impl IoMode {
    pub fn as_str(self) -> &'static str {
        match self {
            IoMode::Direct => "direct",
            IoMode::Buffered => "buffered",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientMode {
    Same,
    Distinct,
}

impl ClientMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ClientMode::Same => "same",
            ClientMode::Distinct => "distinct",
        }
    }
}

#[derive(Debug, Clone)]
pub enum Suite {
    Metadata {
        iters: usize,
        readdir_entries: usize,
        readdir_iters: usize,
    },
    Data {
        size: u64,
        size_label: String,
        qd: usize,
        repeat: usize,
        iters: usize,
    },
    Multiclient {
        size: u64,
        size_label: String,
        clients: usize,
        mode: ClientMode,
        repeat: usize,
    },
    WorkerRead {
        path: String,
        bytes: u64,
        qd: usize,
    },
}

impl Suite {
    pub fn name(&self) -> &'static str {
        match self {
            Suite::Metadata { .. } => "metadata",
            Suite::Data { .. } => "data",
            Suite::Multiclient { .. } => "multiclient",
            Suite::WorkerRead { .. } => "worker-read",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub target: Target,
    pub workdir: String,
    pub json: PathBuf,
    pub suite: Suite,
    pub io: IoMode,
    pub smoke: bool,
}

pub fn parse_size(label: &str) -> Result<u64, CliError> {
    match label {
        "4k" => Ok(4096),
        "40m" => Ok(40 * CHUNK),
        "1g" => Ok(1024 * CHUNK),
        other => Err(CliError::Usage(format!(
            "--size must be 4k|40m|1g, got {other}"
        ))),
    }
}

fn positive<T: std::str::FromStr + PartialOrd + Default>(
    name: &str,
    value: &str,
) -> Result<T, CliError> {
    value
        .parse::<T>()
        .ok()
        .filter(|v| *v > T::default())
        .ok_or_else(|| CliError::Usage(format!("{name} must be a positive integer")))
}

fn parse_target(value: &str) -> Result<Target, CliError> {
    if value.starts_with("nfs://") {
        Ok(Target::Nfs(value.to_string()))
    } else if value.starts_with('/') {
        Ok(Target::Posix(PathBuf::from(value)))
    } else {
        Err(CliError::Usage(
            "--target must be nfs://... or an absolute path".into(),
        ))
    }
}

pub fn parse_args(args: impl Iterator<Item = String>) -> Result<Config, CliError> {
    let args: Vec<String> = args.collect();
    let mut target = None;
    let mut workdir = None;
    let mut json = None;
    let mut io = IoMode::Direct;
    let mut smoke = false;
    let mut i = 0;
    // Global options come first; the first bare word is the suite name.
    while i < args.len() && args[i].starts_with("--") {
        let option = args[i].as_str();
        if option == "--smoke" {
            smoke = true;
            i += 1;
            continue;
        }
        let value = args
            .get(i + 1)
            .ok_or_else(|| CliError::Usage(format!("{option} needs a value")))?;
        match option {
            "--target" => target = Some(parse_target(value)?),
            "--workdir" => workdir = Some(value.clone()),
            "--json" => json = Some(PathBuf::from(value)),
            "--io" => {
                io = match value.as_str() {
                    "direct" => IoMode::Direct,
                    "buffered" => IoMode::Buffered,
                    _ => return Err(CliError::Usage("--io must be direct|buffered".into())),
                }
            }
            other => return Err(CliError::Usage(format!("unknown option {other}"))),
        }
        i += 2;
    }
    let suite_name = args
        .get(i)
        .ok_or_else(|| CliError::Usage("suite name is required".into()))?;
    let rest = &args[i + 1..];
    let mut opts: HashMap<&str, &str> = HashMap::new();
    let mut j = 0;
    while j < rest.len() {
        let value = rest
            .get(j + 1)
            .ok_or_else(|| CliError::Usage(format!("{} needs a value", rest[j])))?;
        opts.insert(rest[j].as_str(), value.as_str());
        j += 2;
    }
    let count = |key: &str, default: usize| -> Result<usize, CliError> {
        opts.get(key).map_or(Ok(default), |v| positive(key, v))
    };
    let size_label = || -> Result<String, CliError> {
        opts.get("--size")
            .map(|s| s.to_string())
            .ok_or_else(|| CliError::Usage("--size is required".into()))
    };
    let suite = match suite_name.as_str() {
        "metadata" => Suite::Metadata {
            iters: if smoke { 1 } else { count("--iters", 200)? },
            readdir_entries: if smoke { 10 } else { count("--readdir-entries", 1000)? },
            readdir_iters: if smoke { 1 } else { count("--readdir-iters", 20)? },
        },
        "data" => {
            let label = size_label()?;
            let qd = count("--qd", 1)?;
            if qd != 1 && qd != 8 {
                return Err(CliError::Usage("--qd must be 1 or 8".into()));
            }
            Suite::Data {
                size: parse_size(&label)?,
                size_label: label,
                qd,
                repeat: if smoke { 1 } else { count("--repeat", 5)? },
                iters: if smoke { 1 } else { count("--iters", 200)? },
            }
        }
        "multiclient" => {
            let label = size_label()?;
            let mode = match opts.get("--mode").copied() {
                Some("same") | None => ClientMode::Same,
                Some("distinct") => ClientMode::Distinct,
                _ => return Err(CliError::Usage("--mode must be same|distinct".into())),
            };
            Suite::Multiclient {
                size: parse_size(&label)?,
                size_label: label,
                clients: count("--clients", 8)?,
                mode,
                repeat: if smoke { 1 } else { count("--repeat", 3)? },
            }
        }
        "worker-read" => Suite::WorkerRead {
            path: opts
                .get("--path")
                .map(|s| s.to_string())
                .ok_or_else(|| CliError::Usage("--path is required".into()))?,
            bytes: opts
                .get("--bytes")
                .map_or(Err(CliError::Usage("--bytes is required".into())), |v| {
                    positive("--bytes", v)
                })?,
            qd: count("--qd", 1)?,
        },
        other => return Err(CliError::Usage(format!("unknown suite {other}"))),
    };
    Ok(Config {
        target: target.ok_or_else(|| CliError::Usage("--target is required".into()))?,
        workdir: workdir.ok_or_else(|| CliError::Usage("--workdir is required".into()))?,
        json: json.unwrap_or_else(|| PathBuf::from("/dev/stdout")),
        suite,
        io,
        smoke,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<Config, CliError> {
        parse_args(s.split_whitespace().map(str::to_string))
    }

    #[test]
    fn infers_backend_from_target() {
        let c = parse("--target nfs://h/e?version=3 --workdir w --json o.json metadata").unwrap();
        assert!(matches!(c.target, Target::Nfs(_)));
        let c = parse("--target /mnt/x --workdir w --json o.json metadata").unwrap();
        assert!(matches!(c.target, Target::Posix(_)));
    }

    #[test]
    fn parses_data_suite_sizes_and_qd() {
        let c = parse("--target /mnt/x --workdir w --json o.json data --size 40m --qd 8").unwrap();
        match c.suite {
            Suite::Data { size, qd, repeat, iters, .. } => {
                assert_eq!(size, 40 * 1024 * 1024);
                assert_eq!(qd, 8);
                assert_eq!(repeat, 5);
                assert_eq!(iters, 200);
            }
            _ => panic!("expected data suite"),
        }
    }

    #[test]
    fn smoke_reduces_iterations_to_one() {
        let c = parse("--target /mnt/x --workdir w --json o.json --smoke data --size 1g --qd 1")
            .unwrap();
        match c.suite {
            Suite::Data { repeat, iters, .. } => assert_eq!((repeat, iters), (1, 1)),
            _ => panic!("expected data suite"),
        }
    }

    #[test]
    fn rejects_bad_size_and_qd() {
        assert!(parse("--target /mnt/x --workdir w --json o.json data --size 5m --qd 1").is_err());
        assert!(parse("--target /mnt/x --workdir w --json o.json data --size 4k --qd 3").is_err());
    }

    #[test]
    fn io_defaults_to_direct_and_worker_requires_bytes() {
        let c = parse("--target /mnt/x --workdir w --json o.json data --size 4k --qd 1").unwrap();
        assert!(matches!(c.io, IoMode::Direct));
        assert!(parse("--target /mnt/x --workdir w worker-read --path p").is_err());
        let c = parse("--target /mnt/x --workdir w worker-read --path p --bytes 4096").unwrap();
        assert!(matches!(c.suite, Suite::WorkerRead { bytes: 4096, qd: 1, .. }));
    }
}
