use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Ms,
    MiBps,
}

#[derive(Debug, Clone)]
pub struct Series {
    pub name: String,
    pub unit: Unit,
    pub samples: Vec<f64>,
    pub reference_only: bool,
}

impl Series {
    pub fn ms(name: &str) -> Self {
        Self {
            name: name.to_string(),
            unit: Unit::Ms,
            samples: Vec::new(),
            reference_only: false,
        }
    }

    pub fn mibps(name: &str) -> Self {
        Self {
            name: name.to_string(),
            unit: Unit::MiBps,
            samples: Vec::new(),
            reference_only: false,
        }
    }
}

pub fn percentile(samples: &[f64], p: f64) -> f64 {
    if samples.is_empty() {
        return f64::NAN;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() as f64 * p).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

pub fn mibps(bytes: u64, seconds: f64) -> f64 {
    bytes as f64 / 1_048_576.0 / seconds
}

pub fn series_json(s: &Series) -> Value {
    let mean = if s.samples.is_empty() {
        f64::NAN
    } else {
        s.samples.iter().sum::<f64>() / s.samples.len() as f64
    };
    match s.unit {
        Unit::Ms => json!({
            "name": s.name,
            "unit": "ms",
            "reference_only": s.reference_only,
            "samples": s.samples,
            "p50": percentile(&s.samples, 0.5),
            "p95": percentile(&s.samples, 0.95),
            "p99": percentile(&s.samples, 0.99),
            "mean": mean,
            "ops_s": if mean > 0.0 { 1000.0 / mean } else { f64::NAN },
        }),
        Unit::MiBps => json!({
            "name": s.name,
            "unit": "MiB/s",
            "reference_only": s.reference_only,
            "samples": s.samples,
            "median": percentile(&s.samples, 0.5),
            "min": s.samples.iter().copied().fold(f64::INFINITY, f64::min),
            "max": s.samples.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        }),
    }
}

pub fn peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_matches_existing_benchmark_convention() {
        let v = [5.0, 1.0, 3.0, 2.0, 4.0];
        assert_eq!(percentile(&v, 0.5), 3.0);
        assert_eq!(percentile(&v, 0.95), 5.0);
    }

    #[test]
    fn ms_series_reports_ops_per_second() {
        let mut s = Series::ms("create");
        s.samples = vec![2.0, 2.0];
        let j = series_json(&s);
        assert_eq!(j["ops_s"], 500.0);
        assert_eq!(j["unit"], "ms");
    }
}
