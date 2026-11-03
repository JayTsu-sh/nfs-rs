use serde_json::{Value, json};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};

pub struct TrackingAllocator;

const LARGE: usize = 512 * 1024;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static LARGE_LIVE: AtomicUsize = AtomicUsize::new(0);
static LARGE_PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);
static LARGE_ALLOCATED: AtomicU64 = AtomicU64::new(0);

fn add(size: usize) {
    PEAK.fetch_max(LIVE.fetch_add(size, Relaxed) + size, Relaxed);
    ALLOCATED.fetch_add(size as u64, Relaxed);
    if size >= LARGE {
        LARGE_PEAK.fetch_max(LARGE_LIVE.fetch_add(size, Relaxed) + size, Relaxed);
        LARGE_ALLOCATED.fetch_add(size as u64, Relaxed);
    }
}

fn remove(size: usize) {
    LIVE.fetch_sub(size, Relaxed);
    if size >= LARGE {
        LARGE_LIVE.fetch_sub(size, Relaxed);
    }
}

// Only this diagnostic executable wraps System. The library and its callers'
// allocator choice are unaffected. No allocation or logging in these hooks.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            add(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            add(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        remove(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, size) };
        if !result.is_null() {
            remove(layout.size());
            add(size);
        }
        result
    }
}

pub struct Phase {
    allocated: u64,
    large_allocated: u64,
}

pub fn start() -> Phase {
    PEAK.store(LIVE.load(Relaxed), Relaxed);
    LARGE_PEAK.store(LARGE_LIVE.load(Relaxed), Relaxed);
    Phase {
        allocated: ALLOCATED.load(Relaxed),
        large_allocated: LARGE_ALLOCATED.load(Relaxed),
    }
}

pub fn large_live() -> usize {
    LARGE_LIVE.load(Relaxed)
}

pub fn sample(name: &str, phase: &Phase, seconds: f64) -> std::io::Result<Value> {
    let live = LIVE.load(Relaxed);
    let peak = PEAK.load(Relaxed);
    let large_live = LARGE_LIVE.load(Relaxed);
    let large_peak = LARGE_PEAK.load(Relaxed);
    let allocated = ALLOCATED.load(Relaxed) - phase.allocated;
    let large_allocated = LARGE_ALLOCATED.load(Relaxed) - phase.large_allocated;
    let status = std::fs::read_to_string("/proc/self/status")?;
    let field = |key: &str| {
        status
            .lines()
            .find(|line| line.starts_with(key))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<u64>().ok())
    };
    let rss = field("VmRSS:").ok_or_else(|| std::io::Error::other("VmRSS unavailable"))?;
    let hwm = field("VmHWM:").ok_or_else(|| std::io::Error::other("VmHWM unavailable"))?;
    Ok(json!({
        "phase": name, "seconds": seconds,
        "live_bytes": live, "peak_live_bytes": peak,
        "large_live_bytes": large_live, "large_peak_live_bytes": large_peak,
        "allocated_bytes": allocated, "large_allocated_bytes": large_allocated,
        "rss_kib": rss, "hwm_kib": hwm, "threads": field("Threads:"),
    }))
}
