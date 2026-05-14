// Microbenchmarks for the NFSv3 XDR decode hot paths.
//
// Establishes a baseline before evaluating any zerocopy / decoder rewrite.
// All fixtures are hand-built XDR byte buffers — no network, no NFS server.

use bytes::{BufMut, Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nfs_rs::__bench::{
    decode_fattr3, decode_post_op_attr, decode_read3resok, decode_readdirplus3resok,
};

// ---------- XDR fixture builders ----------

/// XDR opaque variable: `len: u32` + bytes + zero-pad to 4-byte boundary.
fn put_var_bytes(buf: &mut BytesMut, data: &[u8]) {
    buf.put_u32(data.len() as u32);
    buf.put_slice(data);
    let pad = (4 - (data.len() % 4)) % 4;
    buf.put_bytes(0, pad);
}

/// 84-byte `fattr3` record. Field order matches the RFC 1813 wire layout.
fn put_fattr3(buf: &mut BytesMut) {
    buf.put_i32(1); // type_ = NF3REG (only field with non-obvious encoding)
    buf.put_u32(0o100_644);
    buf.put_u32(1);
    buf.put_u32(1000);
    buf.put_u32(1000);
    buf.put_u64(4096);
    buf.put_u64(4096);
    buf.put_u32(0);
    buf.put_u32(0);
    buf.put_u64(0xdead_beef);
    buf.put_u64(0x1234_5678);
    buf.put_u32(1_700_000_000);
    buf.put_u32(0);
    buf.put_u32(1_700_000_001);
    buf.put_u32(0);
    buf.put_u32(1_700_000_002);
    buf.put_u32(0);
}

/// `post_op_attr` with the TRUE branch (4-byte discriminant + fattr3).
fn put_post_op_attr_true(buf: &mut BytesMut) {
    buf.put_u32(1);
    put_fattr3(buf);
}

/// `post_op_fh3` TRUE with a variable-length handle.
fn put_post_op_fh3_true(buf: &mut BytesMut, handle: &[u8]) {
    buf.put_u32(1);
    put_var_bytes(buf, handle);
}

fn make_fattr3() -> Bytes {
    let mut buf = BytesMut::with_capacity(84);
    put_fattr3(&mut buf);
    debug_assert_eq!(buf.len(), 84);
    buf.freeze()
}

fn make_post_op_attr_true() -> Bytes {
    let mut buf = BytesMut::with_capacity(88);
    put_post_op_attr_true(&mut buf);
    debug_assert_eq!(buf.len(), 88);
    buf.freeze()
}

/// `READ3resok` with a payload of `payload_len` bytes (zeroed).
fn make_read3resok(payload_len: usize) -> Bytes {
    let pad = (4 - (payload_len % 4)) % 4;
    let total = 88 + 4 + 4 + 4 + payload_len + pad;
    let mut buf = BytesMut::with_capacity(total);
    put_post_op_attr_true(&mut buf);
    buf.put_u32(payload_len as u32); // count
    buf.put_u32(1); // eof
    buf.put_u32(payload_len as u32); // data length prefix
    buf.put_bytes(0, payload_len);
    buf.put_bytes(0, pad);
    debug_assert_eq!(buf.len(), total);
    buf.freeze()
}

/// `READDIRPLUS3resok` with `n` entries. Each entry uses a 16-byte name
/// and a 32-byte handle — typical real-world sizes.
fn make_readdirplus3resok(n: usize) -> Bytes {
    let name = b"file_____01.txt_"; // 16 bytes, already 4-aligned
    let handle = [0xAAu8; 32];

    // Wire layout per entry: fileid(8) + filename3(4+name) + cookie(8)
    // + post_op_attr TRUE(88) + post_op_fh3 TRUE(4 + 4 + handle) + nextentry(4)
    let entry_size = 8 + 4 + name.len() + 8 + 88 + 4 + 4 + handle.len() + 4;
    let total = 88 + 8 + 4 + n * entry_size + 4;

    let mut buf = BytesMut::with_capacity(total);
    put_post_op_attr_true(&mut buf);
    buf.put_bytes(0, 8); // cookieverf3
    buf.put_u32(if n == 0 { 0 } else { 1 }); // entries-present marker

    for i in 0..n {
        buf.put_u64(i as u64 + 1);
        put_var_bytes(&mut buf, name);
        buf.put_u64(i as u64 * 100);
        put_post_op_attr_true(&mut buf);
        put_post_op_fh3_true(&mut buf, &handle);
        buf.put_u32(if i + 1 < n { 1 } else { 0 });
    }
    buf.put_u32(1); // eof
    buf.freeze()
}

// ---------- Benches ----------

// Bench loops discard the decode `Result` via `black_box` so per-iter timing
// isn't polluted by branch-on-error paths. Each bench validates its fixture
// once up front to catch fixture bugs without per-iter overhead.

fn bench_fattr3(c: &mut Criterion) {
    let fixture = make_fattr3();
    assert!(decode_fattr3(fixture.clone()).is_ok());
    c.bench_function("fattr3_decode", |b| {
        b.iter(|| {
            // Bytes::clone is a cheap refcount bump — resets cursor for next iter.
            let r = decode_fattr3(black_box(fixture.clone()));
            let _ = black_box(r);
        })
    });
}

fn bench_post_op_attr(c: &mut Criterion) {
    let fixture = make_post_op_attr_true();
    assert!(decode_post_op_attr(fixture.clone()).is_ok());
    c.bench_function("post_op_attr_true_decode", |b| {
        b.iter(|| {
            let r = decode_post_op_attr(black_box(fixture.clone()));
            let _ = black_box(r);
        })
    });
}

fn bench_read3resok(c: &mut Criterion) {
    let mut group = c.benchmark_group("read3resok_decode");
    for &payload in &[4 * 1024usize, 64 * 1024, 1024 * 1024] {
        let fixture = make_read3resok(payload);
        assert!(decode_read3resok(fixture.clone()).is_ok());
        group.throughput(Throughput::Bytes(payload as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}KiB", payload / 1024)),
            &fixture,
            |b, fx| {
                b.iter(|| {
                    let r = decode_read3resok(black_box(fx.clone()));
                    let _ = black_box(r);
                })
            },
        );
    }
    group.finish();
}

fn bench_readdirplus(c: &mut Criterion) {
    let mut group = c.benchmark_group("readdirplus_page_decode");
    for &n in &[10usize, 100, 1000] {
        let fixture = make_readdirplus3resok(n);
        assert!(decode_readdirplus3resok(fixture.clone()).is_ok());
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{n}_entries")),
            &fixture,
            |b, fx| {
                b.iter(|| {
                    let r = decode_readdirplus3resok(black_box(fx.clone()));
                    let _ = black_box(r);
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_fattr3,
    bench_post_op_attr,
    bench_read3resok,
    bench_readdirplus
);
criterion_main!(benches);
