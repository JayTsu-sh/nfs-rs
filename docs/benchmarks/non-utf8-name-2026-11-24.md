# NFS name encoding benchmark (2026-11-24)

Measured on `lisa-194` (Rocky Linux 9.7, Rust 1.95.0) in release mode.
The baseline is commit `0c3faa3`; the candidate is the working tree based on
`5964bc1` with opaque directory names and direct byte encoding. Both builds
used the same temporary benchmark module, outside the submitted source. Each
sample encodes a complete LOOKUP compound body 500,000 times. The table shows
the median ns/op of seven samples, with tests run on one test thread.

| Version / name | Baseline | Candidate | Change |
| --- | ---: | ---: | ---: |
| NFSv4.0, 35 ASCII bytes | 340.88 | 324.33 | -4.9% |
| NFSv4.1, 35 ASCII bytes | 322.70 | 320.63 | -0.6% |
| NFSv4.0, Chinese name | 261.47 | 248.30 | -5.0% |
| NFSv4.1, Chinese name | 250.79 | 253.12 | +0.9% |

The 7-sample ranges overlap for the ordinary-name cases. This measures local
request encoding only; it does not measure network latency, server throughput,
memory usage, or the Python adapter. No statistically clear regression is
established by these samples. Measurements were repeated after the final Rust
name-operation changes. Any later changes to the encoder require a new run.
