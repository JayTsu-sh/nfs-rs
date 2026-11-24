# Byte filename model: local benchmark

Baseline: `bc16e51` (the branch state before the byte-model revision).
Candidate: this byte-model revision. Both were built with Rust 1.95.0
in release mode and measured on `10.131.7.194` on 2026-11-24. Each row is the
median of seven serial runs of 500,000 operations, in nanoseconds per operation.
The benchmark code was injected into temporary copies on the VM; it is not part
of the product source.

| Operation | Baseline | Candidate | Change |
| --- | ---: | ---: | ---: |
| NFSv4.0 LOOKUP encode, 35 ASCII bytes | 340.85 | 351.25 | +3.1% |
| NFSv4.0 LOOKUP encode, Chinese name | 258.19 | 262.07 | +1.5% |
| NFSv4.1 LOOKUP encode, 35 ASCII bytes | 314.92 | 325.93 | +3.5% |
| NFSv4.1 LOOKUP encode, Chinese name | 251.31 | 255.94 | +1.8% |
| NFSv4.1 directory name decode, 35 ASCII bytes | 83.12 | 49.89 | -40.0% |
| NFSv4.1 directory name decode, 255 invalid UTF-8 bytes | 1469.07 | 51.92 | -96.5% |

The decode benchmark includes the former UTF-8 replacement and string allocation
on the baseline. The candidate returns only the original bytes. LOOKUP encode
measurements varied by tens of nanoseconds between samples, so the small positive
differences do not establish a throughput regression. These are local CPU
microbenchmarks; they do not replace the repository's cross-environment NFS
performance gate for release acceptance.
