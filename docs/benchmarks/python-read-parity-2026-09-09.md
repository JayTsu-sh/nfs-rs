# Python read/readinto parity — 2026-09-09

NFSv3: `10.131.7.202:/jsj-data-set/LN_NET`, mountport 2050, nfsport 2049.
Negotiated max_read: 1 MiB; maximum concurrent chunks: 8 (actual 1/4/8).
Release-optimized native extension, synchronous File API. One warm-up and seven
measured rounds per size, alternating method order. Identical verified content;
readinto buffers were allocated once outside timing. Timing excludes seek and
content validation and includes allocation of returned bytes. Server/cache state
was not reset; these are warm sequential file reads, not cold-storage measurements.
All uniquely named remote test files and their directory were removed.

| Size | read | readinto | read_at | readinto_at |
|---|---:|---:|---:|---:|
| 40960 bytes | 55.21 | 54.91 | 55.29 | 55.70 |
| 4194304 bytes | 105.53 | 108.81 | 105.68 | 108.94 |
| 41943040 bytes | 102.36 | 110.01 | 102.55 | 110.45 |

Median throughput in MiB/s. `read` throughput differs by approximately +0.5%,
-3.0%, and -7.0% from `readinto`. Returning immutable Python bytes still requires
allocation and one final payload copy; this is not end-to-end zero-copy I/O.
Raw samples are in the adjacent JSON file. Results describe this host and network,
not a guarantee for faster networks or other storage systems.
