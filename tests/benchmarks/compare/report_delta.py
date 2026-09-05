#!/usr/bin/env python3
"""Before/after tables for an nfs-rs I/O-path change.

usage: report_delta.py BASELINE_DIR TUNED_DIR [--variants base,opt] [--md OUT]

BASELINE_DIR holds the reference run (kernel mount + untuned nfs-rs, any
mount_variant). TUNED_DIR holds a later nfs-rs-only run whose cases carry
``mount_variant`` labels (see run.sh NFSRS_VARIANTS), e.g. ``base`` for the
new binary with the feature switched off and ``opt`` with it on.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from report import (
    HARNESSES,
    PROTOCOLS,
    data_value,
    fmt,
    load_results,
    multiclient_value,
    posix_filters,
    ratio,
    select,
)

DATA_ROWS: list[tuple[str, str, int, str, bool]] = [
    # label, size, qd, direction, lower_is_better
    ("4 KiB write (ms)", "4k", 1, "write", True),
    ("4 KiB read (ms)", "4k", 1, "read", True),
    ("40 MiB QD1 write", "40m", 1, "write", False),
    ("40 MiB QD1 read", "40m", 1, "read", False),
    ("40 MiB QD8 write", "40m", 8, "write", False),
    ("40 MiB QD8 read", "40m", 8, "read", False),
    ("1 GiB QD1 write", "1g", 1, "write", False),
    ("1 GiB QD1 read", "1g", 1, "read", False),
    ("1 GiB QD8 write", "1g", 8, "write", False),
    ("1 GiB QD8 read", "1g", 8, "read", False),
]


def gain(before: float | None, after: float | None, lower_is_better: bool) -> float | None:
    return ratio(before, after) if lower_is_better else ratio(after, before)


def delta_rows(baseline: list[dict[str, Any]], tuned: list[dict[str, Any]], proto: str, harness: str,
               variants: list[str]) -> list[list[str]]:
    rows = []
    nfs_before = {"harness": harness, "backend": "nfsrs"}
    # Kernel columns come from the tuned run when it re-measured the kernel mount.
    kernel = tuned if select(tuned, protocol=proto, backend="posix") else baseline
    for label, size, qd, direction, lower in DATA_ROWS:
        k_direct = data_value(kernel, proto, posix_filters(harness, "default", "direct"), size, qd, direction)
        k_buffered = data_value(kernel, proto, posix_filters(harness, "default", "buffered"), size, qd, direction)
        before = data_value(baseline, proto, nfs_before, size, qd, direction)
        values = [data_value(tuned, proto, {**nfs_before, "mount_variant": v}, size, qd, direction) for v in variants]
        last = values[-1] if values else None
        first = values[0] if values else None
        rows.append([label, fmt(k_direct), fmt(k_buffered), fmt(before), *[fmt(v) for v in values],
                     fmt(gain(first, last, lower)), fmt(gain(k_direct, last, lower)), fmt(gain(k_buffered, last, lower))])
    for mode, label in (("same", "8 客户端同文件 (MiB/s)"), ("distinct", "8 客户端不同文件 (MiB/s)")):
        k_direct = multiclient_value(kernel, proto, posix_filters(harness, "default", "direct"), mode)
        k_buffered = multiclient_value(kernel, proto, posix_filters(harness, "default", "buffered"), mode)
        before = multiclient_value(baseline, proto, nfs_before, mode)
        values = [multiclient_value(tuned, proto, {**nfs_before, "mount_variant": v}, mode) for v in variants]
        last = values[-1] if values else None
        first = values[0] if values else None
        rows.append([label, fmt(k_direct), fmt(k_buffered), fmt(before), *[fmt(v) for v in values],
                     fmt(gain(first, last, False)), fmt(gain(k_direct, last, False)), fmt(gain(k_buffered, last, False))])
    return rows


def render(baseline: list[dict[str, Any]], tuned: list[dict[str, Any]], variants: list[str]) -> str:
    out: list[str] = []
    header = ["负载", "内核 O_DIRECT", "内核 buffered 冷", "nfs-rs 昨日",
              *[f"nfs-rs {v}" for v in variants], f"{variants[-1]}/{variants[0]}", "vs O_DIRECT", "vs buffered"]
    for proto in PROTOCOLS:
        if not select(tuned, protocol=proto):
            continue
        for harness in HARNESSES:
            if not select(tuned, protocol=proto, harness=harness):
                continue
            out.append(f"### NFSv{proto} · {harness}\n")
            out.append("| " + " | ".join(header) + " |")
            out.append("|" + "---|" * len(header))
            for row in delta_rows(baseline, tuned, proto, harness, variants):
                out.append("| " + " | ".join(row) + " |")
            out.append("")
    return "\n".join(out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("tuned", type=Path)
    parser.add_argument("--variants", default="base,opt")
    parser.add_argument("--md", type=Path)
    args = parser.parse_args()
    baseline, _ = load_results(args.baseline)
    tuned, failures = load_results(args.tuned)
    variants = [v for v in args.variants.split(",") if v]
    text = render(baseline, tuned, variants)
    if failures:
        text += "\n失败用例：\n" + "\n".join(f"- {name}: {reason}" for name, reason in sorted(failures.items())) + "\n"
    if args.md:
        args.md.write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
