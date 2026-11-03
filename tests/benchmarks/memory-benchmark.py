#!/usr/bin/env python3
"""Run isolated memory diagnostics; this is not the release performance gate."""
import argparse
import hashlib
import json
import selectors
import signal
import statistics
import subprocess
import time
from pathlib import Path


def analyze(report, tail_limit):
    if report.get("schema_version") != 1:
        raise ValueError("unsupported memory report schema")
    if report.get("status") != "pass" or report.get("large_allocations_released") is not True:
        raise ValueError("benchmark did not release its large allocations")
    config = report["config"]
    phases = report["phases"]
    by_name = {phase["phase"]: phase for phase in phases}
    if len(by_name) != len(phases) or "runtime" not in by_name or "unmounted" not in by_name:
        raise ValueError("missing or duplicate memory lifecycle measurements")
    if by_name["unmounted"]["large_live_bytes"] > by_name["runtime"]["large_live_bytes"]:
        raise ValueError("large allocations remain after release")
    if config["mode"] != "read" and "write" not in by_name:
        raise ValueError("missing WRITE measurement")
    idle = []
    idle_live = []
    if config["mode"] != "write":
        for index in range(config["rounds"]):
            if f"read-{index}" not in by_name or f"idle-{index}" not in by_name:
                raise ValueError("incomplete READ rounds")
            idle.append(by_name[f"idle-{index}"]["large_live_bytes"])
            idle_live.append(by_name[f"idle-{index}"]["live_bytes"])
    growth = max(idle[-3:]) - min(idle[-3:]) if len(idle) >= 3 else None
    live_growth = max(idle_live[-3:]) - min(idle_live[-3:]) if len(idle_live) >= 3 else None
    if growth is not None and growth > tail_limit:
        raise ValueError(f"large live allocation tail did not plateau: {growth} bytes")
    if live_growth is not None and live_growth > tail_limit:
        raise ValueError(f"live allocation tail did not plateau: {live_growth} bytes")
    return {
        "peak_rss_kib": max(phase["hwm_kib"] for phase in phases),
        "peak_live_bytes": max(phase["peak_live_bytes"] for phase in phases),
        "tail_large_live_range_bytes": growth,
        "tail_live_range_bytes": live_growth,
        "plateau": "pass" if growth is not None else "not_evaluated_insufficient_rounds",
        "io": [
            {**phase, "mib_s": config["payload_bytes"] / 2**20 / phase["seconds"]}
            for phase in phases
            if phase["phase"] == "write" or phase["phase"].startswith("read-")
        ],
    }


def save_report(output, tag, text, tail_limit):
    lines = [json.loads(line) for line in text.splitlines() if line.strip()]
    if len(lines) != 1:
        raise ValueError("expected one final benchmark report")
    report = lines[0]
    (output / f"{tag}.json").write_text(json.dumps(report, indent=2) + "\n")
    return {"case": tag, "config": report["config"], **analyze(report, tail_limit)}


def readers(commands, timeout):
    processes = []
    selector = selectors.DefaultSelector()
    try:
        for command in commands:
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, text=True)
            processes.append(process)
            selector.register(process.stdout, selectors.EVENT_READ, process)
        deadline = time.monotonic() + timeout
        remaining = len(processes)
        while remaining:
            if time.monotonic() >= deadline:
                raise TimeoutError("concurrent reader setup timed out")
            for key, _ in selector.select(timeout=0.1):
                line = key.fileobj.readline()
                if not line or json.loads(line).get("status") != "ready":
                    raise ValueError("concurrent reader failed before its start barrier")
                selector.unregister(key.fileobj)
                remaining -= 1
        for process in processes:
            process.stdin.write("go\n")
            process.stdin.flush()
        results = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=max(0.1, deadline - time.monotonic()))
            results.append((process.returncode, stdout, stderr))
        return results
    finally:
        selector.close()
        for process in processes:
            if process.poll() is None:
                process.kill()
        for process in processes:
            process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--url", action="append", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="candidate")
    parser.add_argument("--mib", type=int, default=1024)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--readers", type=int, default=8)
    parser.add_argument("--reader-rounds", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--max-tail-growth-kib", type=int, default=1024,
                        help="diagnostic live-allocation tail range, not a release RSS baseline")
    args = parser.parse_args()
    if min(args.mib, args.rounds, args.repeats, args.readers, args.reader_rounds) <= 0 \
            or args.timeout <= 0 or args.max_tail_growth_kib < 0:
        parser.error("sizes, counts and timeout must be positive; tail limit must be nonnegative")
    if len(set(args.url)) != len(args.url):
        parser.error("duplicate URLs would mix endpoint/protocol results")
    args.binary = args.binary.resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=False)
    summary = {"schema_version": 1, "status": "running", "label": args.label,
               "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
               "tail_limit_bytes": args.max_tail_growth_kib * 1024, "cases": []}
    tail_limit = summary["tail_limit_bytes"]

    def command(url, case, mode, qd, rounds):
        return [str(args.binary), "--url", url, "--run-id", args.run_id, "--case", case,
                "--mode", mode, "--qd", str(qd), "--mib", str(args.mib), "--rounds", str(rounds)]

    def run_one(url, case, tag, mode, qd, rounds):
        print(f"START {tag}", flush=True)
        result = subprocess.run(command(url, case, mode, qd, rounds),
                                capture_output=True, text=True, timeout=args.timeout)
        (args.output / f"{tag}.stderr").write_text(result.stderr)
        if result.returncode:
            (args.output / f"{tag}.stdout").write_text(result.stdout)
            raise RuntimeError(f"{tag} failed ({result.returncode}): {result.stderr}")
        summary["cases"].append(save_report(args.output, tag, result.stdout, tail_limit))

    try:
        for index, url in enumerate(args.url):
            for repeat in range(args.repeats):
                prefix = f"u{index}-r{repeat}"
                seed = f"{prefix}-seed"
                run_one(url, seed, seed, "write", 8, 1)
                for qd in (1, 8):
                    run_one(url, seed, f"{prefix}-read-q{qd}", "read", qd, args.rounds)
                    case = f"{prefix}-both-q{qd}"
                    run_one(url, case, case, "write-read", qd, args.rounds)
                print(f"START {prefix}-{args.readers}-readers", flush=True)
                commands = [command(url, seed, "read", 1, args.reader_rounds) + ["--wait-for-stdin"]
                            for _ in range(args.readers)]
                results = readers(commands, args.timeout)
                workers = []
                for worker, (code, stdout, stderr) in enumerate(results):
                    tag = f"{prefix}-reader-{worker}"
                    (args.output / f"{tag}.stderr").write_text(stderr)
                    if code:
                        (args.output / f"{tag}.stdout").write_text(stdout)
                        raise RuntimeError(f"{tag} failed ({code}): {stderr}")
                    report = save_report(args.output, tag, stdout, tail_limit)
                    summary["cases"].append(report)
                    workers.append(report["peak_rss_kib"])
                summary.setdefault("concurrent_reader_groups", []).append({
                    "url": url, "repeat": repeat, "workers": len(workers),
                    "max_worker_peak_rss_kib": max(workers),
                    "median_worker_peak_rss_kib": statistics.median(workers),
                    "sum_of_individual_peak_rss_kib": sum(workers),
                })
        summary["status"] = "pass"
    except BaseException as error:
        summary["status"] = "failed"
        summary["error"] = str(error)
        raise
    finally:
        (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt("memory benchmark interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    main()
