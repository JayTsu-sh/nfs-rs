#!/usr/bin/env python3
"""AFX executor: run-owned T15 fault, packet acceptance and unconditional cleanup.

Run only through run-afx-t15-e2e.sh. No credentials belong in this program or
its public artifacts. Raw captures stay in the private executor workspace.
"""

import argparse
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import time

from afx_t15_wire import (
    EvidenceError,
    api_calls,
    audit_fault,
    capture_complete,
    choose_plan,
    headers,
    owned_socket,
    rows,
)

TOTAL = 20 * 1024**3
STOP = False
LAB_LOCK = Path("/tmp/nfsrs-svm2-issue40.lock")


class Stopped(RuntimeError):
    pass


def interrupted(_signum, _frame):
    global STOP
    STOP = True


def check_stop():
    if STOP:
        raise Stopped("controller interrupted")


def atomic_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def command(args, **kwargs):
    return subprocess.run(
        args, check=True, capture_output=True, text=True, timeout=90, **kwargs
    )


def valid_run(run_id):
    if not re.fullmatch(r"(?:nightly|release)-[A-Za-z0-9._-]{1,80}", run_id):
        raise EvidenceError("invalid run ID")


def table_for(workspace):
    return "nfsrs_t15_" + hashlib.sha256(str(workspace).encode()).hexdigest()[:12]


def workspace_for(args):
    valid_run(args.run_id)
    workspace = Path(args.workspace).resolve()
    if (
        workspace.parent != Path("/tmp")
        or not workspace.name.startswith("nfsrs-afx-t15-" + args.run_id + ".")
        or Path(__file__).resolve().parent != workspace / "tools"
    ):
        raise EvidenceError("not an owned executor workspace")
    return workspace


def identity_alive(pid, marker):
    try:
        return marker.encode() in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    except FileNotFoundError:
        return False


def stop_owned(pid, marker, sig=signal.SIGTERM):
    # The unique workspace argument rejects stale PID files. A live child cannot
    # have its PID reused until we reap it, so wait for termination before any
    # fixture cleanup, even after /proc temporarily loses its command line.
    if not identity_alive(pid, marker) or os.getpgid(pid) != pid:
        return
    os.killpg(pid, sig)
    parent = True
    for _ in range(100):
        try:
            if os.waitpid(pid, os.WNOHANG)[0] == pid:
                return
        except ChildProcessError:
            parent = False
            if not identity_alive(pid, marker):
                return
        time.sleep(0.05)
    if parent or (identity_alive(pid, marker) and os.getpgid(pid) == pid):
        os.killpg(pid, signal.SIGKILL)
    if parent:
        os.waitpid(pid, 0)


def delete_fault(workspace, state):
    if state.get("fault_reserved"):
        subprocess.run(
            ["nft", "delete", "table", "inet", table_for(workspace)],
            capture_output=True,
            timeout=10,
        )
        if (
            subprocess.run(
                ["nft", "list", "table", "inet", table_for(workspace)],
                capture_output=True,
                timeout=10,
            ).returncode
            == 0
        ):
            raise EvidenceError("owned fault table remains present")


def cleanup(workspace, state):
    errors = []
    try:
        delete_fault(workspace, state)
    except Exception as error:
        errors.append(error)
    for child in reversed(state.get("children", [])):
        try:
            sig = signal.SIGINT if child["kind"] == "capture" else signal.SIGTERM
            stop_owned(child["pid"], child["marker"], sig)
            try:
                os.waitpid(child["pid"], os.WNOHANG)
            except ChildProcessError:
                pass
        except Exception as error:
            errors.append(error)
    if state.get("prepared"):
        try:
            with (workspace / "cleanup.log").open("w") as log:
                subprocess.run(
                    ["bash", str(workspace / "tools/cleanup-run.sh"), state["run_id"]],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=True,
                    timeout=120,
                    env={
                        **os.environ,
                        "LAB_PROFILE": "afx",
                        "LAB_AFX_DATA": state["mds"],
                        "LAB_AFX_EXPORT": state["export"],
                    },
                )
            state["prepared"] = False
        except Exception as error:
            errors.append(error)
    state["cleaned"] = not errors
    atomic_json(workspace / "state.json", state)
    if errors:
        raise EvidenceError("one or more owned cleanup actions failed")


class Probe:
    def __init__(self, runner, mode, label):
        self.runner = runner
        self.log = (runner.workspace / f"{label}.jsonl").open("w")
        self.error = (runner.workspace / f"{label}.stderr").open("w")
        binary = str(runner.workspace / "pnfs_partial_failure")
        self.process = runner.start(
            [binary, mode, runner.url, runner.args.run_id],
            "probe",
            binary,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            stderr=self.error,
        )
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.buffer = b""
        self.queue = []
        self.events = []
        runner.probes.append(self)

    def wait(self, phases, timeout=120):
        deadline = time.monotonic() + timeout
        while True:
            check_stop()
            if self.queue:
                event = self.queue.pop(0)
                self.events.append(event)
                if event.get("phase") in phases:
                    return event
                continue
            if time.monotonic() >= deadline:
                raise EvidenceError("probe phase timed out")
            if not self.selector.select(0.2):
                continue
            chunk = os.read(self.process.stdout.fileno(), 16384)
            if not chunk:
                raise EvidenceError("probe exited before its required phase")
            self.buffer += chunk
            if len(self.buffer) > 65536:
                raise EvidenceError("oversized diagnostic event")
            while b"\n" in self.buffer:
                line, self.buffer = self.buffer.split(b"\n", 1)
                self.log.write(line.decode("utf-8") + "\n")
                self.log.flush()
                event = json.loads(line)
                self.queue.append(event)
                print(json.dumps(event), flush=True)

    def send(self, text, phases, timeout=120):
        self.process.stdin.write((text + "\n").encode())
        self.process.stdin.flush()
        return self.wait(phases, timeout)

    def finish(self):
        if self.process.wait(timeout=15):
            raise EvidenceError("probe exited unsuccessfully")
        self.close()

    def close(self):
        self.selector.close()
        for stream in (self.process.stdin, self.process.stdout, self.log, self.error):
            stream.close()


def lab_lock():
    fd = os.open(LAB_LOCK, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    handle = os.fdopen(fd, "r+")
    deadline = time.monotonic() + 300
    try:
        while True:
            check_stop()
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return handle
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise EvidenceError("AFX lab lock timed out")
                time.sleep(0.2)
    except BaseException:
        handle.close()
        raise


class Runner:
    def __init__(self, args, workspace):
        self.args, self.workspace = args, workspace
        self.state = {
            "run_id": args.run_id,
            "mds": args.mds,
            "export": args.export,
            "controller": os.getpid(),
            "prepared": False,
            "children": [],
        }
        self.report = {
            "schema_version": 1,
            "run_id": args.run_id,
            "commit": args.commit,
            "status": "failed",
            "cleanup": "not-completed",
        }
        self.url = f"nfs://{args.mds}{args.export}/{args.run_id}?version=4.1&noresvport=true&uid=0&gid=0"
        self.lock = None
        self.capture = None
        self.probes = []

    def save(self):
        atomic_json(self.workspace / "state.json", self.state)

    def start(self, args, kind, marker, **kwargs):
        process = subprocess.Popen(args, start_new_session=True, **kwargs)
        self.state["children"].append(
            {"pid": process.pid, "kind": kind, "marker": marker}
        )
        self.save()
        return process

    def prepare(self):
        self.lock = lab_lock()
        with (self.workspace / "prepare.log").open("w") as log:
            # An interrupt only sets STOP while preparation settles. Never clean
            # a colliding/unowned directory or race a still-running prepare.
            process = subprocess.Popen(
                [
                    "bash",
                    str(self.workspace / "tools/prepare-run.sh"),
                    self.args.run_id,
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                env={
                    **os.environ,
                    "LAB_PROFILE": "afx",
                    "LAB_AFX_DATA": self.args.mds,
                    "LAB_AFX_EXPORT": self.args.export,
                },
            )
            rc = process.wait()
        if rc:
            raise EvidenceError("preparation failed; run directory was not adopted")
        self.state["prepared"] = True
        self.save()
        check_stop()

    def stop_capture(self):
        time.sleep(2)  # Drain the final control packets before stopping libpcap.
        self.capture.send_signal(signal.SIGINT)
        self.capture.wait(timeout=10)
        return capture_complete((self.workspace / "capture.stderr").read_text())

    def fault(self, probe, plan, source):
        binary = str(self.workspace / "pnfs_partial_failure")
        if not identity_alive(probe.process.pid, binary):
            raise EvidenceError("probe ownership changed")
        owned_socket(
            command(["ss", "-Hntp"]).stdout,
            probe.process.pid,
            plan["bad_ds"],
            plan["bad_port"],
            source,
        )
        table = table_for(self.workspace)
        if (
            subprocess.run(
                ["nft", "list", "table", "inet", table], capture_output=True
            ).returncode
            == 0
        ):
            raise EvidenceError("refusing to adopt an existing fault table")
        self.state["fault_reserved"] = True
        self.save()
        # This process survives a killed controller and removes ONLY this table.
        self.start(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "watchdog",
                "--workspace",
                str(self.workspace),
                "--run-id",
                self.args.run_id,
            ],
            "watchdog",
            str(self.workspace),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        command(["nft", "add", "table", "inet", table])
        command(
            [
                "nft",
                "add",
                "chain",
                "inet",
                table,
                "input",
                "{ type filter hook input priority -10; policy accept; }",
            ]
        )
        command(
            [
                "nft",
                "add",
                "rule",
                "inet",
                table,
                "input",
                "ip",
                "saddr",
                plan["bad_ds"],
                "ip",
                "daddr",
                source,
                "tcp",
                "sport",
                "2049",
                "tcp",
                "dport",
                str(plan["bad_port"]),
                "counter",
                "drop",
            ]
        )
        event = probe.send(f"fault {plan['offset']}", {"write_error", "write_done"})
        counter = json.loads(
            command(["nft", "-j", "list", "table", "inet", table]).stdout
        )
        packets = sum(
            expr.get("counter", {}).get("packets", 0)
            for item in counter["nftables"]
            for expr in item.get("rule", {}).get("expr", [])
        )
        delete_fault(self.workspace, self.state)
        if (
            not event.get("uncertain")
            or not event.get("verify_then_resume")
            or packets == 0
        ):
            raise EvidenceError("fault did not meet uncertain-outcome acceptance")
        return packets

    def exercise(self):
        seed = Probe(self, "seed", "seed")
        seed.wait({"seed_done"}, 600)
        seed.finish()
        route = json.loads(command(["ip", "-j", "route", "get", self.args.mds]).stdout)[
            0
        ]
        source, interface = route["prefsrc"], route["dev"]
        ipaddress.IPv4Address(source)
        capture_log = (self.workspace / "capture.stderr").open("w")
        pcap = self.workspace / "wire.pcap"
        self.capture = self.start(
            [
                "tcpdump",
                "--immediate-mode",
                "-i",
                interface,
                "-nn",
                "-s",
                "0",
                "-U",
                "-B",
                "8192",
                "-w",
                str(pcap),
                "tcp port 2049",
            ],
            "capture",
            str(pcap),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=capture_log,
        )
        time.sleep(1)
        if self.capture.poll() is not None:
            raise EvidenceError("packet capture failed to start")
        probe = Probe(self, "probe", "fault")
        probe.wait({"ready"})
        for offset in [0] + [g * 1024**3 - 32768 for g in range(1, 20)]:
            event = probe.send(f"warm {offset}", {"write_done", "write_error"})
            if event.get("count") != 65536:
                raise EvidenceError("warmup was not a full committed write")
        time.sleep(1)
        plan = choose_plan(rows(pcap), probe.events, self.args.mds, set(self.args.ds))
        self.report["plan"] = plan
        self.report["dropped_packets"] = self.fault(probe, plan, source)
        probe.send("close", {"umount"})
        probe.finish()
        self.report["capture"] = self.stop_capture()
        capture_log.close()
        self.report["wire"] = audit_fault(headers(pcap), rows(pcap), probe.events, plan)
        recovered = Probe(self, "probe", "recovery")
        recovered.wait({"ready"})
        mutation = recovered.send(f"inspect {plan['offset']}", {"inspect"})
        if {mutation["first"], mutation["second"]} != {"seed", "fault"}:
            raise EvidenceError(
                "did not observe an actual partially mutated file range"
            )
        self.report["partial_mutation"] = [mutation["first"], mutation["second"]]
        resumed = recovered.send(
            f"resume {plan['offset']}", {"write_done", "write_error"}
        )
        if resumed.get("count") != 65536:
            raise EvidenceError("resume did not commit the complete range")
        verified = recovered.send("verify", {"verify_done"}, 600)
        recovered.finish()
        expected = expected_digest(TOTAL)
        if verified["bytes"] != TOTAL or verified["sha256"] != expected:
            raise EvidenceError("full-file checksum or size mismatch")
        self.report.update(
            {
                "bytes_verified": TOTAL,
                "sha256": expected,
                "probe_sha256": hashlib.sha256(
                    (self.workspace / "pnfs_partial_failure").read_bytes()
                ).hexdigest(),
                "pcap_sha256": hashlib.sha256(pcap.read_bytes()).hexdigest(),
            }
        )

    def run(self):
        # Exclusive state creation: retries cannot adopt another invocation.
        with (self.workspace / "state.json").open("x") as file:
            json.dump(self.state, file)
        code = 1
        try:
            self.prepare()
            self.exercise()
            check_stop()
            self.report["status"] = "pass"
            code = 0
        except (EvidenceError, Stopped) as error:
            self.report["reason"] = str(error)
            if str(error).startswith("BLOCKED_CAPABILITY"):
                self.report["status"] = "blocked-capability"
                code = 3
        except Exception:
            self.report["reason"] = "executor/probe failure; inspect private evidence"
        finally:
            try:
                cleanup(self.workspace, self.state)
                self.report["cleanup"] = "pass"
            except Exception:
                self.report.update({"status": "failed", "cleanup": "failed"})
                code = 1
            finally:
                for probe in self.probes:
                    probe.close()
                if self.lock:
                    self.lock.close()
            (self.workspace / "public").mkdir(exist_ok=True)
            atomic_json(self.workspace / "public/result.json", self.report)
            print(json.dumps(self.report), flush=True)
        return code


def expected_digest(total):
    period = bytes((i * 29) % 251 + 1 for i in range(251))
    block = period * 32768
    digest = hashlib.sha256()
    for _ in range(total // len(block)):
        check_stop()
        digest.update(block)
    digest.update(block[: total % len(block)])
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("run", "cleanup", "watchdog"))
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--commit", default="")
    parser.add_argument("--mds", default="192.168.9.101")
    parser.add_argument("--export", default="/pnfs_test2")
    parser.add_argument(
        "--ds", nargs="+", default=[f"192.168.9.{n}" for n in range(101, 105)]
    )
    args = parser.parse_args()
    workspace = workspace_for(args)
    for address in [args.mds] + args.ds:
        ipaddress.IPv4Address(address)
    if (
        args.mds not in args.ds
        or not re.fullmatch(r"/[A-Za-z0-9._/-]+", args.export)
        or ".." in args.export
    ):
        raise EvidenceError("invalid AFX fixture")
    if args.action != "run":
        state = json.loads((workspace / "state.json").read_text())
        if state["run_id"] != args.run_id:
            raise EvidenceError("workspace belongs to another run")
        if args.action == "watchdog":
            time.sleep(180)
            delete_fault(workspace, state)
            return 0
        pid = state["controller"]
        if pid != os.getpid() and identity_alive(pid, str(workspace)):
            os.kill(pid, signal.SIGTERM)
            for _ in range(1200):
                if not identity_alive(pid, str(workspace)):
                    break
                time.sleep(0.1)
            else:
                raise EvidenceError("controller has not settled; refusing cleanup race")
        with lab_lock():
            state = json.loads((workspace / "state.json").read_text())
            cleanup(workspace, state)
        return 0
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit):
        raise EvidenceError("exact source commit is required")
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, interrupted)
    return Runner(args, workspace).run()


if __name__ == "__main__":
    sys.exit(main())
