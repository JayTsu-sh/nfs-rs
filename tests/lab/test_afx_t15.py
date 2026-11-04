"""T15 negative evidence and cleanup contracts; never contacts a storage host."""

import argparse
import copy
import json
import os
from pathlib import Path
import signal
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import afx_t15 as controller
from afx_t15_wire import (
    EvidenceError,
    audit_fault,
    capture_complete,
    choose_plan,
    owned_socket,
    write_header,
    validate_result,
)


BASE = 1024**3 - 32768


def fixture():
    begin = {
        "phase": "write_begin",
        "kind": "warm",
        "offset": BASE,
        "length": 65536,
        "unix_ns": "1000000000",
    }
    end = {
        "phase": "write_done",
        "kind": "warm",
        "count": 65536,
        "unix_ns": "2000000000",
    }
    packets = []
    for n, peer in enumerate(("192.168.9.103", "192.168.9.104")):
        packet = {
            "frame.time_epoch": "1.1",
            "rpc.msgtyp": "0",
            "nfs.tag": "ds-write",
            "tcp.stream": str(n),
            "rpc.xid": hex(n + 1),
            "ip.dst": peer,
            "tcp.srcport": str(4000 + n),
            "nfs.offset4": str(BASE + n * 32768),
            "nfs.write.data_length": "32768",
            "nfs.fhandle": "01020304",
        }
        packets.append(packet)
        packets.append(
            {
                **packet,
                "frame.time_epoch": "1.5",
                "rpc.msgtyp": "1",
                "nfs.count4": "32768",
                "nfs.status": "0,0,0,0",
            }
        )
    return [begin, end], packets


class WireTests(unittest.TestCase):
    def plan(self, events, packets):
        return choose_plan(
            packets,
            events,
            "192.168.9.101",
            {f"192.168.9.{x}" for x in range(101, 105)},
        )

    def test_real_same_call_mapping_is_required(self):
        events, packets = fixture()
        plan = self.plan(events, packets)
        self.assertEqual(plan["offset"], BASE)
        self.assertNotEqual(plan["good_ds"], plan["bad_ds"])
        for field, value in [
            ("nfs.fhandle", ""),
            ("ip.dst", "192.168.9.103"),
            ("nfs.write.data_length", "65536"),
        ]:
            with self.subTest(field=field):
                broken = copy.deepcopy(packets)
                broken[2][field] = value
                with self.assertRaises(EvidenceError):
                    self.plan(events, broken)

    def test_short_or_failed_ack_cannot_pass_capability(self):
        events, packets = fixture()
        for field, value in [("nfs.count4", "1"), ("nfs.status", "0,0,5")]:
            broken = copy.deepcopy(packets)
            broken[3][field] = value
            with self.assertRaises(EvidenceError):
                self.plan(events, broken)

    def test_distinct_calls_cannot_be_joined_into_one_striped_write(self):
        events, packets = fixture()
        packets[2]["frame.time_epoch"] = "3.1"
        packets[3]["frame.time_epoch"] = "3.5"
        events.extend(
            [
                {**events[0], "unix_ns": "3000000000"},
                {**events[1], "unix_ns": "4000000000"},
            ]
        )
        with self.assertRaises(EvidenceError):
            self.plan(events, packets)

    def test_server_supplied_endpoint_cannot_expand_fault_allowlist(self):
        events, packets = fixture()
        packets[2]["ip.dst"] = "203.0.113.99"
        with self.assertRaisesRegex(EvidenceError, "allow-list"):
            self.plan(events, packets)

    def test_partial_tcp_body_still_proves_a_write_attempt(self):
        def u(value):
            return struct.pack(">I", value)

        def opaque(value):
            return u(len(value)) + value + b"\0" * ((-len(value)) % 4)

        body = (
            u(7)
            + u(0)
            + u(2)
            + u(100003)
            + u(4)
            + u(1)
            + u(0)
            + opaque(b"")
            + u(0)
            + opaque(b"")
            + opaque(b"ds-write")
            + u(1)
            + u(3)
            + u(53)
            + b"s" * 16
            + u(1)
            + u(0)
            + u(0)
            + u(1)
            + u(22)
            + opaque(b"handle")
            + u(38)
            + b"t" * 16
            + struct.pack(">Q", BASE)
            + u(0)
            + u(32768)
        )
        raw = (u(0x80000000 | len(body) + 32768) + body).hex()
        header = write_header(raw)
        self.assertEqual((header["offset"], header["length"]), (BASE, 32768))
        with self.assertRaises(ValueError):
            write_header(raw[:30])

    def test_fault_requires_good_ack_and_rejects_mds_replay(self):
        events, packets = fixture()
        plan = self.plan(events, packets)
        events = [
            {**events[0], "kind": "fault"},
            {
                "kind": "fault",
                "phase": "write_error",
                "uncertain": True,
                "verify_then_resume": True,
                "unix_ns": "2000000000",
            },
        ]
        requests = [
            {
                "frame.time_epoch": "1.1",
                "ds": x["ip.dst"],
                "stream": x["tcp.stream"],
                "xid": int(x["rpc.xid"], 16),
                "tag": "ds-write",
                "offset": int(x["nfs.offset4"]),
                "length": 32768,
                "fh": b"handle",
            }
            for x in packets
            if x["rpc.msgtyp"] == "0"
        ]
        self.assertEqual(
            audit_fault(requests, packets, events, plan)["mds_fallback_write_count"], 0
        )
        with self.assertRaises(EvidenceError):
            audit_fault(requests, packets[:1] + packets[2:], events, plan)
        with self.assertRaises(EvidenceError):
            audit_fault(
                requests + [dict(requests[0], ds="192.168.9.101", tag="write")],
                packets,
                events,
                plan,
            )
        # A fully dissected WRITE with an unknown shape must not be ignored.
        with self.assertRaisesRegex(EvidenceError, "unrecognized"):
            audit_fault(
                requests,
                packets + [dict(packets[0], **{"rpc.xid": "0xffff"})],
                events,
                plan,
            )
        events[1]["verify_then_resume"] = False
        with self.assertRaises(EvidenceError):
            audit_fault(requests, packets, events, plan)

    def test_incomplete_or_empty_captures_fail_closed(self):
        def report(a, b, c):
            return f"{a} packets captured\n{b} packets received by filter\n{c} packets dropped by kernel\n"

        self.assertEqual(capture_complete(report(3, 3, 0))["captured"], 3)
        for text in [report(0, 0, 0), report(3, 4, 0), report(3, 3, 1), ""]:
            with self.assertRaises(EvidenceError):
                capture_complete(text)

    def test_fault_matches_exact_process_and_four_tuple(self):
        line = 'ESTAB 0 0 192.168.9.17:4001 192.168.9.104:2049 users:(("probe",pid=123,fd=7))'
        owned_socket(line, 123, "192.168.9.104", 4001, "192.168.9.17")
        for bad in [
            line.replace("pid=123", "pid=1123"),
            line.replace(":4001 ", ":40010 "),
        ]:
            with self.assertRaises(EvidenceError):
                owned_socket(bad, 123, "192.168.9.104", 4001, "192.168.9.17")

    def test_incomplete_or_wrong_build_report_cannot_pass(self):
        value = {
            "schema_version": 1,
            "status": "pass",
            "cleanup": "pass",
            "commit": "a" * 40,
            "probe_sha256": "b" * 64,
            "bytes_verified": 20 * 1024**3,
            "sha256": "c" * 64,
            "pcap_sha256": "d" * 64,
            "plan": {
                "good_ds": "192.168.9.103",
                "bad_ds": "192.168.9.104",
                "length": 65536,
                "stripe_bytes": 32768,
            },
            "wire": {
                "same_ds_fh": True,
                "good_ds_success_observed": True,
                "mds_fallback_write_count": 0,
            },
            "partial_mutation": ["seed", "fault"],
            "dropped_packets": 3,
            "capture": {"captured": 10, "received": 10, "dropped": 0},
        }
        validate_result(value, "a" * 40, "b" * 64)
        for field, bad in [
            ("commit", "e" * 40),
            ("cleanup", "failed"),
            ("bytes_verified", 1),
            ("probe_sha256", "f" * 64),
        ]:
            broken = copy.deepcopy(value)
            broken[field] = bad
            with self.assertRaises(EvidenceError):
                validate_result(broken, "a" * 40, "b" * 64)
        with self.assertRaises(EvidenceError):
            validate_result({}, "a" * 40, "b" * 64)


class CleanupTests(unittest.TestCase):
    def setUp(self):
        controller.STOP = False

    def test_failed_prepare_does_not_authorize_fixture_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            state = {"run_id": "nightly-test", "prepared": False, "children": []}
            with patch.object(controller.subprocess, "run") as run:
                controller.cleanup(Path(root), state)
                run.assert_not_called()

    def test_fault_restoration_requires_successful_absence_query(self):
        root = Path("/tmp/nfsrs-afx-t15-nightly-test.fixture")
        state = {"fault_reserved": True}

        def result(code, tables=None):
            return subprocess.CompletedProcess(
                [], code, json.dumps({"nftables": tables or []}), ""
            )

        with patch.object(
            controller.subprocess,
            "run",
            side_effect=[result(0), subprocess.CalledProcessError(1, ["nft"])],
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                controller.delete_fault(root, state)
        owned = {"table": {"family": "inet", "name": controller.table_for(root)}}
        with patch.object(
            controller.subprocess, "run", side_effect=[result(1), result(0, [owned])]
        ):
            with self.assertRaisesRegex(EvidenceError, "remains present"):
                controller.delete_fault(root, state)
        with patch.object(
            controller.subprocess, "run", side_effect=[result(1), result(0)]
        ):
            controller.delete_fault(root, state)

    def test_fault_cleanup_failure_does_not_skip_remaining_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            state = {
                "run_id": "nightly-test",
                "prepared": True,
                "mds": "192.168.9.101",
                "export": "/pnfs_test2",
                "children": [{"pid": 123, "kind": "probe", "marker": "owned"}],
            }
            with (
                patch.object(
                    controller, "delete_fault", side_effect=RuntimeError("fault")
                ),
                patch.object(controller, "stop_owned") as stop,
                patch.object(controller.subprocess, "run") as run,
            ):
                with self.assertRaises(EvidenceError):
                    controller.cleanup(Path(root), state)
                stop.assert_called_once()
                run.assert_called_once()
                self.assertFalse(state["prepared"])
                self.assertFalse(state["cleaned"])

    def test_prepare_interrupt_settles_ownership_before_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "tools").mkdir()
            prepare = root / "tools/prepare-run.sh"
            clean = root / "tools/cleanup-run.sh"
            prepare.write_text(
                f'#!/bin/bash\ntouch "{root}/created"\nwhile [[ ! -f "{root}/release" ]]; do sleep .02; done\n'
            )
            clean.write_text(f'#!/bin/bash\ntouch "{root}/cleaned"\n')
            code = f"""import sys,signal,argparse
sys.path.insert(0,{str(Path(__file__).parent)!r})
import afx_t15 as x
from pathlib import Path
x.LAB_LOCK=Path({str(root / "lock")!r})
for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,x.interrupted)
a=argparse.Namespace(run_id='nightly-test',mds='192.168.9.101',export='/pnfs_test2',commit='a'*40)
r=x.Runner(a,Path({str(root)!r}));r.exercise=lambda:None
sys.exit(r.run())
"""
            process = subprocess.Popen(
                [sys.executable, "-c", code],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                deadline = time.monotonic() + 5
                while not (root / "created").exists():
                    self.assertIsNone(process.poll())
                    if time.monotonic() > deadline:
                        self.fail("prepare did not start")
                    time.sleep(0.02)
                process.send_signal(signal.SIGTERM)
                time.sleep(0.1)
                self.assertFalse((root / "cleaned").exists())
                (root / "release").touch()
                out, err = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 1, (out, err))
                self.assertTrue((root / "cleaned").exists())
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()

    def test_partial_event_timeout_kills_only_owned_process_group(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "pnfs_partial_failure"
            binary.write_text(
                '#!/usr/bin/env python3\nimport os,time\nos.write(1,b"{")\ntime.sleep(30)\n'
            )
            binary.chmod(0o700)
            args = argparse.Namespace(
                run_id="nightly-test",
                mds="192.168.9.101",
                export="/pnfs_test2",
                commit="a" * 40,
            )
            runner = controller.Runner(args, root)
            probe = controller.Probe(runner, "probe", "partial")
            try:
                with self.assertRaisesRegex(EvidenceError, "timed out"):
                    probe.wait({"ready"}, 0.1)
            finally:
                controller.cleanup(root, runner.state)
                probe.close()
            self.assertIsNotNone(probe.process.poll())


if __name__ == "__main__":
    unittest.main()
