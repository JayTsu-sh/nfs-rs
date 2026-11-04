"""Fail-closed packet evidence for one AFX striped WRITE (RFC 8881 §13.4.4)."""

import csv
import io
import ipaddress
import re
import struct
import subprocess


class EvidenceError(RuntimeError):
    pass


FIELDS = [
    "frame.time_epoch",
    "ip.src",
    "ip.dst",
    "tcp.srcport",
    "tcp.dstport",
    "tcp.stream",
    "rpc.xid",
    "rpc.msgtyp",
    "nfs.tag",
    "nfs.opcode",
    "nfs.offset4",
    "nfs.count4",
    "nfs.write.data_length",
    "nfs.fhandle",
    "nfs.status",
]


def rows(pcap, fields=FIELDS, display="rpc && nfs.opcode == 38"):
    command = [
        "tshark",
        "-n",
        "-r",
        str(pcap),
        "-Y",
        display,
        "-T",
        "fields",
        "-E",
        "separator=/t",
        "-E",
        "header=y",
    ]
    for field in fields:
        command.extend(["-e", field])
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise EvidenceError("packet dissection failed")
    return list(csv.DictReader(io.StringIO(result.stdout), delimiter="\t"))


def api_calls(events, kind):
    calls = []
    pending = None
    for event in events:
        if event.get("kind") != kind:
            continue
        if event["phase"] == "write_begin":
            if pending is not None:
                raise EvidenceError("overlapping diagnostic API calls")
            pending = event
        elif event["phase"] in ("write_done", "write_error"):
            if pending is None:
                raise EvidenceError("API completion without admission")
            calls.append((pending, event))
            pending = None
    if pending is not None:
        raise EvidenceError("unfinished diagnostic API call")
    return calls


def in_call(row, begin, end):
    stamp = float(row["frame.time_epoch"])
    return int(begin["unix_ns"]) / 1e9 <= stamp <= int(end["unix_ns"]) / 1e9


def success(reply, count):
    statuses = reply["nfs.status"].split(",")
    return (
        reply["nfs.count4"] == str(count)
        and bool(statuses)
        and all(s == "0" for s in statuses)
    )


def choose_plan(packets, events, mds, allowed):
    """Both stripes must belong to ONE successful public Mount::write call."""
    replies = {
        (x["tcp.stream"], x["rpc.xid"]): x for x in packets if x["rpc.msgtyp"] == "1"
    }
    for begin, end in api_calls(events, "warm"):
        if begin["length"] != 65536 or end.get("count") != 65536:
            continue
        requests = [
            x for x in packets if x["rpc.msgtyp"] == "0" and in_call(x, begin, end)
        ]
        if len(requests) != 2:
            continue
        requests.sort(key=lambda x: int(x["nfs.offset4"]))
        a, b = requests
        if (
            a["nfs.tag"] not in ("ds-write", "ds-write-mds")
            or b["nfs.tag"] not in ("ds-write", "ds-write-mds")
            or a["nfs.write.data_length"] != "32768"
            or b["nfs.write.data_length"] != "32768"
            or int(a["nfs.offset4"]) != begin["offset"]
            or int(b["nfs.offset4"]) != begin["offset"] + 32768
            or not a["nfs.fhandle"]
            or a["nfs.fhandle"] != b["nfs.fhandle"]
            or a["ip.dst"] == b["ip.dst"]
        ):
            continue
        if not all(x["ip.dst"] in allowed for x in requests):
            raise EvidenceError(
                "layout selected an endpoint outside the AFX allow-list"
            )
        if not all(
            (reply := replies.get((x["tcp.stream"], x["rpc.xid"])))
            and in_call(reply, begin, end)
            and success(reply, 32768)
            for x in requests
        ):
            continue
        good, bad = (a, b) if b["ip.dst"] != mds else (b, a)
        return {
            "offset": begin["offset"],
            "length": 65536,
            "stripe_bytes": 32768,
            "good_ds": good["ip.dst"],
            "bad_ds": bad["ip.dst"],
            "bad_port": int(bad["tcp.srcport"]),
            "same_ds_fh": True,
        }
    raise EvidenceError(
        "BLOCKED_CAPABILITY: no same-WRITE independent-DS stripe evidence"
    )


def write_header(raw):
    """Decode a complete request HEADER even when the WRITE body was blocked.

    Requiring TCP record reassembly would hide the deliberately incomplete bad
    stripe. No FH or session identifier from this private decoder is published.
    """
    data = bytes.fromhex(raw.replace(":", ""))
    pos = 0

    def fixed(count):
        nonlocal pos
        if count > len(data) - pos:
            raise ValueError("short header")
        value = data[pos : pos + count]
        pos += count
        return value

    def uint():
        return struct.unpack(">I", fixed(4))[0]

    def opaque():
        count = uint()
        value = fixed(count)
        fixed((-count) % 4)
        return value

    marker = uint()
    xid = uint()
    if not marker & 0x80000000 or (uint(), uint(), uint(), uint(), uint()) != (
        0,
        2,
        100003,
        4,
        1,
    ):
        raise ValueError("not an NFSv4 COMPOUND call")
    uint()
    opaque()
    uint()
    opaque()  # RPC credential/verifier envelopes
    tag = opaque().decode("ascii")
    minor, count = uint(), uint()
    if minor != 1 or count != 3 or uint() != 53:
        raise ValueError("not SEQUENCE, PUTFH, WRITE")
    session = fixed(16)
    sequence, slot, highest, cachethis = uint(), uint(), uint(), uint()
    if uint() != 22:
        raise ValueError("missing PUTFH")
    fh = opaque()
    if uint() != 38:
        raise ValueError("missing WRITE")
    fixed(16)
    offset = struct.unpack(">Q", fixed(8))[0]
    stability, length = uint(), uint()
    return {
        "xid": xid,
        "tag": tag,
        "session": session,
        "sequence": sequence,
        "slot": slot,
        "cachethis": cachethis,
        "fh": fh,
        "offset": offset,
        "length": length,
    }


def headers(pcap):
    fields = [
        "frame.time_epoch",
        "ip.src",
        "ip.dst",
        "tcp.srcport",
        "tcp.dstport",
        "tcp.stream",
        "tcp.seq_raw",
        "tcp.payload",
    ]
    packets = rows(pcap, fields, "tcp.len>0 && tcp.dstport==2049")
    requests, seen = [], set()
    for packet in packets:
        try:
            request = write_header(packet["tcp.payload"])
        except (ValueError, struct.error, UnicodeDecodeError):
            continue
        key = packet["tcp.stream"], packet["tcp.seq_raw"]
        if key in seen:  # TCP retransmission is not a new NFS RPC attempt.
            continue
        seen.add(key)
        request.update(
            {
                "frame.time_epoch": packet["frame.time_epoch"],
                "ds": packet["ip.dst"],
                "stream": packet["tcp.stream"],
            }
        )
        requests.append(request)
    return requests


def audit_fault(requests, packets, events, plan):
    calls = api_calls(events, "fault")
    if len(calls) != 1:
        raise EvidenceError("expected exactly one faulted API write")
    begin, end = calls[0]
    if (
        begin["length"] != 65536
        or begin["offset"] != plan["offset"]
        or not end.get("uncertain")
        or not end.get("verify_then_resume")
    ):
        raise EvidenceError("fault did not produce Uncertain/VerifyThenResume")
    fault = [x for x in requests if in_call(x, begin, end)]
    good = [x for x in fault if x["ds"] == plan["good_ds"]]
    bad = [x for x in fault if x["ds"] == plan["bad_ds"]]
    if (
        not good
        or not bad
        or {x["ds"] for x in fault} != {plan["good_ds"], plan["bad_ds"]}
    ):
        raise EvidenceError("missing sibling request or unexpected WRITE endpoint")
    if (
        any(
            x["length"] != 32768 or x["tag"] not in ("ds-write", "ds-write-mds")
            for x in fault
        )
        or {x["offset"] for x in fault} != {plan["offset"], plan["offset"] + 32768}
        or len({x["fh"] for x in fault}) != 1
    ):
        raise EvidenceError("wrong stripe mapping or MDS replay")
    # Also inspect fully dissected WRITEs, including shapes our header decoder
    # cannot parse. Unknown WRITE shapes must not make a fallback disappear.
    for packet in packets:
        if packet["rpc.msgtyp"] == "0" and in_call(packet, begin, end):
            if not any(
                x["stream"] == packet["tcp.stream"]
                and x["xid"] == int(packet["rpc.xid"], 16)
                for x in fault
            ):
                raise EvidenceError("unrecognized WRITE during the fault interval")
    if not any(
        packet["rpc.msgtyp"] == "1"
        and in_call(packet, begin, end)
        and success(packet, 32768)
        and any(
            packet["tcp.stream"] == x["stream"]
            and int(packet["rpc.xid"], 16) == x["xid"]
            for x in good
        )
        for packet in packets
    ):
        raise EvidenceError("no authoritative successful sibling reply")
    return {
        "same_ds_fh": True,
        "good_ds_success_observed": True,
        "mds_fallback_write_count": 0,
        "fault_elapsed_s": (int(end["unix_ns"]) - int(begin["unix_ns"])) / 1e9,
    }


def capture_complete(text):
    counts = []
    for pattern in (
        r"(\d+) packets captured",
        r"(\d+) packets received by filter",
        r"(\d+) packets dropped by kernel",
    ):
        match = re.search(pattern, text)
        if match is None:
            raise EvidenceError("missing packet capture completion counters")
        counts.append(int(match.group(1)))
    if counts[0] == 0 or counts[0] != counts[1] or counts[2] != 0:
        raise EvidenceError("packet capture is incomplete")
    return {"captured": counts[0], "received": counts[1], "dropped": counts[2]}


def owned_socket(text, pid, peer, port, source):
    ipaddress.IPv4Address(peer)
    ipaddress.IPv4Address(source)
    matches = []
    for line in text.splitlines():
        columns = line.split()
        if (
            len(columns) >= 6
            and columns[0] == "ESTAB"
            and columns[3] == f"{source}:{port}"
            and columns[4] == f"{peer}:2049"
            and re.search(rf"\bpid={pid},", line)
        ):
            matches.append(line)
    if len(matches) != 1:
        raise EvidenceError("fault socket is not uniquely owned by the probe")


def validate_result(result, commit, probe_sha256):
    """A zero SSH exit is insufficient without a complete exact-build report."""
    try:
        plan, wire, capture = result["plan"], result["wire"], result["capture"]
        valid = (
            result["schema_version"] == 1
            and result["status"] == "pass"
            and result["cleanup"] == "pass"
            and result["commit"] == commit
            and result["probe_sha256"] == probe_sha256
            and result["bytes_verified"] == 20 * 1024**3
            and re.fullmatch(r"[0-9a-f]{64}", result["sha256"])
            and re.fullmatch(r"[0-9a-f]{64}", result["pcap_sha256"])
            and plan["good_ds"] != plan["bad_ds"]
            and plan["length"] == 65536
            and plan["stripe_bytes"] == 32768
            and wire["same_ds_fh"]
            and wire["good_ds_success_observed"]
            and wire["mds_fallback_write_count"] == 0
            and set(result["partial_mutation"]) == {"seed", "fault"}
            and result["dropped_packets"] > 0
            and capture["captured"] == capture["received"] > 0
            and capture["dropped"] == 0
        )
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        raise EvidenceError("incomplete or wrong-build public T15 acceptance report")


if __name__ == "__main__":
    import json
    import sys
    from pathlib import Path

    validate_result(json.loads(Path(sys.argv[1]).read_text()), sys.argv[2], sys.argv[3])
