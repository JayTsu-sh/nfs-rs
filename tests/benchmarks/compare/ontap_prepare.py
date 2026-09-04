#!/usr/bin/env python3
"""Idempotent ONTAP REST preparation/rollback for the nfs-rs perf comparison.

prepare:  export policy (nfs3+nfs4, sys, single client) -> flexvol with junction
          -> raise tcp_max_transfer_size to 1 MiB
rollback: optionally restore tcp_max_transfer_size and/or delete the volume
status:   print the current state as JSON

Credentials come from ONTAP_USER / ONTAP_PASS. --dry-run prints the planned
requests without sending anything.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

ONE_MIB = 1048576
DEFAULT_TRANSFER_SIZE = 65536


@dataclass
class Request:
    method: str
    path: str
    body: dict[str, Any] | None = None
    description: str = ""


@dataclass
class Config:
    svm: str
    volume: str
    size_gb: int
    client: str
    junction: str = ""
    policy: str = ""

    def __post_init__(self) -> None:
        self.junction = self.junction or f"/{self.volume}"
        self.policy = self.policy or self.volume


@dataclass
class State:
    svm_uuid: str | None = None
    nfs_service_uuid: str | None = None
    policy_exists: bool = False
    volume: dict[str, Any] | None = None
    transfer_size: int | None = None
    aggregates: list[dict[str, Any]] = field(default_factory=list)


# ----------------------------------------------------------------------------
# pure planning (unit-testable)
# ----------------------------------------------------------------------------

def plan_prepare(state: State, cfg: Config) -> list[Request]:
    plan: list[Request] = []
    if not state.policy_exists:
        plan.append(Request("POST", "/api/protocols/nfs/export-policies", {
            "svm": {"name": cfg.svm},
            "name": cfg.policy,
            "rules": [{
                "clients": [{"match": f"{cfg.client}/32"}],
                "protocols": ["nfs3", "nfs4"],
                "ro_rule": ["sys"], "rw_rule": ["sys"], "superuser": ["sys"],
            }],
        }, f"create export policy {cfg.policy}"))
    if state.volume is None:
        if not state.aggregates:
            raise RuntimeError("no aggregate available for the new volume")
        best = max(state.aggregates, key=lambda a: a.get("space", {}).get("block_storage", {}).get("available", 0))
        plan.append(Request("POST", "/api/storage/volumes", {
            "svm": {"name": cfg.svm},
            "name": cfg.volume,
            "size": cfg.size_gb * 1024 ** 3,
            "aggregates": [{"name": best["name"]}],
            "nas": {
                "path": cfg.junction,
                "export_policy": {"name": cfg.policy},
                "security_style": "unix",
                "unix_permissions": "777",
            },
        }, f"create volume {cfg.volume} ({cfg.size_gb} GB on {best['name']})"))
    if state.transfer_size != ONE_MIB and state.nfs_service_uuid:
        plan.append(Request("PATCH", f"/api/protocols/nfs/services/{state.nfs_service_uuid}",
                            {"transport": {"tcp_max_transfer_size": ONE_MIB}},
                            f"tcp_max_transfer_size {state.transfer_size} -> {ONE_MIB}"))
    return plan


def plan_rollback(state: State, cfg: Config, restore_transfer_size: bool, delete_volume: bool) -> list[Request]:
    plan: list[Request] = []
    if restore_transfer_size and state.transfer_size != DEFAULT_TRANSFER_SIZE and state.nfs_service_uuid:
        plan.append(Request("PATCH", f"/api/protocols/nfs/services/{state.nfs_service_uuid}",
                            {"transport": {"tcp_max_transfer_size": DEFAULT_TRANSFER_SIZE}},
                            f"tcp_max_transfer_size {state.transfer_size} -> {DEFAULT_TRANSFER_SIZE}"))
    if delete_volume and state.volume is not None:
        uuid = state.volume["uuid"]
        plan.append(Request("PATCH", f"/api/storage/volumes/{uuid}", {"state": "offline"}, f"offline {cfg.volume}"))
        plan.append(Request("DELETE", f"/api/storage/volumes/{uuid}", None, f"delete {cfg.volume}"))
    return plan


# ----------------------------------------------------------------------------
# REST client
# ----------------------------------------------------------------------------

class Ontap:
    def __init__(self, mgmt: str, user: str, password: str) -> None:
        self.base = f"https://{mgmt}"
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {"Authorization": f"Basic {token}", "Accept": "application/json",
                        "Content-Type": "application/json"}
        self.ctx = ssl._create_unverified_context()

    def call(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method, headers=self.headers)
        try:
            with urllib.request.urlopen(req, context=self.ctx, timeout=60) as resp:
                text = resp.read().decode()
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"{method} {path} -> {error.code}: {error.read().decode()[:400]}") from error
        return json.loads(text) if text else {}

    def get(self, path: str) -> dict[str, Any]:
        return self.call("GET", path)

    def apply(self, request: Request) -> None:
        print(f"-> {request.method} {request.path}  # {request.description}")
        result = self.call(request.method, request.path, request.body)
        job = result.get("job")
        if job and job.get("uuid"):
            self.wait_job(job["uuid"])

    def wait_job(self, uuid: str) -> None:
        for _ in range(120):
            job = self.get(f"/api/cluster/jobs/{uuid}")
            state = job.get("state")
            if state == "success":
                return
            if state in ("failure", "error"):
                raise RuntimeError(f"job {uuid} failed: {job.get('message')}")
            time.sleep(2)
        raise RuntimeError(f"job {uuid} did not finish in time")


def collect_state(api: Ontap, cfg: Config) -> State:
    state = State()
    svm = api.get(f"/api/svm/svms?name={cfg.svm}&fields=uuid").get("records", [])
    if not svm:
        raise RuntimeError(f"SVM {cfg.svm} not found")
    state.svm_uuid = svm[0]["uuid"]
    services = api.get(f"/api/protocols/nfs/services?svm.name={cfg.svm}&fields=svm.uuid,transport.tcp_max_transfer_size").get("records", [])
    if services:
        state.nfs_service_uuid = services[0]["svm"]["uuid"]
        state.transfer_size = services[0].get("transport", {}).get("tcp_max_transfer_size")
    state.policy_exists = bool(api.get(f"/api/protocols/nfs/export-policies?svm.name={cfg.svm}&name={cfg.policy}").get("records"))
    vols = api.get(f"/api/storage/volumes?svm.name={cfg.svm}&name={cfg.volume}&fields=uuid,state,size,nas.path,nas.export_policy.name").get("records", [])
    state.volume = vols[0] if vols else None
    state.aggregates = api.get("/api/storage/aggregates?fields=name,space.block_storage.available").get("records", [])
    return state


def state_json(state: State) -> dict[str, Any]:
    return {
        "svm_uuid": state.svm_uuid,
        "policy_exists": state.policy_exists,
        "volume": state.volume,
        "tcp_max_transfer_size": state.transfer_size,
        "aggregates": [{"name": a["name"], "available_gb": round(a.get("space", {}).get("block_storage", {}).get("available", 0) / 1024 ** 3, 1)}
                       for a in state.aggregates],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mgmt", required=True)
    parser.add_argument("--svm", required=True)
    parser.add_argument("--volume", default="nfsrs_perf")
    parser.add_argument("--size-gb", type=int, default=50)
    parser.add_argument("--client", required=True, help="client IP allowed by the export policy")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--restore-transfer-size", action="store_true")
    parser.add_argument("--delete-volume", action="store_true")
    parser.add_argument("action", choices=("prepare", "rollback", "status"))
    args = parser.parse_args()
    user, password = os.environ.get("ONTAP_USER"), os.environ.get("ONTAP_PASS")
    if not user or not password:
        print("ONTAP_USER and ONTAP_PASS are required", file=sys.stderr)
        return 2
    cfg = Config(svm=args.svm, volume=args.volume, size_gb=args.size_gb, client=args.client)
    api = Ontap(args.mgmt, user, password)
    state = collect_state(api, cfg)
    print(json.dumps({"before": state_json(state)}, indent=2))
    if args.action == "status":
        return 0
    plan = plan_prepare(state, cfg) if args.action == "prepare" else plan_rollback(
        state, cfg, args.restore_transfer_size, args.delete_volume)
    if not plan:
        print("nothing to do")
        return 0
    if args.dry_run:
        for request in plan:
            print(f"DRY {request.method} {request.path}  # {request.description}")
            if request.body is not None:
                print("    " + json.dumps(request.body))
        return 0
    for request in plan:
        api.apply(request)
    print(json.dumps({"after": state_json(collect_state(api, cfg))}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
