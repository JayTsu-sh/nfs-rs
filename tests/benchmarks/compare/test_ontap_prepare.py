from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import ontap_prepare as op  # noqa: E402

CFG = op.Config(svm="lizy", volume="nfsrs_perf", size_gb=50, client="10.131.6.181")


def _aggr(name: str, available_gb: int) -> dict:
    return {"name": name, "space": {"block_storage": {"available": available_gb * 1024 ** 3}}}


def test_prepare_from_scratch_creates_policy_volume_and_raises_transfer_size() -> None:
    state = op.State(svm_uuid="s", nfs_service_uuid="n", policy_exists=False, volume=None,
                     transfer_size=65536, aggregates=[_aggr("a1", 100), _aggr("a2", 900)])
    plan = op.plan_prepare(state, CFG)
    assert [(r.method, r.path) for r in plan] == [
        ("POST", "/api/protocols/nfs/export-policies"),
        ("POST", "/api/storage/volumes"),
        ("PATCH", "/api/protocols/nfs/services/n"),
    ]
    policy, volume, nfs = plan
    assert policy.body["rules"][0]["protocols"] == ["nfs3", "nfs4"]
    assert policy.body["rules"][0]["clients"] == [{"match": "10.131.6.181/32"}]
    assert volume.body["aggregates"] == [{"name": "a2"}]
    assert volume.body["nas"]["path"] == "/nfsrs_perf"
    assert volume.body["nas"]["export_policy"] == {"name": "nfsrs_perf"}
    assert volume.body["size"] == 50 * 1024 ** 3
    assert nfs.body == {"transport": {"tcp_max_transfer_size": 1048576}}


def test_prepare_is_idempotent() -> None:
    state = op.State(svm_uuid="s", nfs_service_uuid="n", policy_exists=True, volume={"uuid": "v"},
                     transfer_size=1048576, aggregates=[_aggr("a1", 100)])
    assert op.plan_prepare(state, CFG) == []


def test_rollback_restores_size_and_deletes_volume_only_when_asked() -> None:
    state = op.State(svm_uuid="s", nfs_service_uuid="n", policy_exists=True, volume={"uuid": "v"},
                     transfer_size=1048576)
    assert op.plan_rollback(state, CFG, restore_transfer_size=False, delete_volume=False) == []
    plan = op.plan_rollback(state, CFG, restore_transfer_size=True, delete_volume=True)
    assert [(r.method, r.path) for r in plan] == [
        ("PATCH", "/api/protocols/nfs/services/n"),
        ("PATCH", "/api/storage/volumes/v"),
        ("DELETE", "/api/storage/volumes/v"),
    ]
    assert plan[0].body == {"transport": {"tcp_max_transfer_size": 65536}}
