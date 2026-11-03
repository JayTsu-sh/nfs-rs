import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    "memory_benchmark", Path(__file__).with_name("memory-benchmark.py"))
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def fixture():
    def phase(name, large, hwm=10240):
        return {"phase": name, "large_live_bytes": large, "live_bytes": large + 4096, "hwm_kib": hwm,
                "peak_live_bytes": 2**20, "seconds": 2}

    return {
        "schema_version": 1, "status": "pass", "large_allocations_released": True,
        "config": {"mode": "read", "rounds": 3, "payload_bytes": 2**20},
        "phases": [phase("runtime", 0), phase("read-0", 0, 20480), phase("idle-0", 0),
                   phase("read-1", 0), phase("idle-1", 0), phase("read-2", 0),
                   phase("idle-2", 0), phase("unmounted", 0)],
    }


class MemoryBenchmarkTests(unittest.TestCase):
    def test_does_not_accept_a_release_flag_when_large_allocations_remain(self):
        report = fixture()
        report["phases"][-1]["large_live_bytes"] = 2**20
        with self.assertRaisesRegex(ValueError, "release"):
            benchmark.analyze(report, 1024 * 1024)

    def test_reports_phase_peak_and_actual_live_plateau(self):
        result = benchmark.analyze(fixture(), 1024 * 1024)
        self.assertEqual(result["peak_rss_kib"], 20480)
        self.assertEqual(result["plateau"], "pass")
        self.assertEqual(result["tail_large_live_range_bytes"], 0)
        self.assertEqual(result["io"][0]["mib_s"], 0.5)

    def test_missing_rounds_and_growing_live_tail_fail_closed(self):
        report = fixture()
        report["phases"].pop(6)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            benchmark.analyze(report, 1024 * 1024)
        report = fixture()
        report["phases"][4]["large_live_bytes"] = 2**20
        report["phases"][6]["large_live_bytes"] = 3 * 2**20
        with self.assertRaisesRegex(ValueError, "plateau"):
            benchmark.analyze(report, 1024 * 1024)

    def test_one_round_does_not_claim_a_steady_state_pass(self):
        report = fixture()
        report["config"]["rounds"] = 1
        report["phases"] = report["phases"][:3] + report["phases"][-1:]
        result = benchmark.analyze(report, 1024 * 1024)
        self.assertEqual(result["plateau"], "not_evaluated_insufficient_rounds")

    def test_small_allocation_growth_cannot_hide_below_the_large_block_cutoff(self):
        report = fixture()
        for index, live in [(2, 2**20), (4, 2 * 2**20), (6, 4 * 2**20)]:
            report["phases"][index]["live_bytes"] = live
        with self.assertRaisesRegex(ValueError, "plateau"):
            benchmark.analyze(report, 1024 * 1024)

    def test_reader_processes_start_only_after_every_reader_is_ready(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            worker = root / "worker.py"
            worker.write_text(
                "import json,sys\n"
                "from pathlib import Path\n"
                "root=Path(sys.argv[1]); index=sys.argv[2]\n"
                "(root / ('ready-' + index)).touch()\n"
                "print(json.dumps({'status':'ready'}), flush=True)\n"
                "assert sys.stdin.readline().strip() == 'go'\n"
                "assert len(list(root.glob('ready-*'))) == 3\n"
                "print(json.dumps({'worker':index}), flush=True)\n")
            commands = [[sys.executable, str(worker), name, str(i)] for i in range(3)]
            results = benchmark.readers(commands, 10)
            self.assertEqual([code for code, _, _ in results], [0, 0, 0])
            self.assertEqual([json.loads(out)["worker"] for _, out, _ in results], ["0", "1", "2"])


if __name__ == "__main__":
    unittest.main()
