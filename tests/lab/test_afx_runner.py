"""Run ownership and cleanup contracts without touching a storage endpoint."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


RUNNER = Path(__file__).with_name("run-afx-v42-e2e.sh")


class AfxRunnerTests(unittest.TestCase):
    def run_case(self, *, prepare_status=0, rust_status=0, run_id="nightly-v42-test"):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runner = root / RUNNER.name
            runner.write_text(RUNNER.read_text())
            (root / "common.sh").write_text(
                'validate_run_id() { [[ "$1" =~ ^(nightly|release)-[A-Za-z0-9._-]{1,80}$ ]]; }\n'
                'validate_ipv4() { return 0; }\n'
                'validate_export_path() { return 0; }\n'
            )
            commands = {
                "prepare-run.sh": f'echo prepare >> "$LOG"; exit {prepare_status}',
                "cleanup-run.sh": 'echo cleanup >> "$LOG"',
                "lab-test": f'echo rust >> "$LOG"; exit {rust_status}',
                "python-test": 'echo python >> "$LOG"',
                "flock": 'exit 0',
            }
            for name, body in commands.items():
                script = root / name
                script.write_text("#!/usr/bin/env bash\n" + body + "\n")
                script.chmod(0o755)
            log = root / "operations"
            result = subprocess.run(
                ["bash", str(runner), run_id],
                env={
                    **os.environ,
                    "PATH": f"{root}:{os.environ['PATH']}",
                    "LOG": str(log),
                    "NFS_RS_LAB_TEST_BINARY": str(root / "lab-test"),
                    "NFS_RS_LAB_PYTHON": str(root / "python-test"),
                    "NFS_RS_LAB_EVIDENCE_DIR": str(root / "evidence"),
                },
                capture_output=True, text=True, timeout=10,
            )
            operations = log.read_text().splitlines() if log.exists() else []
            return result, operations

    def test_success_runs_rust_and_both_python_suites_then_cleanup(self):
        result, operations = self.run_case()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(operations, ["python", "prepare", "rust", "rust", "python", "python", "cleanup"])

    def test_rust_failure_still_cleans_the_owned_run(self):
        result, operations = self.run_case(rust_status=1)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(operations, ["python", "prepare", "rust", "cleanup"])

    def test_prepare_collision_never_cleans_an_unowned_run(self):
        result, operations = self.run_case(prepare_status=1)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(operations, ["python", "prepare"])

    def test_unsafe_run_id_never_prepares_or_cleans(self):
        result, operations = self.run_case(run_id="../foreign-run")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(operations, [])


if __name__ == "__main__":
    unittest.main()
