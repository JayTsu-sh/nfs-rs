import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest


class MemoryRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.checkout = self.root / "checkout"
        lab = self.checkout / "tests/lab"
        lab.mkdir(parents=True)
        binary_dir = self.root / "bin"
        binary_dir.mkdir()
        source = Path(__file__).parent
        runner = (source / "run-memory-benchmark.sh").read_text()
        runner = runner.replace("/tmp/terrasync-lab-tests.lock", str(self.root / "tests.lock"))
        runner = runner.replace("/tmp/terrasync-lab-performance.lock", str(self.root / "performance.lock"))
        self.runner = lab / "run-memory-benchmark.sh"
        self.runner.write_text(runner)
        shutil.copyfile(source / "common.sh", lab / "common.sh")
        self.program(binary_dir / "git", "print(__import__('os').environ['FIXTURE_CHECKOUT'])")
        self.program(binary_dir / "flock", "pass")
        self.program(binary_dir / "benchmark", "pass")
        self.program(lab / "prepare-run.sh", """
import os,time
from pathlib import Path
root=Path(os.environ['FIXTURE_ROOT'])
(root/'preparing').touch()
if os.environ.get('COLLISION'): raise SystemExit(3)
while os.environ.get('PAUSE_PREPARE') and not (root/'release_prepare').exists(): time.sleep(.01)
(root/'prepared').touch()
""")
        self.program(lab / "cleanup-run.sh", """
import os
from pathlib import Path
root=Path(os.environ['FIXTURE_ROOT'])
if (root/'controller_started').exists() and not (root/'controller_stopped').exists():
 (root/'cleanup_before_stop').touch()
 raise SystemExit(1)
if (root/'worker_pid').exists():
 try: os.kill(int((root/'worker_pid').read_text()),0)
 except ProcessLookupError: pass
 else:
  (root/'cleanup_before_stop').touch()
  raise SystemExit(1)
(root/'cleaned').touch()
""")
        self.program(binary_dir / "python3", """
import os,signal,subprocess,sys,time
from pathlib import Path
root=Path(os.environ['FIXTURE_ROOT'])
worker=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])
(root/'worker_pid').write_text(str(worker.pid))
def stop(*_): raise KeyboardInterrupt()
signal.signal(signal.SIGTERM,stop)
(root/'controller_started').touch()
try:
 if os.environ.get('FAIL_CONTROLLER'): raise SystemExit(7)
 while not (root/'release_controller').exists(): time.sleep(.01)
except KeyboardInterrupt: raise SystemExit(130)
finally:
 worker.kill()
 worker.wait()
 (root/'controller_stopped').touch()
""")
        self.environment = {
            **os.environ, "PATH": str(binary_dir) + os.pathsep + os.environ["PATH"],
            "FIXTURE_ROOT": str(self.root), "FIXTURE_CHECKOUT": str(self.checkout),
            "NFS_RS_MEMORY_BINARY": str(binary_dir / "benchmark"),
            "LAB_MEMORY_PREPARE_DATA": "127.0.0.1", "LAB_MEMORY_PREPARE_EXPORT": "/fixture",
        }

    @staticmethod
    def program(path, body):
        path.write_text(f"#!{sys.executable}\n" + body + "\n")
        path.chmod(0o755)

    def start(self, **environment):
        process = subprocess.Popen(
            ["bash", str(self.runner), "release-signal-fixture", str(self.root / "evidence"),
             "nfs://127.0.0.1/fixture?version=4.1"],
            cwd=self.checkout, env={**self.environment, **environment},
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True,
        )

        def finish():
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate(timeout=5)

        self.addCleanup(finish)
        return process

    def wait_for(self, name):
        deadline = time.monotonic() + 5
        while not (self.root / name).exists():
            if time.monotonic() > deadline:
                self.fail(f"fixture did not reach {name}")
            time.sleep(.01)

    def test_termination_waits_for_controller_and_workers_before_cleanup(self):
        self.check_signal(signal.SIGTERM, 143)

    def test_interrupt_preserves_nonzero_status_and_cleans_after_workers_stop(self):
        self.check_signal(signal.SIGINT, 130)

    def check_signal(self, sig, expected):
        process = self.start()
        self.wait_for("controller_started")
        process.send_signal(sig)
        result = process.wait(timeout=5)
        self.assertFalse((self.root / "cleanup_before_stop").exists())
        self.assertTrue((self.root / "controller_stopped").exists())
        self.assertTrue((self.root / "cleaned").exists())
        self.assertEqual(result, expected)

    def test_normal_completion_cleans_after_workers_stop(self):
        process = self.start()
        self.wait_for("controller_started")
        (self.root / "release_controller").touch()
        self.assertEqual(process.wait(timeout=5), 0)
        self.assertTrue((self.root / "cleaned").exists())
        self.assertFalse((self.root / "cleanup_before_stop").exists())

    def test_controller_failure_is_preserved_after_cleanup(self):
        process = self.start(FAIL_CONTROLLER="1")
        self.assertEqual(process.wait(timeout=5), 7)
        self.assertTrue((self.root / "cleaned").exists())

    def test_failed_preparation_never_authorizes_cleanup(self):
        process = self.start(COLLISION="1")
        self.assertEqual(process.wait(timeout=5), 3)
        self.assertFalse((self.root / "cleaned").exists())

    def test_signal_during_preparation_waits_for_ownership_before_cleanup(self):
        process = self.start(PAUSE_PREPARE="1")
        self.wait_for("preparing")
        process.send_signal(signal.SIGTERM)
        (self.root / "release_prepare").touch()
        self.assertEqual(process.wait(timeout=5), 143)
        self.assertTrue((self.root / "prepared").exists())
        self.assertTrue((self.root / "cleaned").exists())
        self.assertFalse((self.root / "controller_started").exists())


if __name__ == "__main__":
    unittest.main()
