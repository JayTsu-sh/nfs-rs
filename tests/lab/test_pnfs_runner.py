"""Exercise the real shell gate without contacting the shared NFS lab."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


RUNNER = Path(__file__).with_name("run-pnfs-e2e.sh")


class PnfsRunnerTests(unittest.TestCase):
    def run_gate(self, *, policy=None, ds=False, failure=""):
        with tempfile.TemporaryDirectory(prefix="nfsrs-pnfs-gate-") as directory:
            root = Path(directory)
            commands = {
                "cargo": """#!/bin/bash
set -eu
if [[ "$*" == *nfs_v41_pnfs_write_uses_independent_ds* ]]; then
  [[ "$FAKE_FAILURE" != early ]] || exit 7
  touch "$NFS_RS_LAB_PNFS_READY_FILE"
  for ((i=0; i<1000; i++)); do
    [[ ! -e "$NFS_RS_LAB_PNFS_DONE_FILE" ]] || break
    /bin/sleep 0.005
  done
  [[ -e "$NFS_RS_LAB_PNFS_DONE_FILE" ]] || exit 8
  echo full-readback >> "$FAKE_EVENTS"
  [[ "$FAKE_FAILURE" != readback ]] || exit 9
else
  echo sparse-readback >> "$FAKE_EVENTS"
  [[ "$FAKE_FAILURE" != sparse ]] || exit 10
fi
""",
                "ss": """#!/bin/bash
set -eu
[[ "$FAKE_FAILURE" != observer ]] || exit 11
[[ "$FAKE_FAILURE" != observer-late || ! -e "$FAKE_SS_CALLED" ]] || exit 11
echo 'existing 10.128.56.161:2049'
if [[ -e "$FAKE_SS_CALLED" && "$FAKE_DS" == 1 ]]; then
  echo 'new 10.128.56.161:2049'
fi
touch "$FAKE_SS_CALLED"
""",
                "sleep": "#!/bin/bash\nexec /bin/sleep 0.005\n",
            }
            for name, source in commands.items():
                command = root / name
                command.write_text(source)
                command.chmod(0o755)
            env = os.environ.copy()
            for key in list(env):
                if key.startswith(("LAB_", "NFS_RS_LAB_", "GITHUB_")):
                    del env[key]
            env.update(
                PATH=f"{root}:{env['PATH']}",
                FAKE_EVENTS=str(root / "events"),
                FAKE_SS_CALLED=str(root / "ss-called"),
                FAKE_DS=str(int(ds)),
                FAKE_FAILURE=failure,
                GITHUB_OUTPUT=str(root / "outputs"),
                GITHUB_STEP_SUMMARY=str(root / "summary"),
            )
            if policy is not None:
                env["LAB_PNFS_REQUIRE_INDEPENDENT_DS"] = policy
            result = subprocess.run(
                ["bash", str(RUNNER), "release-gate-regression"],
                env=env, capture_output=True, text=True, timeout=15,
            )
            evidence = {
                name: (root / name).read_text() if (root / name).exists() else ""
                for name in ("events", "outputs", "summary")
            }
            return result, evidence

    def test_explicit_compatibility_mode_checks_both_readbacks(self):
        result, evidence = self.run_gate(policy="0")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(evidence["events"], "full-readback\nsparse-readback\n")
        self.assertIn("independent_ds_observed=false", evidence["outputs"])
        self.assertIn("NOT_OBSERVED", evidence["summary"])
        self.assertIn("NFSv4.1 integrity: PASS", evidence["summary"])

    def test_strict_is_default_and_does_not_accept_preexisting_connections(self):
        result, evidence = self.run_gate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("BLOCKED_CAPABILITY(netapp-pnfs-independent-ds)", result.stderr)
        self.assertNotIn("independent_ds_observed=", evidence["outputs"])

    def test_strict_accepts_new_ds_only_after_both_readbacks(self):
        result, evidence = self.run_gate(ds=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(evidence["events"], "full-readback\nsparse-readback\n")
        self.assertIn("independent_ds_observed=true", evidence["outputs"])

    def test_compatibility_mode_never_hides_io_or_observer_failure(self):
        for failure in ("early", "readback", "sparse", "observer", "observer-late"):
            with self.subTest(failure=failure):
                result, evidence = self.run_gate(policy="0", failure=failure)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("NFSv4.1 integrity: PASS", evidence["summary"])
                self.assertNotIn("independent_ds_observed=", evidence["outputs"])

    def test_invalid_policy_fails_closed(self):
        result, evidence = self.run_gate(policy="optional")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(evidence["events"], "")


if __name__ == "__main__":
    unittest.main()
