# SPDX-License-Identifier: Apache-2.0
"""Verify stage reports and CI failure handling without building the product."""

import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location("validation_runner", ROOT / "scripts/validate.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class ValidationTests(unittest.TestCase):
    def test_failure_stops_remaining_commands_and_reports_executed_stages(self):
        with tempfile.TemporaryDirectory(prefix="validation path with spaces ") as directory:
            report = Path(directory) / "report.json"
            marker = Path(directory) / "unexpected"
            commands = [[sys.executable, "-c", "import sys; print('captured error', file=sys.stderr); sys.exit(7)"],
                        [sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"]]
            with patch.object(runner, "commands", return_value=commands), \
                    patch.object(runner.sys, "stdout", io.TextIOWrapper(io.BytesIO())):
                self.assertEqual(runner.validate(["first", "later"], report, os.environ.copy()), 7)
            result = json.loads(report.read_text())
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["scope"], "executed_stages_only")
            self.assertEqual([(item["stage"], item["exit_code"]) for item in result["stages"]], [("first", 7)])
            self.assertIn("captured error", Path(result["stages"][0]["log"]).read_text())
            self.assertFalse(marker.exists())

    def test_missing_command_records_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            with patch.object(runner, "commands", return_value=[[str(Path(directory) / "missing")]]), \
                    patch.object(runner.sys, "stdout", io.TextIOWrapper(io.BytesIO())):
                self.assertEqual(runner.validate(["missing"], report, os.environ.copy()), 127)
            result = json.loads(report.read_text())
            self.assertEqual(result["stages"][0]["exit_code"], 127)


@unittest.skipUnless(os.name == "posix" and shutil.which("bash"), "CI Bash adapter runs on Linux/macOS")
class LoggingTests(unittest.TestCase):
    def invoke(self, folder, body):
        script = folder / "generated step.sh"
        script.write_text(body)
        environment = os.environ | {"RUNNER_TEMP": str(folder), "GITHUB_STEP_SUMMARY": str(folder / "summary"),
                                    "UC_CI_LOG_NAME": "test/step name"}
        return subprocess.run([sys.executable, str(ROOT / "scripts/ci/run-logged.py"), str(script)],
                              env=environment, capture_output=True, timeout=10)

    def test_stdout_stderr_status_and_summary_survive_logging(self):
        with tempfile.TemporaryDirectory(prefix="CI logs with spaces ") as directory:
            folder = Path(directory)
            result = self.invoke(folder, "echo standard; echo diagnostic >&2; exit 7\n")
            self.assertEqual(result.returncode, 7)
            self.assertEqual((folder / "uc-ci-logs/test_step_name.log").read_bytes(), b"standard\ndiagnostic\n")
            self.assertIn(b"standard\ndiagnostic\n", result.stdout)
            self.assertIn("exit 7", (folder / "summary").read_text())

    def test_pipeline_failure_is_not_masked(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.invoke(Path(directory), "false | true\necho should-not-run\n")
            self.assertEqual(result.returncode, 1)
            self.assertNotIn(b"should-not-run", result.stdout)

    @unittest.skipUnless(sys.platform == "linux", "/dev/full is a Linux fixture")
    def test_log_write_failure_fails_successful_command(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            (folder / "uc-ci-logs").mkdir()
            (folder / "uc-ci-logs/test_step_name.log").symlink_to("/dev/full")
            result = self.invoke(folder, "echo success\n")
            self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
