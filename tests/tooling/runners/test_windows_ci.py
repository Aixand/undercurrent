# SPDX-License-Identifier: Apache-2.0
"""Exercise failure reporting and cleanup without building the product."""

import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("windows_ci", ROOT / "scripts/ci/windows-native.py")
CI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CI)


class WindowsCiTests(unittest.TestCase):
    def test_redirected_unicode_output_preserves_evidence_and_exit_code(self):
        output = io.BytesIO()
        stream = io.TextIOWrapper(output, encoding="cp1252")
        command = [sys.executable, "-c", "print('\\u2717 failure \\u2192 evidence'); raise SystemExit(19)"]
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(CI.sys, "stdout", stream), patch.object(CI.sys, "stderr", stream), \
                    patch.object(CI, "commands", return_value=[command]):
                CI.configure_output()
                result = CI.run_stage("unicode", dict(os.environ, PYTHONIOENCODING="utf-8"), Path(folder))
            stream.flush()
            evidence = (Path(folder) / "unicode.log").read_text(encoding="utf-8")
        self.assertEqual(result["exit_code"], 19)
        self.assertIn("\u2717 failure \u2192 evidence", evidence)
        self.assertEqual(output.getvalue().decode("utf-8").splitlines(), evidence.splitlines())

    def test_failed_command_retains_output_and_stops_remaining_commands(self):
        with tempfile.TemporaryDirectory() as folder:
            logs = Path(folder)
            marker = logs / "should-not-exist"
            commands = [
                [sys.executable, "-c", "print('failure evidence'); raise SystemExit(17)"],
                [sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"],
            ]
            with patch.object(CI, "commands", return_value=commands):
                result = CI.run_stage("probe", dict(os.environ), logs)
            self.assertEqual(result["exit_code"], 17)
            self.assertIn("failure evidence", (logs / "probe.log").read_text())
            self.assertFalse(marker.exists())

    def exercise(self, fail_stage, fail_code, interrupt=False):
        calls = []

        def run(stage, _environment, _logs):
            calls.append(stage)
            if stage == fail_stage and interrupt:
                raise KeyboardInterrupt
            return {"stage": stage, "exit_code": fail_code if stage == fail_stage else 0}

        with tempfile.TemporaryDirectory() as folder:
            with patch.object(CI, "run_stage", side_effect=run), \
                    patch.object(CI, "source_record", return_value={"revision": "test-fixture"}), \
                    patch.object(CI.ctypes, "windll", create=True):
                if interrupt:
                    with self.assertRaises(KeyboardInterrupt):
                        CI.validate(list(CI.STAGES), {}, Path(folder))
                    code = None
                else:
                    code = CI.validate(list(CI.STAGES), {}, Path(folder))
            report = json.loads((Path(folder) / "report.json").read_text())
        return calls, code, report

    def test_failure_after_install_uninstalls_and_preserves_original_exit(self):
        calls, code, report = self.exercise("installed-smoke", 23)
        self.assertEqual(code, 23)
        self.assertEqual(calls[-2:], ["installed-smoke", "uninstall"])
        self.assertNotIn("attach", calls)
        self.assertEqual(report["status"], "failed")

    def test_failed_cleanup_cannot_report_a_successful_workflow(self):
        calls, code, report = self.exercise("uninstall", 29)
        self.assertEqual(code, 29)
        self.assertEqual(calls.count("uninstall"), 1)
        self.assertEqual(report["status"], "failed")

    def test_interruption_still_uninstalls_and_never_reports_passed(self):
        calls, _, report = self.exercise("attach", 0, interrupt=True)
        self.assertEqual(calls[-1], "uninstall")
        self.assertEqual(report["status"], "interrupted")


if __name__ == "__main__":
    unittest.main()
