# SPDX-License-Identifier: Apache-2.0
"""Check discovery, opt-in boundaries and failure propagation without a daemon."""

import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("e2e_runner", ROOT / "tests/e2e/run.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class RunnerTests(unittest.TestCase):
    def test_catalog_covers_every_script_and_points_to_real_commands(self):
        catalog = runner.load_catalog()
        self.assertTrue(catalog)
        self.assertIn("attach-scrolling", {test["id"] for test in catalog})

    def test_default_discovery_never_executes_or_creates_reports(self):
        with patch.object(runner.subprocess, "run") as execute, redirect_stdout(io.StringIO()):
            self.assertEqual(runner.main([]), 0)
        execute.assert_not_called()

    def test_group_excludes_helpers_and_other_platforms(self):
        catalog = [dict(id="ordinary", group="cli", mode="manual", platforms=["linux"]),
                   dict(id="helper", group="cli", mode="helper", platforms=["linux"], notes="needs a live PID"),
                   dict(id="windows", group="cli", mode="ci", platforms=["win32"])]
        self.assertEqual([test["id"] for test in runner.select_tests(catalog, [], "cli", "linux")], ["ordinary"])
        with self.assertRaisesRegex(ValueError, "supporting helper"):
            runner.select_tests(catalog, ["helper"], None, "linux")

    def test_unknown_names_and_wrong_platform_fail_before_execution(self):
        catalog = runner.load_catalog()
        with self.assertRaisesRegex(ValueError, "unknown E2E"):
            runner.select_tests(catalog, ["typo"], None, "linux")
        with self.assertRaisesRegex(ValueError, "host is linux"):
            runner.select_tests(catalog, ["windows-native"], None, "linux")

    def test_missing_binary_fails_before_any_test_or_report(self):
        with tempfile.TemporaryDirectory() as directory:
            reports = Path(directory) / "reports"
            with patch.object(runner.subprocess, "run") as execute, self.assertRaises(SystemExit) as error:
                runner.main(["--test", "attach-picker", "--bin-dir", directory, "--report-dir", str(reports)])
            self.assertEqual(error.exception.code, 2)
            execute.assert_not_called()
            self.assertFalse(reports.exists())

    def test_failure_stops_group_and_records_only_executed_test(self):
        with tempfile.TemporaryDirectory(prefix="e2e runner space ") as directory:
            root = Path(directory).resolve()
            fake = dict(id="failure", path="tests/e2e/failure.py", group="cli", platforms=[runner.sys.platform],
                        mode="manual", requires=[], binaries=[], command=["{python}", "failure.py", "{bin_dir}"], notes="")
            later = fake | {"id": "later"}
            with patch.object(runner, "load_catalog", return_value=[fake, later]), \
                    patch.object(runner.subprocess, "run", return_value=subprocess.CompletedProcess([], 7)) as execute, \
                    redirect_stdout(io.StringIO()):
                status = runner.main(["--group", "cli", "--bin-dir", str(root / "binary directory"), "--report-dir", str(root)])
            self.assertEqual(status, 7)
            execute.assert_called_once()
            self.assertEqual(execute.call_args.args[0][-1], str(root / "binary directory"))
            self.assertEqual(execute.call_args.kwargs["env"]["UC_E2E_BIN_DIR"], str(root / "binary directory"))
            report = json.loads(next(root.glob("run-*/report.json")).read_text())
            self.assertEqual(report["tests"], [{"id": "failure", "exit_code": 7}])
            self.assertEqual(report["scope"], "executed_tests_only")


if __name__ == "__main__":
    unittest.main()
