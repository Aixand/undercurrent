#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Keep documentation recordings limited to the standalone sample workflows."""

import importlib.util
from pathlib import Path
import subprocess
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/docs/render_demos.py"
SPEC = importlib.util.spec_from_file_location("recording_runner", SCRIPT)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)
LOCAL_DEMOS = {
    "overview", "python-control", "interaction", "orchestration", "server-health"
}


class RecordingRunnerTests(unittest.TestCase):
    def test_help_exposes_only_the_standalone_recordings(self):
        result = subprocess.run(
            [sys.executable, "-B", str(SCRIPT), "--help"],
            text=True, capture_output=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        choices = result.stdout.split("{", 1)[1].split("}", 1)[0].split(",")
        self.assertEqual(set(choices), LOCAL_DEMOS | {"all"})
        for name in LOCAL_DEMOS:
            self.assertTrue((RUNNER.ASSETS / f"{name}.tape").is_file())
            self.assertTrue((RUNNER.ASSETS / f"{name}.gif").is_file())
            self.assertTrue((RUNNER.ASSETS / f"{name}-poster.png").is_file())

    def test_all_records_each_local_workflow_without_extra_launches(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "previews"
            arguments = [str(SCRIPT), "all", "--output-dir", str(output)]
            with patch.object(sys, "argv", arguments), \
                    patch.object(RUNNER.sys, "platform", "linux"), \
                    patch.object(RUNNER.shutil, "which", return_value="fixture-tool"), \
                    patch.object(RUNNER.os, "access", return_value=True), \
                    patch.object(RUNNER, "record") as record:
                RUNNER.main()
            names = [call.args[0] for call in record.call_args_list]
            self.assertEqual(len(names), len(LOCAL_DEMOS))
            self.assertEqual(set(names), LOCAL_DEMOS)

    def test_server_shutdown_check_rejects_an_open_listener(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            with self.assertRaisesRegex(RuntimeError, "still listening"):
                RUNNER.verify_listener_closed(listener.getsockname()[1])

    def test_server_shutdown_check_accepts_a_closed_listener(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        RUNNER.verify_listener_closed(port)

    def test_server_shutdown_check_does_not_treat_timeout_as_shutdown(self):
        with patch.object(RUNNER.socket, "create_connection", side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                RUNNER.verify_listener_closed(12345)

    def test_unknown_recording_fails_before_starting_work(self):
        with patch.object(sys, "argv", [str(SCRIPT), "unknown-recording"]), \
                patch.object(RUNNER, "record") as record:
            with self.assertRaises(SystemExit) as error:
                RUNNER.main()
        self.assertEqual(error.exception.code, 2)
        record.assert_not_called()


if __name__ == "__main__":
    unittest.main()
