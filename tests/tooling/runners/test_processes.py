# SPDX-License-Identifier: Apache-2.0
"""Exercise timeout, interruption and identity protections with owned processes."""

import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
from _support.processes import run, start_ticks, terminate_identity


def active(pid):
    path = Path(f"/proc/{pid}/stat")
    try:
        return path.read_text().rsplit(") ", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


class CommandTests(unittest.TestCase):
    def test_binary_io_and_failure_preserve_exact_status(self):
        result = run([sys.executable, "-c", "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())"], input=b"\0\xffhello")
        self.assertEqual(result.stdout, b"\0\xffhello")
        with self.assertRaises(subprocess.CalledProcessError) as failed:
            run([sys.executable, "-c", "import sys; print('failure', file=sys.stderr); sys.exit(7)"])
        self.assertEqual(failed.exception.returncode, 7)
        self.assertEqual(failed.exception.stderr.strip(), b"failure")

    @unittest.skipUnless(sys.platform == "linux", "Linux process identity inspection")
    def test_timeout_kills_stubborn_descendant_even_after_leader_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            pidfile = Path(directory) / "child.pid"
            child = ("import os,signal,time; from pathlib import Path; "
                     "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                     f"Path({str(pidfile)!r}).write_text(str(os.getpid())); time.sleep(30)")
            parent = ("import subprocess,sys,time; from pathlib import Path; "
                      f"subprocess.Popen([sys.executable, '-c', {child!r}]); "
                      f"\nwhile not Path({str(pidfile)!r}).exists(): time.sleep(.01)")
            started = time.monotonic()
            with self.assertRaises(subprocess.TimeoutExpired):
                run([sys.executable, "-c", parent], timeout=.5)
            self.assertLess(time.monotonic() - started, 5)
            pid = int(pidfile.read_text())
            deadline = time.monotonic() + 2
            while active(pid) and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertFalse(active(pid), "timeout left the command's descendant running")

    @unittest.skipUnless(sys.platform == "linux", "Linux process identity inspection")
    def test_identity_mismatch_does_not_signal_process(self):
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        try:
            terminate_identity((child.pid, start_ticks(child.pid) + 1), grace=.01)
            self.assertIsNone(child.poll())
        finally:
            child.kill()
            child.wait(timeout=3)

    @unittest.skipUnless(sys.platform == "linux", "POSIX signals and Linux process inspection")
    def test_signal_interrupts_runner_and_cleans_child(self):
        with tempfile.TemporaryDirectory() as directory:
            pidfile = Path(directory) / "child.pid"
            child = f"import os,time; from pathlib import Path; Path({str(pidfile)!r}).write_text(str(os.getpid())); time.sleep(30)"
            parent = (f"import sys; sys.path.insert(0, {str(ROOT / 'scripts')!r}); "
                      "from _support.processes import handle_signals,run\n"
                      f"with handle_signals(): run([sys.executable, '-c', {child!r}], timeout=None)")
            process = subprocess.Popen([sys.executable, "-c", parent])
            try:
                deadline = time.monotonic() + 5
                while not pidfile.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(pidfile.exists())
                process.send_signal(signal.SIGTERM)
                self.assertEqual(process.wait(timeout=5), 143)
                self.assertFalse(active(int(pidfile.read_text())))
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=3)


if __name__ == "__main__":
    unittest.main()
