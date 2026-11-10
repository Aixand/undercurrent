#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Linux isolated-home regressions for immediate lifecycle control under load."""

from contextlib import contextmanager
from pathlib import Path
import argparse
import fcntl
import json
import os
import pty
import select
import selectors
import signal
import struct
import subprocess
import sys
import tempfile
import termios
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--artifact-dir",
    type=Path,
    default=Path(__file__).resolve().parents[3] / "target/debug",
)
args = parser.parse_args()
if sys.platform != "linux":
    raise SystemExit("This process identity test requires Linux /proc")
binary = str(args.artifact_dir.resolve() / "uc")
label = "lifecycle"

program = """import os,signal,sys,time
mode=sys.argv[3]
def on_term(*_):
    open(sys.argv[2],'w').close()
    if mode=='responsive': sys.exit(0)
signal.signal(signal.SIGTERM,on_term)
with open(sys.argv[1],'w') as out: out.write(str(os.getpid()))
time.sleep(120)
"""


def wait(predicate, seconds=5):
    end = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() > end:
            raise RuntimeError("condition timeout")
        time.sleep(0.005)


@contextmanager
def scenario(name):
    with tempfile.TemporaryDirectory(prefix="uc-lifecycle-e2e-") as directory:
        root = Path(directory)
        home = root / "home"
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("UC_", "UNDERCURRENT_"))
        }
        env["UNDERCURRENT_HOME"] = str(home)
        clients = []

        class Rig:
            home_dir = home
            environment = env
            processes = clients

            def cli(self, *args, check=True):
                p = subprocess.run(
                    [binary, *args], env=env, capture_output=True, text=True, timeout=28
                )
                if check and p.returncode:
                    raise RuntimeError((name, args, p.stdout, p.stderr))
                return p

            def launch(self, *args):
                p = subprocess.Popen(
                    [binary, *args],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                clients.append(p)
                return p

            def start(self, n="worker", behavior="ignore", grace=None, labels=()):
                marker = root / (n + ".pid")
                term = root / (n + ".term")
                args = ["start", n, "--no-detect"]
                if grace:
                    args += ["--stop-timeout", grace]
                for l in labels:
                    args += ["--label", l]
                args += [
                    "--",
                    sys.executable,
                    "-u",
                    "-c",
                    program,
                    str(marker),
                    str(term),
                    behavior,
                ]
                self.cli(*args)
                wait(lambda: marker.exists() and marker.read_text())
                return term

            def watch(self):
                p = self.launch("watch", "--jsonl")
                with selectors.DefaultSelector() as sel:
                    sel.register(p.stdout, selectors.EVENT_READ)
                    if not sel.select(5):
                        raise RuntimeError("watch activation failed")
                assert json.loads(p.stdout.readline())["record"] == "session_snapshot"

            def measure(self, *args, check=True, extra=None):
                begin = time.monotonic()
                p = self.cli(*args, check=check)
                row = {
                    "build": label,
                    "case": name,
                    "command": " ".join(args),
                    "seconds": round(time.monotonic() - begin, 3),
                    "exit_code": p.returncode,
                }
                if extra:
                    row.update(extra)
                if p.returncode:
                    row["error"] = p.stderr.splitlines()[0]
                print(json.dumps(row), flush=True)
                if check:
                    assert time.monotonic() - begin < 2.5, row
                return p

        rig = Rig()
        try:
            rig.cli("daemon", "start")
            yield rig
        finally:
            for p in clients:
                if p.poll() is None:
                    p.kill()
                p.communicate(timeout=3)
            expected = ("UNDERCURRENT_HOME=" + str(home)).encode()
            for p in Path("/proc").iterdir():
                if not p.name.isdecimal():
                    continue
                try:
                    handle = os.pidfd_open(int(p.name))
                except (PermissionError, ProcessLookupError):
                    continue
                try:
                    if expected in (p / "environ").read_bytes().split(b"\0"):
                        signal.pidfd_send_signal(handle, signal.SIGKILL)
                except (FileNotFoundError, PermissionError, ProcessLookupError):
                    pass
                finally:
                    os.close(handle)


with scenario("immediate_stop_and_restart") as r:
    term = r.start(grace="20s")
    r.watch()
    r.measure("restart", "worker")
    assert not term.exists(), "immediate restart sent a graceful signal"
    r.measure("stop", "worker")
    r.measure("clean", "worker", "--yes")

with scenario("force_interrupts_timed_out_stop") as r:
    term = r.start(grace="20s")
    result = r.measure(
        "--timeout", "200ms", "stop", "worker", "--graceful", check=False
    )
    assert result.returncode != 0
    wait(term.exists)
    r.measure("stop", "worker")
    r.measure("clean", "worker", "--yes")

with scenario("control_under_saturated_graceful_workers") as r:
    terms = [
        r.start("slow" + str(i), grace="20s", labels=["batch=slow"]) for i in range(8)
    ]
    r.start("urgent")
    r.watch()
    pending = r.launch("stop", "--label", "batch=slow", "--graceful")
    wait(lambda: all(p.exists() for p in terms))
    r.measure("daemon", "status")
    r.measure("stop", "urgent")
    # Also exercise a queued lease acquisition when immediate shutdown cancels it.
    duplicate = r.launch("stop", "slow0", "--graceful")
    time.sleep(0.1)
    r.measure("daemon", "restart")
    assert (r.home_dir / "state/undercurrent.db").exists()
    pending.communicate(timeout=5)
    duplicate.communicate(timeout=5)
    assert len(json.loads(r.cli("ls", "--json").stdout)["items"]) == 9
    r.start("after")
    r.measure("stop", "after")
    r.measure("clean", "--all", "--yes")

with scenario("daemon_restart_interrupts_session_restart") as r:
    term = r.start(grace="20s")
    pending = r.launch("restart", "worker", "--graceful")
    wait(term.exists)
    r.measure("daemon", "restart")
    pending.communicate(timeout=5)
    sessions = json.loads(r.cli("ls", "--json").stdout)["items"]
    assert len(sessions) == 1 and sessions[0]["lifecycle"]["state"] in (
        "stopped",
        "exited",
    )
    r.measure("clean", "--all", "--yes")

with scenario("reset_under_saturated_graceful_workers") as r:
    terms = [
        r.start("slow" + str(i), grace="20s", labels=["batch=slow"]) for i in range(8)
    ]
    pending = r.launch("stop", "--label", "batch=slow", "--graceful")
    wait(lambda: all(p.exists() for p in terms))
    r.measure("reset", "--yes")
    assert not (r.home_dir / "state/undercurrent.db").exists()
    pending.communicate(timeout=5)

with scenario("graceful_stop_cleans_surviving_descendant") as r:
    marker = r.home_dir.parent / "tree-child"
    tree_program = """import os,signal,subprocess,sys,time
signal.signal(signal.SIGTERM,lambda *_:sys.exit(0))
child_code='import os,signal,sys,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); signal.signal(signal.SIGHUP,signal.SIG_IGN); open(sys.argv[1],"w").write(str(os.getpid())); time.sleep(120)'
child=subprocess.Popen([sys.executable,'-c',child_code,sys.argv[1]])
time.sleep(120)
"""
    r.cli(
        "start",
        "tree",
        "--no-detect",
        "--stop-timeout",
        "200ms",
        "--",
        sys.executable,
        "-c",
        tree_program,
        str(marker),
    )
    wait(lambda: marker.exists() and marker.read_text())
    child = int(marker.read_text())
    handle = os.pidfd_open(child)
    try:
        r.measure("stop", "tree", "--graceful")
        with selectors.DefaultSelector() as selector:
            selector.register(handle, selectors.EVENT_READ)
            assert selector.select(2), "child survived confirmed stop"
        r.measure("clean", "tree", "--yes")
    finally:
        os.close(handle)

with scenario("reset_cleans_descendants_after_root_exit") as r:
    marker = r.home_dir.parent / "orphan-child"
    root_pid = r.home_dir.parent / "orphan-root"
    tree_program = """import os,subprocess,sys,time
child_code='import os,signal,sys,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); signal.signal(signal.SIGHUP,signal.SIG_IGN); open(sys.argv[1],"w").write(str(os.getpid())); time.sleep(120)'
subprocess.Popen([sys.executable,'-c',child_code,sys.argv[1]])
while not os.path.exists(sys.argv[1]): time.sleep(.005)
open(sys.argv[2],'w').write(str(os.getpid()))
"""
    r.cli(
        "start",
        "tree",
        "--no-detect",
        "--",
        sys.executable,
        "-c",
        tree_program,
        str(marker),
        str(root_pid),
    )
    wait(lambda: root_pid.exists() and root_pid.read_text())
    wait(lambda: not Path("/proc/" + root_pid.read_text()).exists())
    handle = os.pidfd_open(int(marker.read_text()))
    try:
        r.measure("reset", "--yes")
        with selectors.DefaultSelector() as selector:
            selector.register(handle, selectors.EVENT_READ)
            assert selector.select(2), "child survived reset after root exit"
    finally:
        os.close(handle)

with scenario("group_delete_parallel_graceful_and_immediate") as r:
    for graceful in (True, False):
        master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 100, 0, 0))
        view = subprocess.Popen(
            [binary, "group", "new", "test", "--shell", "/bin/sh", "--no-persist"],
            env=dict(r.environment, TERM="xterm-256color"),
            stdin=slave,
            stdout=slave,
            stderr=slave,
            start_new_session=True,
        )
        r.processes.append(view)
        os.close(slave)
        try:
            registry = r.home_dir / "state/group/workspaces.json"

            def group():
                while select.select([master], [], [], 0)[0]:
                    try:
                        os.read(master, 65536)
                    except OSError:
                        break
                if not registry.exists():
                    return None
                return next(
                    iter(json.loads(registry.read_text())["namespaces"].values()), {}
                ).get("test")

            wait(lambda: group() and group()["members"], 10)
            time.sleep(0.2)
            os.write(master, b"\x1bud")
            wait(lambda: view.poll() is not None, 10)
            for i in range(3):
                name = "stubborn" + str(graceful) + str(i)
                r.start(name, grace="1s")
                r.cli("group", "add", "test", name)
            started = time.monotonic()
            r.measure(
                "group",
                "delete",
                "test",
                "--stop",
                *(["--graceful"] if graceful else []),
            )
            if graceful:
                assert time.monotonic() - started >= 0.9
        finally:
            os.close(master)

with scenario("daemon_stop_preserves_state_and_graceful_opt_in") as r:
    r.start()
    r.watch()
    r.measure("daemon", "stop")
    assert (r.home_dir / "state/undercurrent.db").exists()
    r.cli("daemon", "start")
    r.watch()
    started = time.monotonic()
    r.measure("daemon", "stop", "--graceful", "--drain-timeout", "1s")
    assert time.monotonic() - started >= 0.9
