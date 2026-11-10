#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Real-daemon group view checks in an isolated product home, using only Python stdlib.

Build uc, uc-daemon, uc-session-host, then run this file. UC_TEST_BIN_DIR overrides
<repository>/target/debug. No installed daemon or user workspace is modified.
"""
import copy
import fcntl
import json
import os
from pathlib import Path
import pty
import select
import signal
import struct
import subprocess
import tempfile
import termios
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
BIN = Path(os.environ.get("UC_TEST_BIN_DIR", ROOT / "target/debug"))
UC = str(BIN / "uc")


class Terminal:
    def __init__(self, env, *args):
        self.master, slave = pty.openpty()
        self.original = termios.tcgetattr(slave)
        self.slave = os.dup(slave)
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 100, 0, 0))
        self.process = subprocess.Popen([UC, *args], env=env, stdin=slave,
                                        stdout=slave, stderr=slave, start_new_session=True)
        os.close(slave)
        self.output = b""

    def read(self, seconds=0.1):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            ready, _, _ = select.select([self.master], [], [], max(0, end - time.monotonic()))
            if ready:
                try:
                    chunk = os.read(self.master, 65536)
                except OSError:
                    break
                if not chunk:
                    break
                self.output = (self.output + chunk)[-2 * 1024 * 1024:]
        return self.output

    def expect(self, text, timeout=15):
        end = time.monotonic() + timeout
        while text not in self.output and time.monotonic() < end:
            self.read()
        assert text in self.output, (text, self.output[-3000:])

    def send(self, data):
        os.write(self.master, data)

    def shortcut(self, action):
        self.send(b"\x1bu" + action)
        self.read(0.15)

    def resize(self, rows, columns):
        fcntl.ioctl(self.master, termios.TIOCSWINSZ, struct.pack("HHHH", rows, columns, 0, 0))
        os.kill(self.process.pid, signal.SIGWINCH)
        self.read(0.15)

    def detach(self):
        self.shortcut(b"d")
        self.finish(0)

    def finish(self, expected):
        self.read(0.2)
        assert self.process.wait(timeout=15) == expected, self.output[-3000:]
        assert termios.tcgetattr(self.slave) == self.original, "terminal modes not restored"
        self.close()

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)
        for field in ("master", "slave"):
            fd = getattr(self, field)
            if fd >= 0:
                os.close(fd)
                setattr(self, field, -1)


def main():
    with tempfile.TemporaryDirectory(prefix="uc-group-test-") as home:
        env = {k: v for k, v in os.environ.items() if not k.startswith("UNDERCURRENT_")}
        env.update(UNDERCURRENT_HOME=home, TERM="xterm-256color", SHELL="/bin/sh")
        terminals = []
        Path(home, "config.toml").write_text("schema_version = \"0.1.0-beta\"\n[services]\nmax_query_page_size = 512\n")

        def run(*args, check=True):
            result = subprocess.run([UC, *args], env=env, text=True, capture_output=True, timeout=30)
            if check:
                assert result.returncode == 0, (args, result.stdout, result.stderr)
            return result.stdout if check else result

        def open_group(*args):
            t = Terminal(env, *args)
            terminals.append(t)
            return t

        def sessions():
            # CLI cursors are connection-scoped; keep this isolated test's bounded
            # catalog in one response instead of reusing a cursor across processes.
            page = json.loads(run("--json", "ls", "--page-size", "512"))
            assert page["next_cursor"] is None
            return page["items"]

        def wait_for(predicate, terminal=None, timeout=15):
            end = time.monotonic() + timeout
            while time.monotonic() < end:
                if predicate():
                    return
                if terminal:
                    terminal.read(0.05)
                else:
                    time.sleep(0.02)
            raise AssertionError(terminal.output[-3000:] if terminal else "condition timed out")

        registry = Path(home, "state", "group", "workspaces.json")

        def group(name):
            return next(iter(json.loads(registry.read_text())["namespaces"].values()))[name]

        def selected(name):
            return group(name)["selected"]

        try:
            run("daemon", "start")
            legacy = Path(home, "state", "tui")
            legacy.mkdir(mode=0o700)
            legacy_registry = legacy / "workspaces.json"
            legacy_registry.write_text("unsupported legacy registry\n")
            assert not registry.exists()
            assert json.loads(run("--json", "group", "ls")) == []
            assert registry.parent.is_dir() and not registry.exists()
            assert legacy_registry.read_text() == "unsupported legacy registry\n"
            before = sessions()
            created = json.loads(run("--json", "group", "create", "services", "--cwd", home))
            assert created == {"created": True, "name": "services"}, created
            saved = json.loads(run("--json", "group", "inspect", "services"))
            assert saved["name"] == "services" and saved["cwd"] == str(Path(home).resolve()), saved
            assert saved["members"] == [] and not saved["persistent"] and saved["pending_name"] is None, saved
            assert sessions() == before, "headless group creation must not start a shell"
            duplicate = run("group", "create", "services", check=False)
            assert duplicate.returncode != 0, duplicate
            assert json.loads(run("--json", "group", "inspect", "services")) == saved
            run("group", "delete", "services")
            assert sessions() == before, "headless group inspection and deletion must leave sessions alone"
            t = open_group("group", "new", "backend", "--shell", "/bin/sh")
            wait_for(lambda: len(sessions()) == 1, t)
            wait_for(lambda: selected("backend") is not None, t)
            first = selected("backend")
            t.shortcut(b"c")
            wait_for(lambda: len(sessions()) == 2 and selected("backend") != first, t)
            second = selected("backend")
            t.shortcut(b"p")
            wait_for(lambda: selected("backend") == first, t)
            t.shortcut(b"n")
            wait_for(lambda: selected("backend") == second, t)
            t.shortcut(b"?")
            t.expect(b"Undercurrent group shortcuts")
            t.send(b"\x1b")
            t.read(0.15)
            t.shortcut(b"s")
            t.expect(b"Group sessions")
            t.send(b"\r")
            wait_for(lambda: selected("backend") == first, t)
            t.send(b"printf 'UC_TEST_%s\\n' RENDERED\r")
            t.expect(b"UC_TEST_RENDERED")
            busy = open_group("group", "attach", "backend")
            busy.expect(b"group is busy")
            busy.finish(70)
            t.resize(12, 50)
            t.send(b"stty size\r")
            t.expect(b"11 50")
            t.resize(2, 8)
            t.expect(b"Resize terminal")
            t.resize(24, 100)
            t.detach()
            assert all(s["lifecycle"]["state"] == "running" for s in sessions())
            print("PASS: create, C/N/P/S/help, exclusive group lock, resize, detach", flush=True)

            # Retired storage is inert; current groups keep using state/group.
            assert len(json.loads(run("--json", "group", "ls"))) == 1
            assert registry.is_file()
            assert legacy_registry.read_text() == "unsupported legacy registry\n"
            t = open_group("group", "new", "other")
            wait_for(lambda: len(sessions()) == 3, t)
            wait_for(lambda: selected("other") is not None, t)
            other = selected("other")
            t.shortcut(b"n")
            assert selected("other") == other
            t.detach()
            t = open_group("group", "attach", "backend")
            t.expect(b"backend")
            assert selected("backend") == first
            run("start", "external", "--", "/bin/cat")
            run("group", "add", "backend", "external")
            t.shortcut(b"s")
            t.expect(b"external")
            assert run("group", "add", "other", "external", check=False).returncode != 0
            run("group", "remove", "backend", "external")
            t.send(b"\x1b")
            t.read(0.2)
            t.detach()
            assert next(s for s in sessions() if s["name"] == "external")["lifecycle"]["state"] == "running"
            print("PASS: group isolation, reopen, concurrent add/remove, one-group membership", flush=True)

            # Simulate accepted start + crash before membership commit using a unique receipt.
            token = str(uuid.uuid4())
            run("start", "recovered", "--label", f"uc.group.creation={token}", "--", "/bin/cat")
            data = json.loads(registry.read_text())
            saved = next(iter(data["namespaces"].values()))["backend"]
            saved["pending"] = {"token": token, "request": {
                "name": "recovered", "command": {"program": "/bin/cat", "arguments": []},
                "working_directory": home, "terminal": {"columns": 100, "rows": 23},
                "profile": None, "labels": {"uc.group.creation": token},
            }}
            registry.write_text(json.dumps(data))
            before = len(sessions())
            t = open_group("group", "attach", "backend")
            wait_for(lambda: group("backend")["pending"] is None, t)
            assert len(sessions()) == before
            assert any(m["name"] == "recovered" for m in group("backend")["members"])
            t.detach()
            t = open_group("group", "attach", "backend")
            t.expect(b"backend")
            t.process.kill()
            t.process.wait(timeout=5)
            t.close()
            t = open_group("group", "attach", "backend")
            t.expect(b"backend")
            t.detach()
            print("PASS: accepted creation recovery without duplication; crash releases group lock", flush=True)

            # Direct attachment still has its original, unframed behavior.
            t = open_group("attach", "backend-shell-1")
            t.read(0.3)
            t.detach()
            if Path("/usr/bin/vim").exists():
                run("start", "editor", "--", "/usr/bin/vim", "-Nu", "NONE", "-n", "-i", "NONE")
                run("group", "add", "other", "editor")
                t = open_group("group", "attach", "other")
                t.expect(b"other")
                t.shortcut(b"n")
                wait_for(lambda: selected("other") == next(s["id"] for s in sessions() if s["name"] == "editor"), t)
                t.send(b"iVIM_RENDERED\x1b")
                t.expect(b"VIM_RENDERED")
                t.send(b":q!\r")
                wait_for(lambda: next(s for s in sessions() if s["name"] == "editor")["lifecycle"]["state"] == "exited", t)
                assert t.process.poll() is None, "process exit closed workspace"
                t.detach()
                print("PASS: full-screen Vim editing and exited process retention", flush=True)

            # Removing every member leaves a durable empty group; the picker can reopen it.
            for member in copy.deepcopy(group("other")["members"]):
                run("group", "remove", "other", member["id"])
            t = open_group("group", "attach", "other")
            t.expect(b"No active session")
            t.shortcut(b"c")
            wait_for(lambda: len(group("other")["members"]) == 1, t)
            t.detach()
            t = open_group("group")
            t.expect(b"Undercurrent groups")
            t.send(b"\x1b")
            t.finish(0)
            # Optional scale qualification: selected output must not stall detach.
            scale = int(os.environ.get("UC_GROUP_TEST_SCALE", "0"))
            if scale:
                assert 4 <= scale <= 300
                run("start", "flood", "--", "/usr/bin/python3", "-u", "-c",
                    "import time\nwhile True:\n print('output-pressure ' * 200)\n time.sleep(.001)")
                run("group", "add", "backend", "flood")
                for index in range(scale - len(group("backend")["members"])):
                    name = f"idle-{index}"
                    run("start", name, "--", "/bin/sleep", "300")
                    run("group", "add", "backend", name)
                t = open_group("group", "attach", "backend")
                t.expect(b"backend")
                t.shortcut(b"n")
                t.expect(b"output-pressure")
                t.read(1)
                status = Path(f"/proc/{t.process.pid}/status").read_text()
                rss = next(line for line in status.splitlines() if line.startswith("VmRSS:"))
                started = time.monotonic()
                t.detach()
                latency = time.monotonic() - started
                assert latency < 3, latency
                print(f"PASS: {scale} grouped sessions under output pressure; detach {latency:.2f}s; {rss}", flush=True)
            t = open_group("group", "attach", "backend")
            t.expect(b"backend")
            run("daemon", "restart")
            t.read(0.5)
            assert t.process.poll() is None, "daemon restart closed the workspace"
            t.shortcut(b"?")
            t.expect(b"Undercurrent group shortcuts")
            t.send(b"\x1b")
            t.read(0.2)
            t.detach()
            assert len(json.loads(run("--json", "group", "ls"))) == 2
            t = open_group("group", "attach", "backend")
            t.expect(b"backend")
            t.detach()
            assert legacy_registry.read_text() == "unsupported legacy registry\n"
            print("PASS: empty groups, group picker, daemon restart retains grouping", flush=True)
        finally:
            for terminal in terminals:
                terminal.close()
            try:
                run("stop", "--all")
            finally:
                run("daemon", "stop")



if __name__ == "__main__":
    main()
