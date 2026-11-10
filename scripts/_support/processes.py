# SPDX-License-Identifier: Apache-2.0
"""Bounded subprocess execution and Linux identities for isolated test runtimes."""

from contextlib import contextmanager
import os
from pathlib import Path
import signal
import subprocess
import time


def exit_code(code):
    return code if code >= 0 else 128 - code


@contextmanager
def handle_signals():
    def interrupted(number, _frame):
        raise SystemExit(128 + number)

    signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGHUP"):
        signals.append(signal.SIGHUP)
    previous = {number: signal.signal(number, interrupted) for number in signals}
    try:
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def terminate_group(process, grace=2):
    # The leader may have exited while descendants still hold its output pipes.
    if os.name == "posix":
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            process.poll()
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                break
            time.sleep(.02)
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    elif process.poll() is None:
        # Native daemon/session-host ownership is handled by each test's cleanup.
        process.kill()
    process.wait(timeout=grace + 1)


def run(command, *, timeout=20, env=None, cwd=None, input=None, check=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    command = [str(arg) for arg in command]
    process = subprocess.Popen(
        command, cwd=cwd, env=env, stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
        stdout=stdout, stderr=stderr, start_new_session=os.name == "posix",
    )
    try:
        output, errors = process.communicate(input=input, timeout=timeout)
    except BaseException:
        terminate_group(process)
        # Escaped daemons must not turn a timeout into an unbounded pipe read.
        for name in ("stdin", "stdout", "stderr"):
            pipe = getattr(process, name)
            reader = getattr(process, "_" + name + "_thread", None)
            if pipe is not None and (reader is None or not reader.is_alive()):
                pipe.close()
        raise
    result = subprocess.CompletedProcess(command, process.returncode, output, errors)
    if check and result.returncode:
        raise subprocess.CalledProcessError(result.returncode, command, output, errors)
    return result


def start_ticks(pid):
    try:
        return int(Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()[19])
    except (FileNotFoundError, ProcessLookupError):
        return None


def identity_alive(identity):
    return start_ticks(identity[0]) == identity[1]


def assert_gone(identity, label):
    if identity_alive(identity):
        raise AssertionError(f"{label} process incarnation remains after shutdown (pid {identity[0]})")


def identities(executable=None, *, home=None, socket=None, marker=None):
    """Select only stable process identities belonging to this test's runtime."""
    expected = Path(executable).resolve() if executable else None
    inodes = set()
    if socket is not None:
        for line in Path('/proc/net/unix').read_text().splitlines()[1:]:
            fields = line.split()
            if len(fields) >= 8 and fields[7] == str(socket):
                inodes.add(f"socket:[{fields[6]}]")
        if not inodes:
            raise AssertionError(f"private socket missing from kernel table: {socket}")
    result = []
    deadline = time.monotonic() + 5
    for path in Path('/proc').iterdir():
        if not path.name.isdecimal():
            continue
        if time.monotonic() > deadline:
            raise TimeoutError("timed out locating owned runtime processes")
        try:
            pid = int(path.name)
            before = start_ticks(pid)
            if before is None or (expected and (path / 'exe').resolve(strict=True) != expected):
                continue
            if home is not None or marker is not None:
                environment = (path / 'environ').read_bytes().split(b'\0')
                if home is not None and f"UNDERCURRENT_HOME={home}".encode() not in environment:
                    continue
                if marker is not None and marker.encode() not in environment:
                    continue
            if socket is not None:
                targets = set()
                for fd in (path / 'fd').iterdir():
                    try:
                        targets.add(os.readlink(fd))
                    except FileNotFoundError:
                        pass
                if not targets & inodes:
                    continue
            if start_ticks(pid) == before:
                result.append((pid, before))
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return result


def single_identity(executable, **options):
    found = identities(executable, **options)
    if len(found) != 1:
        raise AssertionError(f"expected one {Path(executable).name} process, found {len(found)}")
    return found[0]


def terminate_identity(identity, grace=3):
    pid, _ = identity
    descriptor = None
    try:
        if hasattr(os, "pidfd_open"):
            descriptor = os.pidfd_open(pid)
        if not identity_alive(identity):
            return
        def send(number):
            if descriptor is not None:
                signal.pidfd_send_signal(descriptor, number)
            elif identity_alive(identity):
                os.kill(pid, number)
        send(signal.SIGTERM)
        deadline = time.monotonic() + grace
        while identity_alive(identity) and time.monotonic() < deadline:
            time.sleep(.05)
        send(signal.SIGKILL)
    except ProcessLookupError:
        pass
    finally:
        if descriptor is not None:
            os.close(descriptor)


def isolated_environment(home):
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("UNDERCURRENT_", "UC_"))}
    environment["UNDERCURRENT_HOME"] = str(home)
    return environment
