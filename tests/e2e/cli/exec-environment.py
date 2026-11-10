#!/usr/bin/env python3
"""Exercise live exports against isolated real CLI/daemon/host binaries."""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, default=Path("target/release"))
    parser.add_argument("--fish", default="fish", help="Optional Fish executable")
    options = parser.parse_args()
    uc = str((options.bin_dir / "uc").resolve())
    with tempfile.TemporaryDirectory(prefix="uc-env-", dir="/private/tmp" if sys.platform == "darwin" else None) as temporary:
        root = Path(temporary)
        product = root / "product"
        product.mkdir(mode=0o700)
        (product / "config.toml").write_text("schema_version = \"0.1.0-beta\"\n[local]\n")
        (product / "config.toml").chmod(0o600)
        runtime = root / "run"
        runtime.mkdir(mode=0o700)
        launch, live = root / "launch", root / "live"
        launch.mkdir()
        live.mkdir()
        tools = live / "bin"
        tools.mkdir()
        path_tool = tools / "uc-env-check-tool"
        path_tool.write_text("#!/bin/sh\nprintf path-ok\n")
        path_tool.chmod(0o700)
        private_marker = "uc-private-capture-sentinel-c410"
        environment = os.environ.copy()
        for key in list(environment):
            if key.startswith("UNDERCURRENT_") or key.startswith("UC_"):
                environment.pop(key)
        environment.update(UNDERCURRENT_HOME=str(product), XDG_RUNTIME_DIR=str(runtime), HOME=str(root), ZDOTDIR=str(root))

        def run(*args, check=True):
            result = subprocess.run(
                [uc, "--timeout", "10s", *args], env=environment,
                capture_output=True, text=True, timeout=20,
            )
            if check and result.returncode:
                # Do not print environment values or terminal output on failure.
                raise AssertionError(f"uc {args[0]} failed ({result.returncode})")
            return result

        def wait_for(predicate, label):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if predicate():
                    return
                time.sleep(0.1)
            raise AssertionError(f"timed out: {label}")

        def exports(name, *flags):
            code = (
                "import json,os; print(json.dumps({k:os.environ.get(k) for k in "
                "['UC_ENV_KEEP','UC_ENV_REMOVE','UC_ENV_EMPTY','UC_ENV_MULTI','PWD']}))"
            )
            return json.loads(run("exec", name, *flags, "--", "/usr/bin/python3", "-c", code).stdout)

        started = False
        daemon = None
        try:
            daemon = subprocess.Popen([str((options.bin_dir / "uc-daemon").resolve())],
                                      env=environment, stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL, start_new_session=True)
            started = True
            wait_for(lambda: run("daemon", "status", check=False).returncode == 0, "daemon startup")
            shells = [("bash", shutil.which("bash"), ["--norc", "-i"]),
                      ("zsh", shutil.which("zsh"), ["-f", "-i"]),
                      ("fish", shutil.which(options.fish), ["--no-config", "-i"])]
            tested = 0
            for shell, executable, args in shells:
                if not executable:
                    print(f"SKIP {shell}: executable unavailable", flush=True)
                    continue
                hook = root / f"{shell}.hook"
                hook.write_text(run("init", shell, "--print").stdout)
                setup = root / f"{shell}.setup"
                if shell == "fish":
                    setup.write_text("set -gx UC_ENV_KEEP live\nset -e UC_ENV_REMOVE\n"
                                     "set -gx UC_ENV_EMPTY ''\nset -gx UC_ENV_MULTI 'line one\nline two'\n"
                                     f"set -gx UC_ENV_PRIVATE {private_marker}\nset -gx PATH {shlex.quote(str(tools))} $PATH\n")
                else:
                    setup.write_text("export UC_ENV_KEEP=live\nunset UC_ENV_REMOVE\n"
                                     "export UC_ENV_EMPTY=''\nexport UC_ENV_MULTI='line one\nline two'\n"
                                     f"export UC_ENV_PRIVATE={private_marker}\nexport PATH={shlex.quote(str(tools))}:$PATH\n")
                identity = root / (shell + ".identity")
                shell_pid = "$fish_pid" if shell == "fish" else "$$"
                with setup.open("a") as setup_file:
                    setup_file.write(f"printf '%s\\n' \"$UC_SESSION\" {shell_pid} > {shlex.quote(str(identity))}\n")
                name = "env-" + shell
                run("start", name, "--cwd", str(launch), "--env", "UC_ENV_KEEP=launch",
                    "--env", "UC_ENV_REMOVE=launch", "--", executable, *args)
                missing = run("exec", name, "--dir-launch", "--", "/bin/true", check=False)
                assert missing.returncode and "--env-launch" in missing.stderr
                run("send", name, "source " + shlex.quote(str(hook)))
                wait_for(lambda: "live environment: available" in run("inspect", name).stdout,
                         shell + " initial capture")
                run("send", name, "source " + shlex.quote(str(setup)))
                wait_for(lambda: exports(name, "--dir-launch")["UC_ENV_KEEP"] == "live", shell + " export")
                # Knowing the owner token does not let an unrelated process publish
                # a snapshot for a root shell that is not its ancestor.
                session_id, root_pid = identity.read_text().splitlines()
                assert (product / "run" / "session-host.sock").is_socket()
                assert root_pid.isdigit()
                spoof = subprocess.run(
                    [str((options.bin_dir / "uc-session-host").resolve()), "--capture-shell-environment"],
                    env={"UC_SESSION": session_id, "UC_CAPTURE_SHELL_PID": root_pid,
                         "UC_SHELL_HOST": str(product / "run" / "session-host.sock"),
                         "UC_SHELL_ENV_BYTES": "65536", "UC_SHELL_ENV_ENTRIES": "4096",
                         "UC_ENV_KEEP": "spoofed"}, capture_output=True, timeout=5,
                )
                assert spoof.returncode != 0
                assert exports(name, "--dir-launch")["UC_ENV_KEEP"] == "live"
                # An incomplete/oversized capture invalidates the older snapshot.
                shrink = ("set -g __uc_saved_limit $UC_SHELL_ENV_BYTES; set -gx UC_SHELL_ENV_BYTES 1"
                          if shell == "fish" else "__uc_saved_limit=$UC_SHELL_ENV_BYTES; export UC_SHELL_ENV_BYTES=1")
                restore = ("set -gx UC_SHELL_ENV_BYTES $__uc_saved_limit"
                           if shell == "fish" else "export UC_SHELL_ENV_BYTES=$__uc_saved_limit")
                run("send", name, shrink)
                wait_for(lambda: "live environment: unavailable" in run("inspect", name).stdout,
                         shell + " oversized snapshot invalidation")
                missing = run("exec", name, "--dir-launch", "--", "/bin/true", check=False)
                assert missing.returncode and "--env-launch" in missing.stderr
                run("send", name, restore)
                wait_for(lambda: "live environment: available" in run("inspect", name).stdout,
                         shell + " capture recovery")
                # Nested shells must not replace the managed root's snapshot.
                nested_ready, nested_done = root / (shell + ".nested-ready"), root / (shell + ".nested-done")
                nested = (
                    'export UC_ENV_KEEP=nested; '
                    'UC_CAPTURE_SHELL_PID=$$ "$UC_SHELL_HELPER" --capture-shell-environment; '
                    f'touch {shlex.quote(str(nested_ready))}; sleep 1; touch {shlex.quote(str(nested_done))}'
                )
                run("send", name, "/bin/bash -c " + shlex.quote(nested))
                wait_for(nested_ready.exists, shell + " nested capture attempt")
                assert exports(name, "--dir-launch")["UC_ENV_KEEP"] == "live"
                wait_for(nested_done.exists, shell + " nested completion")
                run("send", name, "cd " + shlex.quote(str(live)))
                run("send", name, "sleep 120")
                wait_for(lambda: f"working directory: {live}\n" in run("inspect", name).stdout,
                         shell + " live directory")
                actual = exports(name)
                assert actual == {"UC_ENV_KEEP": "live", "UC_ENV_REMOVE": None,
                                  "UC_ENV_EMPTY": "", "UC_ENV_MULTI": "line one\nline two", "PWD": str(live)}
                assert exports(name, "--dir-launch")["PWD"] == str(launch)
                assert run("exec", name, "--", "uc-env-check-tool").stdout == "path-ok"
                assert private_marker not in run("logs", name).stdout
                assert private_marker not in run("--json", "inspect", name).stdout
                # Hold an execution open while checking its durable journal.
                child = subprocess.Popen([uc, "--timeout", "10s", "exec", name, "--", "/bin/sleep", "1"],
                                         env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    journal = product / "state" / "session-host" / "session-host.execution-jobs"
                    wait_for(lambda: journal.exists() and any(journal.iterdir()), shell + " execution journal")
                    for record in journal.iterdir():
                        if record.is_file():
                            assert private_marker.encode() not in record.read_bytes()
                    assert child.wait(timeout=15) == 0
                finally:
                    if child.poll() is None:
                        child.kill()
                        child.wait(timeout=5)
                    child.communicate()
                original = exports(name, "--env-launch")
                assert original["UC_ENV_KEEP"] == "launch" and original["UC_ENV_REMOVE"] == "launch"
                assert original["PWD"] == str(live)
                assert exports(name, "--dir-launch", "--env-launch")["PWD"] == str(launch)
                assert run("exec", name, "--", "/bin/sh", "-c", "exit 7", check=False).returncode == 7
                if shell == "bash":
                    # Kill only the daemon subprocess owned by this test. The host
                    # and busy shell must survive, retaining the private snapshot.
                    daemon.kill()
                    daemon.wait(timeout=10)
                    run("daemon", "start")
                    wait_for(lambda: f"working directory: {live}\n" in run("inspect", name).stdout,
                             "directory recovery after daemon crash")
                    assert exports(name)["UC_ENV_KEEP"] == "live"
                    print("PASS daemon reconnection: live snapshot survives", flush=True)
                # A fresh process incarnation must not inherit the old snapshot.
                run("restart", name)
                missing = run("exec", name, "--dir-launch", "--", "/bin/true", check=False)
                assert missing.returncode and "--env-launch" in missing.stderr
                run("stop", name, "--force")
                run("clean", name, "--yes")
                tested += 1
                print(f"PASS {shell}: exports, unset, empty/multiline, four mode combinations, PATH, nested isolation, private-value protection, busy session, exit status, restart", flush=True)
            assert tested, "no supported shells available"
        finally:
            if started:
                run("daemon", "stop", check=False)
            if daemon is not None and daemon.poll() is None:
                daemon.terminate()
                try:
                    daemon.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    daemon.kill()
                    daemon.wait(timeout=5)


if __name__ == "__main__":
    main()
