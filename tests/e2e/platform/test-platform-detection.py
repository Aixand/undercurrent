#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Shared Linux/macOS automatic detection using isolated shells and local app fixtures.

Build uc/uc-daemon/uc-session-host, then pass their directory (default target/debug).
No AI tool, account, network service, or real shell configuration is used.
"""
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
    if sys.platform not in ("linux", "darwin"):
        raise SystemExit("detection tests require Linux or macOS")
    shells = {"zsh": "/bin/zsh", "bash": "/bin/bash", "fish": os.environ.get("UC_TEST_FISH") or shutil.which("fish")}
    assert shells["fish"], "Fish is required; install it or set UC_TEST_FISH"
    project = Path(__file__).resolve().parents[3]
    binaries = Path(sys.argv[1] if len(sys.argv) > 1 else project / "target/debug").resolve()
    with tempfile.TemporaryDirectory(prefix="uc-detect-", dir=str(Path(tempfile.gettempdir()).resolve()) if sys.platform == "linux" else "/private/tmp") as tmp:
        root = Path(tmp)
        shell_home = root / "shell"
        shell_home.mkdir()
        # Debian/Ubuntu's global zshrc otherwise starts compinit, which can
        # prompt about the hosted runner's completion-directory permissions.
        (shell_home / ".zshenv").write_text("skip_global_compinit=1\n")
        # Fish 3 stores generated completions in data; Fish 4 uses cache.
        # Preparing both avoids an unrelated first-run manpage indexing helper
        # sharing the foreground group for tens of seconds on CI machines.
        for directory in (".local/share/fish/generated_completions", ".cache/fish/generated_completions"):
            (shell_home / directory).mkdir(parents=True)
        changed_directory = root / "changed-directory"
        changed_directory.mkdir()
        # CI exports hundreds of unrelated variables and shell customizations.
        # Keep the fixture bounded and independent of the runner's login setup.
        env = {"PATH": os.environ["PATH"], "TERM": "xterm-256color", "LANG": "C.UTF-8"}
        env.update(UNDERCURRENT_HOME=str(root / "product"), HOME=str(shell_home),
                   ZDOTDIR=str(shell_home), XDG_CONFIG_HOME=str(shell_home / ".config"),
                   PATH=str(binaries) + os.pathsep + env["PATH"])
        source = root / "fixture.c"
        source.write_text('#include <stdio.h>\n#include <unistd.h>\n'
                          'int main(void) { puts("DETECTION_FIXTURE_READY"); fflush(stdout); '
                          'sleep(120); return 0; }\n')
        image = root / "version-1.2.3"
        subprocess.run(["cc", str(source), "-o", str(image)], check=True, timeout=30)
        for name in ("codex", "claude"):
            (root / name).symlink_to(image)
        codex = shlex.quote(str(root / "codex"))
        claude = shlex.quote(str(root / "claude"))

        def uc(*args, check=True, structured=False):
            flags = ["--json"] if structured else []
            result = subprocess.run([str(binaries / "uc"), "--timeout", "10s", *flags, *args],
                                    env=env, cwd=root, capture_output=True, text=True, timeout=20)
            if check and result.returncode:
                raise AssertionError(f"uc {args}: {result.stdout}{result.stderr}")
            return json.loads(result.stdout) if structured else result

        def summary(name):
            return uc("inspect", name, structured=True)["summary"]

        def wait(name, label, predicate):
            deadline = time.monotonic() + 12
            while True:
                latest = summary(name)
                if predicate(latest):
                    return latest
                if time.monotonic() >= deadline:
                    output = uc("logs", name, check=False)
                    raise AssertionError(f"{name}: {label}: {json.dumps(latest)}\nTerminal: {output.stdout}{output.stderr}\nDetails: {uc('inspect', name, check=False).stdout}")
                time.sleep(0.1)

        def ready(name, shell):
            return wait(name, "ready shell", lambda s: s["profile"] == shell
                        and s["profile_context"]["effective_origin"] == "cooperative_shell"
                        and s["foreground"]["execution"]["state"] == "quiescent"
                        and s["foreground"]["interaction"]["state"] == "available")

        def app(name, profile="codex"):
            return wait(name, "foreground " + profile, lambda s: s["profile"] == profile
                        and s["profile_context"]["effective_origin"] == "foreground_process")

        def send(name, text):
            uc("send", name, text)

        def interrupt(name):
            uc("send", name, "--key", "ctrl-c")

        try:
            # These calls touch only the temporary HOME, not the user's startup files.
            for shell in shells:
                uc("init", shell, "--prompt", "off")
            uc("daemon", "start")
            for shell in shells:
                name = "auto-" + shell
                uc("start", name, "--interactive", "--shell", shells[shell],
                   "--cwd", str(root), "-e", "HOME=" + str(shell_home),
                   "-e", "ZDOTDIR=" + str(shell_home),
                   "-e", "XDG_DATA_HOME=" + str(shell_home / ".local/share"),
                   "-e", "XDG_CACHE_HOME=" + str(shell_home / ".cache"))
                ready(name, shell)
                send(name, "set -gx UC_PLATFORM_CAPTURE_TEST changed" if shell == "fish" else "export UC_PLATFORM_CAPTURE_TEST=changed")
                ready(name, shell)
                wait(name, "shell after export", lambda s: s["profile"] == shell)
                deadline = time.monotonic() + 12
                while "live environment: available" not in uc("inspect", name).stdout:
                    assert time.monotonic() < deadline, "shell environment was not captured"
                    time.sleep(0.1)
                send(name, "cd " + shlex.quote(str(changed_directory)))
                wait(name, "live working directory", lambda s:
                     s["profile_context"]["current_directory"] == str(changed_directory))
                ready(name, shell)
                send(name, codex)
                app(name)
                interrupt(name)
                ready(name, shell)
                send(name, codex + " &")
                ready(name, shell)
                send(name, "kill $last_pid" if shell == "fish" else "kill %1")
                ready(name, shell)
                send(name, codex)
                app(name)
                uc("send", name, "--key", "ctrl-z")
                ready(name, shell)
                send(name, "fg")
                app(name)
                interrupt(name)
                ready(name, shell)
                send(name, codex + " | " + claude)
                wait(name, "ambiguous pipeline", lambda s:
                     s["profile_context"]["unclassified_reason"] == "ambiguous")
                interrupt(name)
                ready(name, shell)
                if shell == "fish":
                    assert "could not read response to Primary Device Attribute" not in uc("logs", name).stdout, "Fish terminal query negotiation timed out"
                uc("stop", name)
                print(shell + ": prompt/app/prompt, background, suspend/resume, pipeline passed", flush=True)

            # An interactive shell without uc init can match applications, but
            # must never fabricate cooperative-shell readiness.
            plain = root / "plain"
            plain.mkdir()
            (plain / ".zshenv").write_text("skip_global_compinit=1\n")
            uc("start", "no-hooks", "--interactive", "--shell", "/bin/zsh", "--cwd", str(root),
               "-e", "ZDOTDIR=" + str(plain), "-e", "HOME=" + str(plain))
            send("no-hooks", codex)
            app("no-hooks")
            interrupt("no-hooks")
            plain_result = wait("no-hooks", "unhooked shell", lambda s: s["profile"] == "zsh")
            assert plain_result["profile_context"]["effective_origin"] != "cooperative_shell"
            uc("stop", "no-hooks")

            uc("start", "fixed", "--profile", "codex", "--", str(root / "codex"))
            fixed = wait("fixed", "fixed profile", lambda s: s["profile"] == "codex")
            assert fixed["profile_context"]["binding"] == "fixed"
            uc("stop", "fixed")
        finally:
            uc("daemon", "stop", check=False)
    print("Linux/macOS automatic detection tests passed")


if __name__ == "__main__":
    main()
