#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Qualify private pipes and a disposable daemon across Windows logon contexts.

Run from an elevated SSH session for the same user currently signed in to the
desktop. Requires a built pipe_peer_probe example and matching UC binaries.
Only test-owned tasks, processes, and homes are created or retired.
"""

from __future__ import annotations

import argparse
import base64
import ctypes
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time
import uuid


def context():
    from ctypes import wintypes as w

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel.GetCurrentProcess.restype = w.HANDLE
    kernel.CloseHandle.argtypes = [w.HANDLE]
    advapi.OpenProcessToken.argtypes = [w.HANDLE, w.DWORD, ctypes.POINTER(w.HANDLE)]
    advapi.GetTokenInformation.argtypes = [w.HANDLE, w.DWORD, ctypes.c_void_p, w.DWORD,
                                         ctypes.POINTER(w.DWORD)]
    advapi.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(w.LPWSTR)]
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    token = w.HANDLE()
    if not advapi.OpenProcessToken(kernel.GetCurrentProcess(), 8, ctypes.byref(token)):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        def query(kind):
            value, size = ctypes.create_string_buffer(4096), w.DWORD()
            if not advapi.GetTokenInformation(token, kind, value, len(value), ctypes.byref(size)):
                raise ctypes.WinError(ctypes.get_last_error())
            return value

        user, text = query(1), w.LPWSTR()
        if not advapi.ConvertSidToStringSidW(ctypes.c_void_p.from_buffer(user), ctypes.byref(text)):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            sid = text.value
        finally:
            kernel.LocalFree(text)
        session = w.DWORD()
        if not kernel.ProcessIdToSessionId(os.getpid(), ctypes.byref(session)):
            raise ctypes.WinError(ctypes.get_last_error())
        return {"pid": os.getpid(), "sid": sid, "session": session.value,
                "elevationType": w.DWORD.from_buffer(query(18)).value}
    finally:
        kernel.CloseHandle(token)


def powershell(code):
    encoded = base64.b64encode(code.encode("utf-16le")).decode("ascii")
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
        capture_output=True, timeout=25, creationflags=0x08000000,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.decode(errors="replace")[-2500:])


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def save(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def wait_file(path, seconds=60):
    deadline = time.monotonic() + seconds
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"No fixture result: {path}")
        time.sleep(0.1)


def command(args, env=None, stdin=None):
    result = subprocess.run(args, env=env, input=stdin, capture_output=True,
                            text=True, encoding="utf-8", timeout=35,
                            creationflags=0x08000000)
    if result.returncode:
        raise RuntimeError(f"{Path(args[0]).name}: {result.stderr[-2000:]}")
    return result.stdout


def bridge_request(binary, env, request):
    # EOF intentionally shuts down the bridge's SDK. Keep stdin open until the
    # request finishes, as the actual Python transport does.
    frames = queue.Queue()
    with subprocess.Popen([str(binary)], env=env, stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, encoding="utf-8", creationflags=0x08000000) as process:
        def read_frames():
            try:
                for line in process.stdout:
                    frames.put(json.loads(line))
            except Exception as error:
                frames.put({"type": "reader_error", "error": str(error)})
            finally:
                frames.put({"type": "eof"})

        reader = threading.Thread(target=read_frames, daemon=True)
        reader.start()
        try:
            ready = frames.get(timeout=30)
            assert ready["type"] == "ready", ready
            process.stdin.write(json.dumps(request) + "\n")
            process.stdin.flush()
            reply = frames.get(timeout=30)
            assert reply.get("id") == request["id"], reply
            return reply
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            reader.join(timeout=5)


def run_worker(job_path):
    job = json.loads(job_path.read_text(encoding="utf-8"))
    report = Path(job["report"])
    result = {"ok": False, "context": context()}
    try:
        if job["mode"] == "pipe":
            output = subprocess.run(
                [job["probe"], job["role"], job["endpoint"], str(report.with_suffix(".peer.json"))],
                capture_output=True, timeout=35, creationflags=0x08000000,
            )
            result.update(exit=output.returncode,
                          peer=json.loads(report.with_suffix(".peer.json").read_text()))
            result["ok"] = output.returncode == 0
        else:
            env = os.environ.copy()
            for key in ("UNDERCURRENT_CONFIG", "UNDERCURRENT_ENDPOINT", "XDG_CONFIG_HOME",
                        "XDG_STATE_HOME", "UC_SESSION"):
                env.pop(key, None)
            env.update(UNDERCURRENT_HOME=job["home"], HOME=job["user"], USERPROFILE=job["user"])
            Path(job["user"]).mkdir(exist_ok=True)

            def uc(*args):
                return json.loads(command([job["uc"], "--json", *args], env=env))

            if job["mode"] == "uc-owner":
                started = False
                try:
                    uc("daemon", "start")
                    started = True
                    uc("start", "peer-context-proof", "--profile", "server", "--",
                       sys.executable.replace("pythonw.exe", "python.exe"), "-c",
                       "import time; time.sleep(180)")
                    session = next(row for row in uc("ls")["items"]
                                   if row["name"] == "peer-context-proof")
                    save(Path(job["ready"]), {"id": session["id"], "context": result["context"]})
                    wait_file(Path(job["stop"]), 150)
                finally:
                    if started:
                        uc("daemon", "stop", "--graceful", "--drain-timeout", "5s")
            else:
                expected = json.loads(Path(job["ready"]).read_text())["id"]
                assert any(row["id"] == expected for row in uc("ls")["items"])
                # A real request through the matching bridge exercises the SDK
                # transport, independently of the CLI's local transport.
                request = {"id": "peer-context-list", "op": "request", "body": {
                    "operation": "list_sessions", "input": {"filter": {
                        "labels": {}, "ids": [], "name": None},
                        "projection": "with_labels", "page_size": 100, "cursor": None}}}
                reply = bridge_request(Path(job["uc"]).with_name("uc-python-bridge.exe"), env, request)
                assert reply["type"] == "result", reply
                assert any(row["id"] == expected for row in reply["result"]["result"]["items"])
                result.update(sessionId=expected, cli=True, bridge=True)
            result["ok"] = True
    except Exception as error:
        result["error"] = str(error)
    save(report, result)
    return 0 if result["ok"] else 1


def qualify(args):
    work = args.work.resolve()
    work.mkdir(parents=True, exist_ok=False)
    owner = context()
    assert owner["session"] == 0, "Run this qualification from SSH, not the desktop"
    assert owner["elevationType"] in (1, 2), "An elevated SSH token is required"
    script = Path(__file__).resolve()
    pythonw = Path(sys.executable).with_name("pythonw.exe")
    assert pythonw.is_file(), pythonw
    tasks = []
    report = {"passed": False, "coordinator": owner, "pipes": [], "daemonClients": []}

    def launch(label, job, privilege):
        job_path = work / (label + ".job.json")
        job["report"] = str(work / (label + ".json"))
        save(job_path, job)
        arguments = [str(pythonw), "-I", "-B", str(script), "--worker", str(job_path)]
        if privilege == "ssh":
            subprocess.Popen(arguments, creationflags=0x08000000)
        else:
            name = "Undercurrent peer qualification " + uuid.uuid4().hex
            tasks.append(name)
            action_args = subprocess.list2cmdline(arguments[1:])
            powershell(
                "$ErrorActionPreference='Stop';"
                f"$a=New-ScheduledTaskAction -Execute {quote(pythonw)} -Argument {quote(action_args)};"
                "$u=[Security.Principal.WindowsIdentity]::GetCurrent().Name;"
                "$p=New-ScheduledTaskPrincipal -UserId $u -LogonType Interactive "
                f"-RunLevel {'Limited' if privilege == 'regular' else 'Highest'};"
                "$s=New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 4);"
                f"Register-ScheduledTask -TaskName {quote(name)} -Action $a -Principal $p -Settings $s | Out-Null;"
                f"Start-ScheduledTask -TaskName {quote(name)}"
            )
        return Path(job["report"])

    def collect(path, privilege):
        wait_file(path)
        data = json.loads(path.read_text(encoding="utf-8"))
        token = data["context"]
        assert token["sid"] == owner["sid"], data
        expected_session = token["session"] == 0 if privilege == "ssh" else token["session"] != 0
        assert expected_session, data
        assert token["elevationType"] == {"regular": 3, "admin": 2}.get(
            privilege, owner["elevationType"]), data
        return data

    stop = work / "daemon.stop"
    daemon = None
    try:
        for label, server, client, anonymous in (
            ("regular-regular", "regular", "regular", False),
            ("regular-admin", "regular", "admin", False),
            ("admin-regular", "admin", "regular", False),
            ("regular-ssh", "regular", "ssh", False),
            ("regular-ssh-anonymous", "regular", "ssh", True),
        ):
            endpoint = work / (label + ".endpoint")
            common = {"mode": "pipe", "probe": str(args.probe.resolve()), "endpoint": str(endpoint)}
            s = launch(label + "-server", common | {"role": "server"}, server)
            wait_file(endpoint.with_suffix(".ready"))
            c = launch(label + "-client", common | {"role": "anonymous" if anonymous else "client"}, client)
            sr, cr = collect(s, server), collect(c, client)
            assert sr["ok"] == cr["ok"] == (not anonymous), (label, sr, cr)
            if anonymous:
                assert sr["peer"]["winerror"] == 1347, sr  # ERROR_CANT_OPEN_ANONYMOUS
            report["pipes"].append({"case": label, "passed": True, "server": sr, "client": cr})

        common = {"uc": str(args.uc.resolve()), "home": str(work / "daemon-home"),
                  "user": str(work / "fake-user"), "ready": str(work / "daemon.ready.json"),
                  "stop": str(stop)}
        daemon = launch("daemon-owner", common | {"mode": "uc-owner"}, "regular")
        wait_file(Path(common["ready"]))
        for privilege in ("regular", "admin", "ssh"):
            path = launch("daemon-client-" + privilege, common | {"mode": "uc-client"}, privilege)
            result = collect(path, privilege)
            assert result["ok"], result
            report["daemonClients"].append({"clientContext": privilege, **result})
        report["checksPassed"] = True
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        stop.touch()
        try:
            if daemon:
                result = collect(daemon, "regular")
                report["daemonCleanup"] = result
                assert result["ok"], result
        finally:
            errors = []
            for name in reversed(tasks):
                try:
                    powershell(f"Stop-ScheduledTask -TaskName {quote(name)} -ErrorAction SilentlyContinue;"
                               f"Unregister-ScheduledTask -TaskName {quote(name)} -Confirm:$false -ErrorAction Stop")
                except Exception as error:
                    errors.append(str(error))
            report["taskCleanupErrors"] = errors
            report["passed"] = (report.get("checksPassed", False)
                                and report.get("daemonCleanup", {}).get("ok", False)
                                and not errors)
            save(work / "report.json", report)
            if errors:
                raise RuntimeError("Could not retire all fixture tasks: " + str(errors))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path)
    parser.add_argument("--uc", type=Path)
    parser.add_argument("--work", type=Path, help="New directory for evidence and disposable state")
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if os.name != "nt":
        parser.error("native Windows is required")
    if args.worker:
        return run_worker(args.worker)
    if not all((args.probe, args.uc, args.work)):
        parser.error("--probe, --uc and --work are required")
    result = qualify(args)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
