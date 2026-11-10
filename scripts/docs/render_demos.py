#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Record and verify the documentation demos in a fresh, private runtime home."""
import argparse
import csv
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
ASSETS = ROOT / "docs/assets/demos"
TERMINAL = {"stopped", "exited", "failed"}
DEMO_SESSIONS = {
    "overview": ("demo",),
    "python-control": ("python-demo",),
    "interaction": ("review",),
    "orchestration": ("check", "total", "report"),
    "server-health": ("api", "commands"),
}
OFFLINE_DEMOS = tuple(DEMO_SESSIONS)


def checked(command, *, env, cwd, timeout=30):
    result = subprocess.run(command, env=env, cwd=cwd, capture_output=True,
                            text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{command[0]} exited {result.returncode}: {result.stderr.strip()}")
    return result.stdout


def verify_listener_closed(port):
    # Stopping a wrapper can end the child without invoking its signal handler.
    # Require observable service shutdown, rather than a graceful-callback flag.
    # Windows can take about two seconds to report a refused loopback connection.
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=5):
            pass
    except ConnectionRefusedError:
        return
    raise RuntimeError("The sample server is still listening after stop")


def verify_workflow(name, workspace, report):
    read = lambda filename: json.loads((workspace / filename).read_text())
    if name in {"overview", "python-control"}:
        report["worker"] = read("worker-evidence.json")
    if name == "overview":
        before, after = (read(f"checkpoint-{phase}.json") for phase in ("detached", "continued"))
        if before["pid"] != after["pid"] or after["ticks"] <= before["ticks"]:
            raise RuntimeError("Worker identity or progress did not survive detach")
        if before["inspection"]["summary"]["id"] != after["inspection"]["summary"]["id"]:
            raise RuntimeError("The recording switched to a different session")
        if any(s["inspection"]["summary"]["lifecycle"]["state"] != "running" for s in (before, after)):
            raise RuntimeError("The worker was not running at both checkpoints")
        report["detach_verification"] = {"same_pid": True, "same_session": True,
                                         "before_ticks": before["ticks"], "after_ticks": after["ticks"]}
    elif name == "python-control":
        if report["worker"]["messages"] != ["hello from Python"]:
            raise RuntimeError("The worker did not receive exactly the expected Python input")
    elif name == "interaction":
        checkpoint = read("checkpoint-requested.json")
        summary = checkpoint["inspection"]["summary"]
        interaction = summary["foreground"]["interaction"]
        if (summary["id"] != report["sessions"]["review"]["id"]
                or summary["lifecycle"]["state"] != "running"
                or interaction["state"] != "requested"
                or interaction["semantic"] != "demo.ask"
                or checkpoint["import_exists"]):
            raise RuntimeError("The importer was not waiting for confirmation before writing")
        with (workspace / "orders.csv").open(newline="") as source:
            expected = list(csv.DictReader(source))
        if read("imported.json") != expected:
            raise RuntimeError("Confirmation did not produce the expected import")
        report["interaction_verification"] = {"state": interaction, "imported_rows": len(expected),
                                               "no_import_before_confirmation": True}
    elif name == "server-health":
        phases = ("healthy", "request-error", "unhealthy", "recovered")
        checkpoints = [read(f"server-{phase}.json") for phase in phases]
        identity = report["sessions"]["api"]["id"]
        pid = checkpoints[0]["server"]["pid"]
        request_error = "[ERROR] GET /orders?demo_error=1 -> 500: simulated request failure"
        dependency_error = "[ERROR] GET /orders -> 503: inventory dependency unavailable"
        for index, (phase, checkpoint) in enumerate(zip(phases, checkpoints)):
            healthy = phase != "unhealthy"
            health = "healthy" if healthy else "unhealthy"
            summary = checkpoint["detail"]["summary"]
            results = checkpoint["detail"]["health_results"]
            server = checkpoint["server"]
            if (summary["id"] != identity or summary["lifecycle"]["state"] != "running"
                    or summary["health"]["status"] != health or summary["health"]["configured_checks"] != 1
                    or (summary["health"]["observed_at_millis"] or 0) < server["lastEventAt"]
                    or checkpoint["http_status"] != (200 if healthy else 503)
                    or checkpoint["http_body"] != {"status": health}
                    or len(results) != 1 or results[0]["rule_index"] != 0
                    or results[0]["safe_detail"] != ("http_ok" if healthy else "http_status_not_ok")
                    or results[0]["passed"] != healthy):
                raise RuntimeError(f"The server's {phase} checkpoint did not match a fresh HTTP check")
            if (server["pid"] != pid or server["dependencyAvailable"] != healthy
                    or server["isolatedErrors"] != (0 if index == 0 else 1)
                    or server["dependencyErrors"] != (1 if index >= 2 else 0)
                    or server["successfulOrders"] != (1, 2, 2, 3)[index]
                    or server["stopped"]):
                raise RuntimeError(f"The same server did not serve the expected requests in {phase}")
            if ((request_error in checkpoint["logs"]) != (index >= 1)
                    or (dependency_error in checkpoint["logs"]) != (index >= 2)):
                raise RuntimeError(f"Retained error history does not match {phase}")
        stopped = read("server-evidence.json")
        if (stopped["pid"] != pid or not stopped["dependencyAvailable"]
                or stopped["healthyProbes"] < 3 or stopped["unhealthyProbes"] < 1):
            raise RuntimeError("The server did not observe failure and recovery")
        verify_listener_closed(stopped["port"])
        report["server_verification"] = {"same_process": True, "http_statuses": [200, 200, 503, 200],
            "http_rule_verified": True, "healthy_after_request_error": True,
            "unhealthy_while_running": True, "recovered_with_error_history": True,
            "successful_requests_before_after_error_and_recovery": True,
            "listener_closed_after_stop": True}
    else:
        parallel, prerequisites = read("parallel-snapshot.json"), read("prerequisite-exits.json")
        expected_ids = {report["sessions"][job]["id"] for job in ("check", "total")}
        if (len(parallel) != 2 or {s["id"] for s in parallel} != expected_ids
                or any(s["lifecycle"]["state"] != "running" for s in parallel)):
            raise RuntimeError("The independent jobs did not both run")
        if (len(prerequisites) != 2 or {s["id"] for s in prerequisites} != expected_ids
                or any(s["lifecycle"] != {"state": "exited", "code": 0} for s in prerequisites)):
            raise RuntimeError("Both prerequisites must succeed before the report")
        checks, totals, result = (read(f"{job}-result.json") for job in ("check", "total", "report"))
        if (max(checks["started_ns"], totals["started_ns"]) >= min(checks["finished_ns"], totals["finished_ns"])
                or result["started_ns"] < max(checks["finished_ns"], totals["finished_ns"])):
            raise RuntimeError("Parallel or dependent job ordering was not preserved")
        with (workspace / "orders.csv").open(newline="") as source:
            rows = list(csv.DictReader(source))
        total = sum(int(row["quantity"]) * Decimal(row["unit_price"]) for row in rows)
        expected = f"Validated orders: {len(rows)}\nOrder total: {total}\n"
        if checks["valid"] is not True or (workspace / "report.txt").read_text() != expected:
            raise RuntimeError("The report does not agree with the input data")
        report["orchestration_verification"] = {"parallel_sessions": 2, "prerequisites_succeeded": True,
                                                 "dependent_started_after_both": True, "report": expected}


def record(name, args):
    artifacts = ROOT / "target/vhs-demos" / f"{name}-{time.time_ns()}"
    artifacts.mkdir(parents=True)
    # Short, owner-only paths satisfy local IPC's ownership and socket-path bounds.
    stage = Path(tempfile.mkdtemp(prefix="uc-vhs-", dir="/tmp")).resolve()
    home, workspace, render = (stage / part for part in ("home", "workspace", "render"))
    for directory in (home, workspace, render):
        directory.mkdir(mode=0o700)
    for source in (ASSETS / "fixtures").iterdir():
        if not source.is_file():
            continue
        shutil.copy2(source, workspace / source.name)
    if name == "server-health":
        shutil.copytree(ASSETS / "fixtures/server", workspace, dirs_exist_ok=True)
    if name == "interaction":
        (home / "profiles").mkdir(mode=0o700)
        shutil.copy2(ASSETS / "fixtures/ask.yaml", home / "profiles/ask.yaml")
    (workspace / ".recording-owned").touch()
    bin_dir = args.bin_dir.resolve()
    env = os.environ | {
        "UNDERCURRENT_HOME": str(home),
        "UNDERCURRENT_PYTHON_BRIDGE": str(bin_dir / "uc-python-bridge"),
        "UC_DEMO_WORKSPACE": str(workspace),
        "PYTHONPATH": str(ROOT / "python/src"),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
        "PATH": str(bin_dir) + os.pathsep + os.environ.get("PATH", ""),
        "VHS_PUBLISH": "false",
        "TERM": "xterm-256color",
    }
    env.pop("BASH_ENV", None)
    env.pop("ENV", None)
    if name == "server-health":
        # A bind collision fails startup; the checker verifies our server before fault injection.
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        env.update({"PORT": str(port), "APP": f"http://127.0.0.1:{port}",
                    "npm_config_update_notifier": "false", "npm_config_offline": "true",
                    "npm_config_cache": str(stage / "npm-cache"),
                    "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"})
        env.pop("NODE_OPTIONS", None)
    if args.browser:
        tools = stage / "tools"
        tools.mkdir()
        (tools / "chromium").symlink_to(args.browser.resolve())
        env["PATH"] = str(tools) + os.pathsep + env["PATH"]
    cli = str(bin_dir / "uc")
    uc = lambda *values: checked([cli, *values], env=env, cwd=workspace)
    report = {"demo": name, "artifacts": str(artifacts), "temporary_home": str(home),
              "vhs": checked(["vhs", "--version"], env=env, cwd=ROOT).strip(),
              "source_revision": checked(["git", "rev-parse", "HEAD"], env=env, cwd=ROOT).strip(),
              "source_hashes": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                for p in ASSETS.rglob("*") if p.suffix in {".py", ".tape", ".csv", ".yaml", ".mjs", ".json"}}}
    if name == "server-health":
        report["node_version"] = checked(["node", "--version"], env=env, cwd=ROOT).strip()
        report["npm_version"] = checked(["npm", "--version"], env=env, cwd=ROOT).strip()
    process, error, cleanup_error = None, None, None
    daemon_started = False
    try:
        (artifacts / "start.log").write_text(uc("daemon", "start"))
        daemon_started = True
        if name == "server-health":
            # Begin already attached to a managed shell; both panes belong to uc.
            (artifacts / "commands-start.log").write_text(uc(
                "start", "commands", "--env", "PS1=uc $ ", "--env", "HISTFILE=/dev/null",
                "--", "bash", "--noprofile", "--norc", "-i"))
        tape = (ASSETS / f"{name}.tape").read_text()
        tape = tape.replace(f"Output docs/assets/demos/{name}.gif", f'Output "{render / (name + ".gif")}"')
        tape = tape.replace(f"Screenshot docs/assets/demos/{name}-poster.png",
                            f'Screenshot "{render / (name + "-poster.png")}"')
        selected = artifacts / f"{name}.tape"
        selected.write_text(tape)
        print(f"Recording {name}; log: {artifacts / 'vhs.log'}", flush=True)
        with (artifacts / "vhs.log").open("w") as log:
            process = subprocess.Popen(["vhs", str(selected)], cwd=ROOT, env=env,
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=300 if name == "server-health" else 180)
            if code:
                raise RuntimeError(f"VHS exited {code}; see {artifacts / 'vhs.log'}")
        if not (workspace / "recording-complete").exists():
            raise RuntimeError("The tape did not reach its final checkpoint")
        report["sessions"] = {}
        for session_name in DEMO_SESSIONS[name]:
            summary = json.loads(uc("inspect", session_name, "--json"))["summary"]
            if summary["lifecycle"]["state"] not in {"stopped", "exited"}:
                raise RuntimeError(f"The recorded session did not stop: {summary['lifecycle']}")
            if (summary["lifecycle"]["state"] == "exited"
                    and summary["lifecycle"].get("code") != 0):
                raise RuntimeError(f"The demo process {session_name} did not exit successfully")
            report["sessions"][session_name] = summary
        verify_workflow(name, workspace, report)
        probe = checked(["ffprobe", "-v", "error", "-show_entries", "stream=width,height,duration,nb_frames",
                         "-of", "json", str(render / f"{name}.gif")], env=env, cwd=ROOT)
        report["video"] = json.loads(probe)
        stream = report["video"]["streams"][0]
        if int(stream["nb_frames"]) < 2 or float(stream["duration"]) < 10:
            raise RuntimeError("The generated recording is empty or incomplete")
        poster = render / f"{name}-poster.png"
        if not poster.is_file() or poster.read_bytes()[:8] != b"\x89PNG\r\n\x1a\n":
            raise RuntimeError("VHS did not finish capturing the static poster")
        report["assets"] = {path.name: {"bytes": path.stat().st_size,
                                      "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                            for path in (render / f"{name}.gif", poster)}
        report["verified"] = True
    except BaseException as failure:
        error = failure
        report["error"] = str(failure)
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        try:
            # The home was created for this one recording; no shared daemon is touched.
            if daemon_started:
                listing = json.loads(uc("ls", "--json"))
                for item in listing["items"]:
                    if item["lifecycle"]["state"] not in TERMINAL:
                        uc("stop", item["name"])
        except Exception as failure:
            cleanup_error = failure
            report["session_cleanup_error"] = str(failure)
        try:
            (artifacts / "stop.log").write_text(uc("daemon", "stop"))
            if cleanup_error is None:
                report["cleanup"] = "daemon ownership released"
        except Exception as failure:
            cleanup_error = failure
            report["cleanup_error"] = str(failure)
        for evidence in [*workspace.glob("*.json"), *workspace.glob("*.txt")]:
            shutil.copy2(evidence, artifacts / evidence.name)
        if error is None and cleanup_error is None:
            try:
                for suffix in (".gif", "-poster.png"):
                    destination = args.output_dir / f"{name}{suffix}"
                    shutil.copy2(render / destination.name, destination)
                shutil.rmtree(stage)
                report["temporary_state_removed"] = True
            except Exception as failure:
                error = failure
                report["error"] = str(failure)
        if error is not None or cleanup_error is not None:
            report["retained_stage"] = str(stage)
        (artifacts / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    if error or cleanup_error:
        raise RuntimeError(f"Recording failed; inspect {artifacts / 'report.json'}; retained {stage}") from (error or cleanup_error)
    print(f"Verified {name}: {stream['duration']}s; saved to {args.output_dir}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("demo", choices=[*DEMO_SESSIONS, "all"], nargs="?", default="all")
    parser.add_argument("--bin-dir", type=Path, default=ROOT / "target/debug")
    parser.add_argument("--output-dir", type=Path, default=ASSETS)
    parser.add_argument("--browser", type=Path, help="Chromium executable if it is not on PATH")
    args = parser.parse_args()
    if sys.platform not in {"linux", "darwin"}:
        parser.error("This recording runner requires Linux or macOS")
    for tool in ("vhs", "ttyd", "ffmpeg", "ffprobe", "bash", "python3", "git"):
        if shutil.which(tool) is None:
            parser.error(f"Missing recording dependency: {tool}")
    if args.demo in {"server-health", "all"}:
        for tool in ("node", "npm", "curl"):
            if shutil.which(tool) is None:
                parser.error(f"Missing server recording dependency: {tool}")
    for binary in ("uc", "uc-daemon", "uc-session-host", "uc-python-bridge"):
        if not os.access(args.bin_dir / binary, os.X_OK):
            parser.error(f"Build the matching native executables first: {args.bin_dir / binary}")
    if args.browser and not os.access(args.browser, os.X_OK):
        parser.error("--browser must name an executable Chromium browser")
    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for name in (OFFLINE_DEMOS if args.demo == "all" else [args.demo]):
        record(name, args)


if __name__ == "__main__":
    main()
