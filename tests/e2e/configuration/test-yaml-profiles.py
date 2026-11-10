#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise live YAML profiles using colocated uc, uc-daemon and uc-session-host.

Run after cargo build -p undercurrent-daemon --bins -p undercurrent-cli --bin uc:
    python3 tests/e2e/configuration/test-yaml-profiles.py target/debug
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

PROFILE = """schema_version: 0.1.0-beta
name: yaml-test
archetype: unclassified
input_delivery: {type: immediate}
foreground_rules:
  - claim: {type: interaction, state: available, modality: text, semantic: yaml.initial}
    patterns: ['YAML_READY']
    confidence_basis_points: 10000
"""
PROGRAM = """import json,os,sys,time,tty
tty.setraw(0)
print('YAML_READY', flush=True)
with open(sys.argv[1], 'w', buffering=1) as log:
    while True:
        data = os.read(0, 1)
        if not data: break
        log.write(json.dumps([data.hex(), time.monotonic()]) + '\\n')
        if data in (b'\\r', b'\\n'): print('YAML_READY', flush=True)
"""


def run(binaries):
    shipped_profiles = Path(__file__).resolve().parents[3] / "crates/undercurrent-daemon/profiles"
    expected_profiles = {path.name for path in shipped_profiles.glob("*.yaml")}
    assert expected_profiles, "shipped profile definitions are missing"
    work = Path(tempfile.mkdtemp(prefix="uc-yaml-e2e-"))
    env = dict(os.environ, UNDERCURRENT_HOME=str(work))
    # Avoid inheriting a caller's explicit configuration into an isolated test.
    for name in ("UNDERCURRENT_CONFIG", "UNDERCURRENT_DATABASE_PATH", "UNDERCURRENT_INSTANCE_LOCK_PATH", "UNDERCURRENT_LOCAL_ENDPOINT"):
        env.pop(name, None)
    log = (work / "stderr.log").open("w")
    daemon = subprocess.Popen([str(binaries / "uc-daemon")], env=env, stdout=log, stderr=log)
    passed = False

    def uc(*args):
        result = subprocess.run([str(binaries / "uc"), "--json", *args], env=env,
                                capture_output=True, text=True, timeout=15)
        if result.returncode:
            raise RuntimeError(result.stderr + result.stdout)
        return json.loads(result.stdout)

    def wait(check, timeout=10):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            if daemon.poll() is not None:
                raise AssertionError((work / "stderr.log").read_text())
            try:
                last = check()
                if last:
                    return last
            except (RuntimeError, FileNotFoundError):
                pass
            time.sleep(0.1)
        raise AssertionError(f"condition did not settle: {last}; evidence: {work}")

    def semantic():
        return uc("inspect", "yaml-test")["summary"]["foreground"]["interaction"]["semantic"]

    def delay():
        return uc("profile", "inspect", "yaml-test")["input_delivery"].get("delay_millis", 0)

    def reject_logged():
        return any("profile reload rejected" in path.read_text()
                   for path in (work / "logs").glob("*.jsonl"))

    try:
        wait(lambda: uc("profile", "inspect", "codex"))
        assert {path.name for path in (work / "profiles").glob("*.yaml")} == expected_profiles
        # A Python launcher and a simulated Codex child share the foreground group.
        # The current folder-access dialog is near the top of a tall terminal.
        fixture = Path(__file__).resolve().parents[3] / "crates/undercurrent-daemon/src/profile_builtins/fixtures/codex-folder-access.txt"
        child = ["/bin/bash", "-c", 'exec -a codex /bin/bash -c \'cat "$1"; read -r answer\' codex "$1"',
                 "uc-codex-fixture", str(fixture)]
        wrapper = work / "wrapper.py"
        wrapper.write_text(f"import subprocess\nsubprocess.run({child!r}, check=True)\n")
        uc("start", "wrapper-test", "--columns", "236", "--rows", "98", "--env", f"PYTHONHOME={sys.base_prefix}",
           "--", "/bin/bash", "-c", '"$1" "$2"; result=$?; exit "$result"',
           "uc-wrapper", sys.executable, str(wrapper))

        def wrapper_summary():
            return uc("explain", "wrapper-test")["summary"]

        wait(lambda: wrapper_summary()["profile"] == "codex")
        assert wrapper_summary()["profile_context"]["binding"] == "foreground_adaptive"
        wait(lambda: wrapper_summary()["foreground"]["interaction"]["semantic"] == "codex.workspace_trust")
        interaction = wrapper_summary()["foreground"]["interaction"]
        assert interaction["state"] == "requested" and interaction["modality"] == "options", interaction
        evidence = wrapper_summary()["profile_context"]["matching_evidence"]
        assert {item["profile"] for item in evidence} == {"bash", "python", "codex"}, evidence
        assert all(item["process_incarnations"] for item in evidence), evidence
        assert next(item for item in evidence if item["profile"] == "python")["suppressed"]
        assert not next(item for item in evidence if item["profile"] == "codex")["suppressed"]
        explanation = subprocess.run([str(binaries / "uc"), "explain", "wrapper-test"], env=env,
                                     capture_output=True, text=True, check=True, timeout=15).stdout
        assert "python [fallback suppressed by application match]" in explanation, explanation
        assert "codex [application candidate]" in explanation, explanation
        wrapper_identity = uc("inspect", "wrapper-test", "--process")["process"]["instance_id"]
        python_profile = work / "profiles/python.yaml"
        python_original = python_profile.read_text()
        python_profile.write_text(python_original.replace("matching_fallback: true", "matching_fallback: false"))
        wait(lambda: wrapper_summary()["profile_context"]["unclassified_reason"] == "ambiguous")
        ambiguous_evidence = wrapper_summary()["profile_context"]["matching_evidence"]
        assert all(item["suppressed"] == item["matching_fallback"] for item in ambiguous_evidence), ambiguous_evidence
        # Explicit selection stays fixed even while automatic selection is ambiguous.
        uc("start", "pinned-test", "--profile", "codex", "--env", f"PYTHONHOME={sys.base_prefix}",
           "--", "/bin/bash", "-c", '"$1" "$2"; result=$?; exit "$result"',
           "uc-wrapper", sys.executable, str(wrapper))
        pinned = uc("explain", "pinned-test")["summary"]
        assert pinned["profile"] == "codex" and pinned["profile_context"]["binding"] == "fixed", pinned
        python_profile.write_text(python_original)
        wait(lambda: wrapper_summary()["profile"] == "codex")
        assert uc("inspect", "wrapper-test", "--process")["process"]["instance_id"] == wrapper_identity
        uc("stop", "wrapper-test")
        uc("stop", "pinned-test")
        path = work / "profiles/yaml-test.yaml"
        path.write_text(PROFILE)
        wait(lambda: uc("profile", "inspect", "yaml-test"))
        program = work / "terminal.py"
        program.write_text(PROGRAM)
        received = work / "received.jsonl"
        created = uc("start", "yaml-test", "--profile", "yaml-test", "--profile-mode", "fixed", "--env", f"PYTHONHOME={sys.base_prefix}",
                     "--", sys.executable, str(program), str(received))
        process = uc("inspect", "yaml-test", "--process")["process"]
        if process is None:
            raise AssertionError(f"session exited: {created}; output: {uc('logs', 'yaml-test')}")
        identity = process["instance_id"]
        wait(lambda: semantic() == "yaml.initial")
        # No new output is produced: the reload must refresh a quiet live terminal.
        changed = PROFILE.replace("yaml.initial", "yaml.updated").replace(
            "{type: immediate}", "{type: paced_trailing_submit, delay_millis: 600}")
        temporary = path.with_suffix(".tmp")
        temporary.write_text(changed)
        temporary.replace(path)
        wait(lambda: delay() == 600 and semantic() == "yaml.updated")
        assert uc("inspect", "yaml-test", "--process")["process"]["instance_id"] == identity
        uc("send", "yaml-test", "abc")
        wait(lambda: len(received.read_text().splitlines()) >= 4)
        events = [json.loads(line) for line in received.read_text().splitlines()]
        assert [entry[0] for entry in events[:3]] == ["61", "62", "63"], events
        assert events[3][0] in ("0d", "0a"), events
        assert events[3][1] - events[2][1] >= 0.5, events
        wait(lambda: semantic() == "yaml.updated")
        path.write_text(changed.replace("schema_version: 0.1.0-beta", "schema_version: 999"))
        wait(reject_logged)
        assert delay() == 600 and semantic() == "yaml.updated"
        # Correcting the file recovers without restarting the daemon.
        path.write_text(changed.replace("600", "300"))
        wait(lambda: delay() == 300)
        path.unlink()
        time.sleep(3)
        assert delay() == 300, "referenced custom profile must not disappear"
        uc("stop", "yaml-test")
        wait(lambda: all(row["name"] != "yaml-test" for row in uc("profile", "list")["items"]))
        assert uc("inspect", "yaml-test")["summary"]["id"] == created["session"]["id"]
        try:
            uc("restart", "yaml-test")
        except RuntimeError as error:
            assert "profile" in str(error) and "unavailable" in str(error), error
        else:
            raise AssertionError("restart must reject a deleted profile before process IO")
        assert uc("inspect", "yaml-test", "--process")["process"] is None
        uc("daemon", "stop", "--graceful", "--drain-timeout", "1s")
        assert daemon.wait(timeout=10) == 0
        # A malformed override prevents startup, remains available for repair,
        # and does not prevent a corrected subsequent startup and clean shutdown.
        shipped = work / "profiles/codex.yaml"
        original = shipped.read_text()
        shipped.write_text(original.replace("schema_version: 0.1.0-beta", "schema_version: 999"))
        daemon = subprocess.Popen([str(binaries / "uc-daemon")], env=env, stdout=log, stderr=log)
        assert daemon.wait(timeout=15) != 0
        assert "schema_version: 999" in shipped.read_text()
        assert any("codex.yaml" in p.read_text() and "YAML profile startup rejected" in p.read_text()
                   for p in (work / "logs").glob("*.jsonl"))
        shipped.write_text(original.replace("OpenAI Codex CLI", "Preserved customization"))
        daemon = subprocess.Popen([str(binaries / "uc-daemon")], env=env, stdout=log, stderr=log)
        wait(lambda: uc("profile", "inspect", "codex")["summary"]["description"] == "Preserved customization")
        uc("daemon", "stop", "--graceful", "--drain-timeout", "1s")
        assert daemon.wait(timeout=10) == 0
        passed = True
        print("PASS: default foreground wrapper detection, Codex folder-access interaction, ambiguity and pinned profiles, seeding, quiet-session reload, live input pacing, invalid-edit recovery, protected deletion, startup rejection/recovery, preserved customization, shutdown")
    finally:
        if daemon.poll() is None:
            for session in ("yaml-test", "wrapper-test", "pinned-test"):
                try:
                    uc("stop", session)
                except Exception:
                    pass
            try:
                uc("daemon", "stop", "--graceful", "--drain-timeout", "1s")
                daemon.wait(timeout=10)
            except Exception:
                daemon.terminate()
                daemon.wait(timeout=10)
        log.close()
        if passed:
            shutil.rmtree(work)
        else:
            print(f"Retained test evidence: {work}", file=sys.stderr)


if __name__ == "__main__":
    directory = Path(sys.argv[1] if len(sys.argv) > 1 else "target/debug").resolve()
    for name in ("uc", "uc-daemon", "uc-session-host"):
        if not (directory / name).is_file():
            raise SystemExit(f"Missing {directory / name}; build the binaries first")
    run(directory)
