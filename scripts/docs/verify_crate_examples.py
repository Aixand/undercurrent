#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Execute the offline Rust walkthroughs and preserve bounded evidence."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE_LIMIT = 64 * 1024
RETIRED_CONFIG_ENV = (
    "UNDERCURRENT_CONFIG",
    "UNDERCURRENT_DATABASE_PATH",
    "UNDERCURRENT_INSTANCE_LOCK_PATH",
    "UNDERCURRENT_LOCAL_ENDPOINT",
)
CONFIG_PACKAGE = "undercurrent-config"


CASES = {
    "foundation_values": (
        "aixand-undercurrent-foundation",
        (
            "canonical: {\"a\":1,\"b\":2}",
            "digest: "
            "sha256:43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777",
            "bounded string: ready",
            "opaque URI: trust://passport/subjects/sub_01",
            "privacy: safe value accepted; sensitive value rejected",
        ),
    ),
    "model_walkthrough": (
        "undercurrent-domain",
        (
            "session=example-session lifecycle=running revision=4",
            "attempt_reconciliation_required=false",
            "output_verified=true truncated=true",
        ),
    ),
    "inspect_payload": (
        "aixand-undercurrent-protocol",
        (
            "stop_session: contract valid; JSON round trip matched",
            "retry metadata: same digest; changed revision: different digest",
            "terminal: 3 bytes at offsets 41..44; digest verified",
        ),
    ),
    "inspect_cancellation": (
        "aixand-undercurrent-protocol",
        (
            "cancellation: valid round trip; command and product digests differ",
            "changed reason: decoded shape, rejected semantic digest",
            "status: exact lookup constructed; no command was sent",
        ),
    ),
    "inspect_frames": (
        "aixand-undercurrent-wire",
        (
            "negotiated protocol 0.1.0-beta; maximum frame 4096 bytes",
            "decoded fragmented request and following ping",
            "binary payload preserved; credit includes the full frame",
        ),
    ),
    "framing_roundtrip": (
        "aixand-undercurrent-transport-local",
        (
            "verified one complete UCMP Ping and clean EOF",
        ),
    ),
    "bounded_delivery": (
        "aixand-undercurrent-multiplex",
        (
            "second frame waits for consumed credit",
            "consuming sequence 1 returns credit",
            "resume after consumed sequence 1 delivers sequence 2",
            "all connection and journal entries released",
        ),
    ),
    "retained_result": (
        "aixand-undercurrent-transport-core",
        (
            "reserved -> pending -> completed -> replayed -> expired",
        ),
    ),
    "output_gap": (
        "undercurrent-application",
        (
            "gap: requested=0, available=4, end=12",
            "data: 4..8 = efgh",
            "data: 8..12 = ijkl",
        ),
    ),
    "repository_roundtrip": (
        "undercurrent-store-sqlite",
        (
            "reopened: name=renamed, sessions=1, pending_events=2",
            "verified backup: release=0.1.0-beta, identities=1",
            "restored: sessions=1, pending_events=2, next_sequence=3",
            "temporary repository removed",
        ),
    ),
}


def capture(stream):
    """Return a bounded prefix and indicate whether further bytes were written."""
    stream.seek(0)
    data = stream.read(EVIDENCE_LIMIT + 1)
    return data[:EVIDENCE_LIMIT].decode("utf-8", errors="replace"), len(data) > EVIDENCE_LIMIT


class Verifier:
    def __init__(self, target_dir, evidence_dir, timeout):
        channel = tomllib.loads((ROOT / "rust-toolchain.toml").read_text(encoding="utf-8"))
        self.cargo = [
            "cargo", f"+{channel['toolchain']['channel']}", "run", "--locked", "--offline",
            "--target-dir", str(target_dir),
        ]
        self.evidence_dir = evidence_dir
        self.timeout = timeout
        self.env = os.environ.copy()
        for name in RETIRED_CONFIG_ENV:
            self.env.pop(name, None)

    def run(self, name, package, *, label=None, args=(), markers=(), check=None):
        command = [*self.cargo, "-p", package, "--example", name]
        if args:
            command.extend(("--", *args))

        problems = []
        status = None
        with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
            try:
                result = subprocess.run(
                    command, cwd=ROOT, env=self.env, stdin=subprocess.DEVNULL,
                    stdout=stdout, stderr=stderr, timeout=self.timeout, check=False,
                )
                status = result.returncode
                if status:
                    problems.append(f"Cargo exited with status {status}")
            except subprocess.TimeoutExpired:
                problems.append(f"Cargo exceeded the {self.timeout}s timeout")
            except OSError as error:
                problems.append(str(error))
            output_text, output_truncated = capture(stdout)
            errors_text, errors_truncated = capture(stderr)

        if output_truncated:
            problems.append("stdout exceeded the evidence limit")
        if not problems:
            lines = set(output_text.splitlines())
            missing = [marker for marker in markers if marker not in lines]
            problems.extend(f"missing stdout line {marker!r}" for marker in missing)
            if check is not None:
                problems.extend(check(output_text))

        evidence = self.evidence_dir / f"{label or name}.json"
        evidence.write_text(
            json.dumps(
                {
                    "command": command,
                    "exit_status": status,
                    "stdout": output_text,
                    "stdout_truncated": output_truncated,
                    "stderr": errors_text,
                    "stderr_truncated": errors_truncated,
                    "expected_stdout_lines": markers,
                    "problems": problems,
                },
                indent=2,
            ) + "\n",
            encoding="utf-8",
        )
        print(f"{'FAIL' if problems else 'PASS'} {evidence}")
        return not problems

    def config(self):
        with tempfile.TemporaryDirectory(prefix="uc-inspect-config-") as directory:
            home = Path(directory).resolve()
            fixture = home / "config.yaml"
            if not self.run(
                "inspect_config", CONFIG_PACKAGE, label="inspect_config_sample",
                args=("--sample",), markers=("schema_version: 0.1.0-beta",),
                check=lambda output: write_fixture(fixture, output),
            ):
                return False
            original = fixture.read_bytes()
            return self.run(
                "inspect_config", CONFIG_PACKAGE, args=(str(home),),
                check=lambda output: check_config(home, original, output),
            )


def write_fixture(fixture, output):
    fixture.write_text(output, encoding="utf-8")
    return []


def check_config(home, fixture_bytes, output):
    expected = {
        ("scope",): "on_disk_effective_configuration",
        ("cli_connection_profile",): "compiled_standard",
        ("prefix_source",): "main_config_or_default",
        ("settings", "cli", "command_timeout_millis"): 45000,
        ("settings", "cli", "list_page_size"): 17,
        ("settings", "shortcuts", "prefix"): "ctrl-a",
        ("settings", "terminal", "scrollback", "memory_lines"): 0,
        ("settings", "terminal", "replay_bytes"): 1048576,
    }
    problems = []
    try:
        report = json.loads(output)
    except ValueError as error:
        return [f"invalid JSON report: {error}"]
    for fields, expected_value in expected.items():
        actual = report
        for field in fields:
            actual = actual.get(field) if isinstance(actual, dict) else None
        if type(actual) is not type(expected_value) or actual != expected_value:
            problems.append(f"unexpected {'.'.join(fields)}: {actual!r}")
    encoded_home = json.dumps(str(home), ensure_ascii=False)[1:-1]
    normalized_report = json.dumps(report, ensure_ascii=False)
    if encoded_home in normalized_report:
        problems.append("report exposed the fixture home")
    if {path.name for path in home.iterdir()} != {"config.yaml"}:
        problems.append("inspection changed the fixture home entries")
    elif (home / "config.yaml").read_bytes() != fixture_bytes:
        problems.append("inspection changed the fixture contents")
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", type=Path, required=True, help="Cargo build directory")
    parser.add_argument("--evidence-dir", type=Path, required=True, help="new directory for JSON")
    parser.add_argument("--timeout", type=int, default=600, help="seconds per Cargo invocation")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    evidence_dir = args.evidence_dir.resolve()
    try:
        evidence_dir.mkdir(parents=True)
    except OSError as error:
        parser.error(str(error))

    verifier = Verifier(args.target_dir.resolve(), evidence_dir, args.timeout)
    results = [
        verifier.run(name, package, markers=markers)
        for name, (package, markers) in CASES.items()
    ]
    results.append(verifier.config())
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
