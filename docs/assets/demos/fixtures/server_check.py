"""Read-only evidence for the local npm server's health and retained error logs."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import build_opener, ProxyHandler


def main(phase):
    if phase not in {"healthy", "request-error", "unhealthy", "recovered"} or not Path(".recording-owned").is_file():
        raise RuntimeError("Use the recording runner and a declared health phase")
    def uc(*args):
        return json.loads(subprocess.run(
            ["uc", *args, "--json"], check=True, capture_output=True, text=True, timeout=15,
        ).stdout)

    evidence = json.loads(Path("server-evidence.json").read_text())
    expected_health = "unhealthy" if phase == "unhealthy" else "healthy"
    deadline = time.monotonic() + 15
    # A matching cached observation could predate the request or recovery.
    # Wait for a new daemon probe; the checker itself never changes app state.
    while True:
        detail = uc("explain", "api")
        summary = detail["summary"]
        health = summary["health"]
        if (health["status"] == expected_health
                and (health["observed_at_millis"] or 0) >= evidence["lastEventAt"]):
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("No fresh daemon health observation after the sample event")
        time.sleep(0.2)
    if summary["lifecycle"]["state"] != "running":
        raise RuntimeError("Expected a running API with the requested health status")
    if evidence["port"] != int(os.environ["PORT"]):
        raise RuntimeError("The API did not bind the recording's port")
    opener = build_opener(ProxyHandler({}))
    try:
        response = opener.open(os.environ["APP"] + "/health", timeout=3)
    except HTTPError as error:
        response = error
    with response:
        status, body = response.status, json.loads(response.read(1024))
    if status != (503 if phase == "unhealthy" else 200) or body != {"status": expected_health}:
        raise RuntimeError("The actual HTTP health response does not match")
    output = uc("logs", "api", "--max-bytes", "4096")
    if output["result"] != "data" or output["from_offset"] != 0 or output["has_more"]:
        raise RuntimeError("The sample's retained log is not complete within 4096 bytes")
    text = bytes(output["bytes"]).decode()
    request_error = "[ERROR] GET /orders?demo_error=1 -> 500: simulated request failure"
    dependency_error = "[ERROR] GET /orders -> 503: inventory dependency unavailable"
    if ((request_error in text) != (phase != "healthy")
            or (dependency_error in text) != (phase in {"unhealthy", "recovered"})):
        raise RuntimeError("Retained error output does not match the phase")
    Path(f"server-{phase}.json").write_text(json.dumps({
        "detail": detail, "server": evidence, "http_status": status, "http_body": body,
        "logs": text,
    }, indent=2) + "\n")


if __name__ == "__main__":
    main(sys.argv[1])
