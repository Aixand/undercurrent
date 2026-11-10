"""Small CSV jobs with a bounded start barrier for the orchestration recording."""
import csv
from decimal import Decimal
import json
from pathlib import Path
import sys
import time

mode = sys.argv[1]
if mode not in {"check", "total", "report"}:
    raise SystemExit("Unknown job")
started = time.monotonic_ns()
if mode != "report":
    Path(f"{mode}-ready.json").write_text(json.dumps({"started_ns": started}))
    print(f"READY: {mode}", flush=True)
    deadline = time.monotonic() + 60
    # The coordinator opens this barrier after observing both live sessions.
    while not Path("begin-work").exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("Coordinator did not release the jobs")
        time.sleep(0.05)
    with Path("orders.csv").open(newline="") as source:
        rows = list(csv.DictReader(source))
    if mode == "check":
        if not rows or not all(row["item"] and int(row["quantity"]) > 0
                               and Decimal(row["unit_price"]) > 0 for row in rows):
            raise ValueError("Invalid order data")
        result = {"rows": len(rows), "valid": True}
    else:
        total = sum(int(row["quantity"]) * Decimal(row["unit_price"]) for row in rows)
        result = {"rows": len(rows), "total": str(total)}
    print(f"DONE: {mode}", flush=True)
else:
    checks = json.loads(Path("check-result.json").read_text())
    totals = json.loads(Path("total-result.json").read_text())
    if checks["valid"] is not True or checks["rows"] != totals["rows"]:
        raise ValueError("Job results disagree")
    result = {"rows": checks["rows"], "total": totals["total"]}
    report = f"Validated orders: {result['rows']}\nOrder total: {result['total']}\n"
    Path("report.txt").write_text(report)
    print(report, end="", flush=True)
result.update(started_ns=started, finished_ns=time.monotonic_ns())
Path(f"{mode}-result.json").write_text(json.dumps(result, indent=2) + "\n")
