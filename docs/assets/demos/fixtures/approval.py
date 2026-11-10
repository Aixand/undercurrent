"""Sample importer: confirmation writes only inside the recording workspace."""
import csv
import json
from pathlib import Path
import selectors
import sys

with Path("orders.csv").open(newline="") as source:
    rows = list(csv.DictReader(source))
print("SAMPLE IMPORTER", flush=True)
print("Preview: orders.csv -> imported.json", flush=True)
print(f"APPROVAL REQUIRED: import {len(rows)} rows [yes/no]", flush=True)
with selectors.DefaultSelector() as reader:
    reader.register(sys.stdin, selectors.EVENT_READ)
    if not reader.select(timeout=90):
        raise TimeoutError("No confirmation received")
    answer = sys.stdin.readline().strip()
if answer != "yes":
    raise SystemExit("Import cancelled")
# Clear the old prompt so it no longer constitutes visible request evidence.
print("\x1b[2J\x1b[H", end="", flush=True)
Path("imported.json").write_text(json.dumps(rows, indent=2) + "\n")
print(f"APP_ACK: imported {len(rows)} rows", flush=True)
