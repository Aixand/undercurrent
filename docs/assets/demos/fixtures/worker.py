"""Example application for the recordings; all progress is produced by this process."""
import argparse
import json
import os
from pathlib import Path
import selectors
import signal
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument("--interactive", action="store_true")
args = parser.parse_args()
alive = True

def stop(*_):
    global alive
    alive = False

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGHUP, stop)
state = {"pid": os.getpid(), "ticks": 0, "messages": []}

def record():
    temporary = Path("worker-evidence.tmp")
    temporary.write_text(json.dumps(state))
    temporary.replace("worker-evidence.json")

print("UNDERCURRENT DEMO WORKER", flush=True)
print("READY: waiting for input" if args.interactive else "Progress continues while you are detached.", flush=True)
record()
started = time.monotonic()
if args.interactive:
    reader = selectors.DefaultSelector()
    reader.register(sys.stdin, selectors.EVENT_READ)
    while alive and time.monotonic() - started < 120:
        if not reader.select(timeout=0.1):
            continue
        line = sys.stdin.readline()
        if not line:
            break
        value = line.rstrip("\r\n")
        state["messages"].append(value)
        record()
        print(f"APP_ACK: {value}", flush=True)
    reader.close()
else:
    next_tick = started
    while alive and time.monotonic() - started < 120:
        if time.monotonic() >= next_tick:
            state["ticks"] += 1
            record()
            print(f"  tick {state['ticks']:02d} | working", flush=True)
            next_tick += 1
        time.sleep(0.05)
print("Worker stopped.", flush=True)
