"""Coordinate independent jobs and a dependent report using the public SDK.

Run through render_demos.py, which owns the isolated home and failure cleanup.
"""
import asyncio
import json
from pathlib import Path
import sys
from undercurrent import Client
from demo_helpers import wait_for_line

async def require_success(client, session):
    ended = await client.wait(session, lifecycle={"exited", "failed", "stopped"}, timeout=15)
    if ended.lifecycle != "exited" or ended.raw["lifecycle"].get("code") != 0:
        raise RuntimeError(f"{session.name} did not exit successfully")
    print(f"  {session.name:<8} exited  code=0", flush=True)
    return ended.raw

async def main():
    async with Client() as client:
        print("PLAN: check + total -> report", flush=True)
        async with asyncio.TaskGroup() as tasks:
            starts = [tasks.create_task(client.start(
                [sys.executable, "-u", "job.py", name], name=name,
                detection=False, labels={"demo": "orchestration"},
            )) for name in ("check", "total")]
        sessions = [task.result() for task in starts]
        for session in sessions:
            await wait_for_line(client, session, f"READY: {session.name}")
        snapshots = [await client.inspect(session) for session in sessions]
        if any(snapshot.lifecycle != "running" for snapshot in snapshots):
            raise RuntimeError("Both jobs must be running before release")
        print("RUNNING: two independent jobs", flush=True)
        for snapshot in snapshots:
            print(f"  {snapshot.name:<8} {snapshot.lifecycle}", flush=True)
        Path("parallel-snapshot.json").write_text(json.dumps([s.raw for s in snapshots]))
        # Keep the live jobs visible long enough to read the recording.
        await asyncio.sleep(3)
        Path("begin-work").touch()
        print("WAIT: both jobs must exit successfully", flush=True)
        results = await asyncio.gather(*(require_success(client, s) for s in sessions))
        Path("prerequisite-exits.json").write_text(json.dumps(results))
        print("START: dependent report", flush=True)
        report = await client.start(
            [sys.executable, "-u", "job.py", "report"], name="report",
            detection=False, labels={"demo": "orchestration"},
        )
        await require_success(client, report)
        print("COMPLETE: report.txt is ready", flush=True)

asyncio.run(main())
