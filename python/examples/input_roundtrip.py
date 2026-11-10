"""Run against an isolated, already-started daemon; see ../../docs/sdk/python/quickstart.md.

Creates one native terminal session, verifies an application acknowledgment,
and stops its known process if still live. Does not remove its durable record
or stop the daemon. No failed mutation is retried automatically.
"""

import argparse
import asyncio
import sys
import uuid

from undercurrent import Client
from _output_interval import output_page_end

TERMINAL = {"stopped", "exited", "failed"}
CHILD = (
    "import sys; "
    "value = sys.stdin.readline().rstrip('\\r\\n'); "
    "print('UC_ACK:' + value, flush=True)"
)


async def expect_ack(client, session_id, marker):
    """Check bytes across page boundaries without keeping the whole transcript."""
    offset = 0
    suffix = b""
    async with asyncio.timeout(10):
        while True:
            page = await client.read_output(
                session_id, from_offset=offset, max_bytes=32768
            )
            end = output_page_end(page, offset)
            combined = suffix + page.data
            if marker in combined:
                return
            suffix = combined[-(len(marker) - 1):]
            offset = end
            if not page.has_more:
                await asyncio.sleep(0.05)


async def run(args):
    name = "python-example-" + uuid.uuid4().hex
    nonce = uuid.uuid4().hex
    delivery_id = uuid.uuid4().hex
    print(f"Runtime name for start reconciliation: {name}", flush=True)
    async with Client(home=args.home, bridge=args.bridge) as client:
        print(f"Runtime home: {client.home}", flush=True)
        session = None
        try:
            session = await client.start(
                [sys.executable, "-u", "-c", CHILD],
                name=name,
                detection=False,
                labels={"python-example": name},
            )
            print(f"Session ID: {session.id}", flush=True)
            await client.wait(session, lifecycle="running", timeout=10)
            print(f"Delivery ID for reconciliation: {delivery_id}", flush=True)
            receipt = await client.send_input(
                session, nonce, paste=False, delivery_id=delivery_id
            )
            print(f"Terminal delivery receipt: {receipt}", flush=True)
            # Only the fixture adds UC_ACK:. PTY echo of the nonce is insufficient.
            await expect_ack(client, session.id, ("UC_ACK:" + nonce).encode())
            ended = await client.wait(session, lifecycle=TERMINAL, timeout=10)
            if ended.lifecycle != "exited" or ended.raw["lifecycle"].get("code") != 0:
                raise RuntimeError(f"Unexpected process result: {ended.raw['lifecycle']}")
        finally:
            if session is not None:
                try:
                    current = await client.inspect(session.id)
                    if current.lifecycle not in TERMINAL:
                        stopped = await client.stop(session.id)
                        if stopped.lifecycle not in TERMINAL:
                            raise RuntimeError("Stop did not return a terminal snapshot")
                except Exception:
                    print(
                        f"Cleanup unconfirmed: inspect session {session.id} in "
                        f"{client.home} with a fresh client. Do not delete that home yet.",
                        file=sys.stderr,
                    )
                    raise
    print("Verified application acknowledgment and zero exit status.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", help="Isolated runtime home; defaults to UNDERCURRENT_HOME")
    parser.add_argument("--bridge", help="Matching uc-python-bridge executable")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
