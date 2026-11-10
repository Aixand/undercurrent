"""Observe an existing session without stopping it; see ../../docs/sdk/python/streams.md.

Run from the repository root after installing the package and starting the
selected daemon. Event checkpoints printed here are not durable application
checkpoints. A gap ends this example instead of claiming complete history.
"""

import argparse
import asyncio
from contextlib import aclosing
import json
import math
import sys

from undercurrent import Client


def positive_seconds(value):
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("timeout must be finite and positive")
    return value


def sequence(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError("sequence must be positive")
    return value


async def run(args):
    async with Client(home=args.home, bridge=args.bridge) as client:
        iterator = (
            client.watch(args.session_id)
            if args.mode == "watch"
            else client.events(args.session_id, after_sequence=args.after_sequence)
        )
        deadline = asyncio.timeout(args.timeout)
        try:
            async with deadline, aclosing(iterator) as stream:
                async for item in stream:
                    if args.mode == "watch":
                        current = item.session
                        print(
                            current.id, current.revision, current.lifecycle,
                            current.interaction, f"resynchronized={item.resynchronized}",
                            flush=True,
                        )
                    elif item["item"] == "gap":
                        raise RuntimeError(
                            f"Event history gap: {item['value']}; reconcile current state "
                            "before resuming an application workflow"
                        )
                    elif item["item"] == "session_event":
                        event = item["value"]
                        print(json.dumps(event, sort_keys=True), flush=True)
                        print(f"Observed event checkpoint: {event['sequence']}", file=sys.stderr)
                    else:
                        raise RuntimeError(f"Unexpected event envelope: {item['item']}")
        except TimeoutError:
            if not deadline.expired():
                raise  # A bridge/request timeout is a failure, not our observation deadline.
            print("Observation deadline reached; subscription closed.", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session_id", help="Stable ID, not a session name")
    parser.add_argument("--mode", choices=("watch", "events"), default="watch")
    parser.add_argument(
        "--after-sequence", type=sequence,
        help="Saved positive event checkpoint; omit to start live",
    )
    parser.add_argument("--timeout", type=positive_seconds, default=10.0)
    parser.add_argument("--home")
    parser.add_argument("--bridge")
    args = parser.parse_args()
    if args.mode != "events" and args.after_sequence is not None:
        parser.error("--after-sequence applies only to --mode events")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
