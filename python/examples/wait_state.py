"""Wait for observed state on an existing session; see ../../docs/sdk/python/state.md.

Read-only: closes its wait subscription and client, leaving the process running.
Repeated --condition entries select axes; comma-separated states use OR within
one axis. Successful observation is not application task completion.
"""

import argparse
import asyncio
import math

from undercurrent import Client


def duration(value):
    value = float(value)
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError("duration must be finite and nonnegative")
    return value


def revision(value):
    value = int(value)
    if value < 0:
        raise argparse.ArgumentTypeError("revision must be nonnegative")
    return value


async def run(args, conditions):
    async with Client(home=args.home, bridge=args.bridge) as client:
        current = await client.wait_for_stability(
            args.session_id,
            stable_for=args.stable_for,
            timeout=args.timeout,
            after_revision=args.after_revision,
            **conditions,
        )
        print(
            f"Observed {current.id} at revision {current.revision}: "
            f"lifecycle={current.lifecycle}, execution={current.execution}, "
            f"interaction={current.interaction}, constraint={current.constraint}, "
            f"health={current.health}"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session_id", help="Stable ID, not a session name")
    parser.add_argument("--condition", action="append", required=True, metavar="AXIS=STATE[,STATE]")
    parser.add_argument("--stable-for", type=duration, default=2.0)
    parser.add_argument("--timeout", type=duration, default=30.0)
    parser.add_argument("--after-revision", type=revision)
    parser.add_argument("--home")
    parser.add_argument("--bridge")
    args = parser.parse_args()
    if args.timeout == 0:
        parser.error("--timeout must be positive")
    conditions = {}
    for condition in args.condition:
        axis, separator, values = condition.partition("=")
        if not separator or not values or axis in conditions:
            parser.error("use one --condition AXIS=STATE[,STATE] per axis")
        # Prevent collisions with helper keyword arguments before entering a client.
        if axis not in {"lifecycle", "execution", "interaction", "constraint", "health"}:
            parser.error(f"unknown condition axis: {axis}")
        conditions[axis] = values.split(",")
    asyncio.run(run(args, conditions))


if __name__ == "__main__":
    main()
