#!/usr/bin/env python3
"""Read bounded retained output from an existing session using Python SDK 0.1.

Requires Python 3.11+, the installed undercurrent package, its matching bridge,
and an already running daemon selected by UNDERCURRENT_HOME. Writes raw bytes
to stdout; errors and the final exclusive byte offset go to stderr. This example
does not start or stop a managed process. See ../docs/internals/output-and-streams.md.
"""

import argparse
import asyncio
import sys

import undercurrent
from undercurrent.errors import OutputGap, UndercurrentError


def nonnegative(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return parsed


async def read_retained(session_id: str, offset: int) -> int:
    async with undercurrent.Client() as runtime:
        for _ in range(1000):
            page = await runtime.read_output(session_id, from_offset=offset, max_bytes=4096)
            if page.from_offset != offset:
                raise RuntimeError("output page did not start at the requested offset")
            sys.stdout.buffer.write(page.data)
            # SDK 0.1 next_offset is the producer head, not this page's end.
            offset = page.from_offset + len(page.data)
            if not page.has_more:
                sys.stdout.buffer.flush()
                print(f"Read through exclusive byte offset {offset}", file=sys.stderr)
                return 0
            if not page.data:
                raise RuntimeError("output page made no progress")
        print(f"Page budget reached; partial output ends at offset {offset}", file=sys.stderr)
        return 3


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session_id", help="existing stable session ID")
    parser.add_argument("--from-offset", type=nonnegative, default=0)
    args = parser.parse_args()
    try:
        return asyncio.run(read_retained(args.session_id, args.from_offset))
    except OutputGap as error:
        print(
            f"Retention gap: output is partial. Earliest available offset is "
            f"{error.available_from}. Review before restarting there.",
            file=sys.stderr,
        )
        return 2
    except (UndercurrentError, TimeoutError, RuntimeError) as error:
        print(f"Output read failed; any emitted bytes are partial: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
