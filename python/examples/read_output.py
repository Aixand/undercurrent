"""Copy currently retained bytes to stdout; see ../../docs/sdk/python/streams.md.

Use a stable session ID and an already-running daemon. Redirect stdout to a
file; status and next offsets go to stderr. This is a bounded-page read loop,
not a live terminal subscription. It creates no session and stops no process.
"""

import argparse
import asyncio
import sys

from undercurrent import Client, OutputGap
from _output_interval import output_page_end


def offset_value(value):
    value = int(value)
    if value < 0:
        raise argparse.ArgumentTypeError("offset must be nonnegative")
    return value


async def run(args):
    offset = args.from_offset
    async with Client(home=args.home, bridge=args.bridge) as client:
        while True:
            try:
                page = await client.read_output(
                    args.session_id, from_offset=offset, max_bytes=32768
                )
            except OutputGap as gap:
                if not args.allow_gap:
                    raise
                if gap.available_from <= offset:
                    raise RuntimeError("Retention gap did not advance") from gap
                print(
                    f"INCOMPLETE CAPTURE: skipping [{offset}, {gap.available_from})",
                    file=sys.stderr, flush=True,
                )
                offset = gap.available_from
                continue
            end = output_page_end(page, offset)
            sys.stdout.buffer.write(page.data)
            sys.stdout.buffer.flush()
            offset = end
            print(f"Next byte offset: {offset}", file=sys.stderr, flush=True)
            if not page.has_more:
                return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session_id", help="Stable ID, not a session name")
    parser.add_argument("--from-offset", type=offset_value, default=0)
    parser.add_argument("--allow-gap", action="store_true", help="Explicitly accept missing bytes")
    parser.add_argument("--home")
    parser.add_argument("--bridge")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
