"""Inspect an existing daemon without creating or stopping managed sessions.

Run from the repository root with PYTHONPATH=python/src; see ../docs/development/contributing.md.
"""

import argparse
import asyncio
import sys

from undercurrent import Client, OutputGap, UndercurrentError


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", help="Runtime home of an already running daemon")
    parser.add_argument("--bridge", help="Path to the matching uc-python-bridge")
    parser.add_argument("--session", help="Exact session ID to inspect instead of listing")
    parser.add_argument("--from-offset", type=int, default=0, help="First output byte offset")
    parser.add_argument("--max-pages", type=int, default=4, help="Maximum 4096-byte output pages")
    args = parser.parse_args()
    if args.from_offset < 0 or args.max_pages <= 0:
        parser.error("--from-offset must be nonnegative and --max-pages must be positive")
    return args


async def inspect(args):
    async with Client(home=args.home, bridge=args.bridge) as client:
        if args.session is None:
            sessions = await client.list()
            print(f"Connected; {len(sessions)} session(s)")
            for session in sessions:
                print(f"{session.id} {session.lifecycle} {session.name!r}")
            return

        session = await client.inspect(args.session)
        print(f"{session.id} revision={session.revision} lifecycle={session.lifecycle}")
        offset = args.from_offset
        for _ in range(args.max_pages):
            page = await client.read_output(session.id, from_offset=offset, max_bytes=4096)
            # next_offset is the available output end, not this page's end.
            returned_end = page.from_offset + len(page.data)
            print(f"[{page.from_offset}, {returned_end}) available_end={page.next_offset}")
            # repr keeps terminal control bytes visible without executing them.
            print(repr(page.data))
            if not page.has_more:
                print(f"Caught up at offset {returned_end}; later output may still arrive")
                return
            if returned_end <= offset:
                raise RuntimeError("Output pagination did not advance")
            offset = returned_end
        print(f"Page limit reached; continue explicitly with --from-offset {offset}")


def main():
    args = parse_args()
    try:
        asyncio.run(inspect(args))
    except OutputGap as error:
        print(
            f"Output is incomplete; earliest retained offset is {error.available_from}. "
            "Choose explicitly whether to resume there with --from-offset.",
            file=sys.stderr,
        )
        return 2
    except UndercurrentError as error:
        print(
            f"{error.category}: {error}; product_category={error.product_category}, "
            f"disposition={error.disposition}, effect_certainty={error.effect_certainty}",
            file=sys.stderr,
        )
        return 1
    except TimeoutError:
        print("Python deadline expired; no mutation was requested by this example", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
