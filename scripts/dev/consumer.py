#!/usr/bin/env python3
"""Portable artifact CLI; copied into an installation without the developer tree."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

if len(sys.argv) > 1 and sys.argv[1] == "exec":
    import argparse
    from operations import execute
    from core import Failure

    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", type=Path, required=True)
    parser.add_argument("--command", required=True)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args(sys.argv[2:])
    try:
        raise SystemExit(
            execute(
                args.prefix,
                args.command,
                args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments,
            )
        )
    except (Failure, OSError, ValueError, KeyError, TypeError) as error:
        print("error: " + str(error), file=sys.stderr)
        raise SystemExit(1)
else:
    from cli import main

    raise SystemExit(main())
