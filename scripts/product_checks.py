# SPDX-License-Identifier: Apache-2.0
"""Run focused product-adapter checks and the existing SDK unit tests."""

import ast
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]

if __name__ == "__main__":
    if sys.argv[1:] == ["check"]:
        for filename in (
            "product_payload.py",
            "product_checks.py",
            "verify_product.py",
            "mcp_probe.py",
            "qualify_product_installation.py",
            "prepare_product_runtime.py",
        ):
            ast.parse(
                (ROOT / "scripts" / filename).read_text(encoding="utf-8"),
                filename=filename,
            )
        print("Product adapter syntax passed")
    elif sys.argv[1:] == ["test"]:
        sys.path.insert(0, str(ROOT / "python/src"))
        suite = unittest.defaultTestLoader.discover(str(ROOT / "python/tests"))
        raise SystemExit(
            not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
        )
    else:
        raise SystemExit("Choose check or test")
