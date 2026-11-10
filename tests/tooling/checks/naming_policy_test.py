#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify the naming policy rejects regressions while preserving real ownership."""

import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("naming_policy", ROOT / "scripts/checks/check-names.py")
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)


class NamingPolicyTests(unittest.TestCase):
    def test_retired_names_are_rejected_case_insensitively(self):
        for value in ["INTESSIO.connection.multiplex", "operation.undercurrent.request",
                      "operation/undercurrent", "AIXAND_DEVELOPMENT_HOME"]:
            with self.subTest(value=value):
                self.assertEqual(POLICY.violations("crates/example/src/lib.rs", value), [1])

    def test_company_names_paths_and_shared_installer_are_preserved(self):
        for value in ["https://github.com/Aixand/undercurrent", "~/.aixand/undercurrent",
                      r"%USERPROFILE%\.aixand\undercurrent", ".aixand/other-product", r"%LOCALAPPDATA%\Aixand\undercurrent",
                      '"repository": "Aixand/undercurrent"', "Aixand's documentation profile",
                      "Aixand Undercurrent", "aixand.my_terminal", "other@aixand.org",
                      "AIXAND_DEV_PYTHON", "AIXAND_DEV_ROOT", "AIXAND_DEV_TOOLCHAIN",
                      "AIXAND_INSTALLER_CACHE", "AIXAND_COMMAND_NAMES", "aixand_command",
                      "aixand_docs_check"]:
            with self.subTest(value=value):
                self.assertEqual(POLICY.violations("docs/example.md", value), [])

    def test_allowed_reference_does_not_mask_a_retired_name_on_the_same_line(self):
        self.assertEqual(POLICY.violations("docs/example.md",
                         "~/.aixand/undercurrent uses intessio.connection.multiplex"), [1])
        self.assertEqual(POLICY.violations("docs/example.md",
                         "https://github.com/Aixand/undercurrent uses AIXAND_DEVELOPMENT_HOME"), [1])

    def test_attribution_exception_is_exact(self):
        self.assertEqual(POLICY.violations(POLICY.ATTRIBUTION, "Intessio Foundation"), [])
        self.assertEqual(POLICY.violations("crates/other/NOTICE", "Intessio Foundation"), [1])

    def test_company_reference_does_not_mask_private_integrations(self):
        self.assertEqual(POLICY.violations("SECURITY.md", "mailto:corporate@aixand.org"), [])
        self.assertEqual(POLICY.violations("SECURITY.md",
                         "corporate@aixand.org uses operation/undercurrent"), [1])
        self.assertEqual(POLICY.violations("example.py", "AIXAND_DEV_ROOT and intessio.connection"), [1])

    def test_other_company_product_namespaces_are_rejected(self):
        for value in ["AIXAND_EXAMPLE_PRODUCT", "AIXAND_CONNECTION", "aixand-suite", "Aixand Suite"]:
            with self.subTest(value=value):
                self.assertEqual(POLICY.violations("docs/example.md", value), [1])

    def test_generic_runner_and_serial_carrier_detect_are_allowed(self):
        for value in ["test runner", "CI runner", "carrier detect signal", "carrier-detect signal",
                      "std::task::Wake", "external/workflow-engine"]:
            with self.subTest(value=value):
                self.assertEqual(POLICY.violations("example.rs", value), [])

    def test_product_names_and_external_integrations_are_allowed(self):
        self.assertEqual(POLICY.violations("example.rs",
                         "undercurrent.connection.multiplex\ncustom-workflow\nexample/workflow"), [])


if __name__ == "__main__":
    unittest.main()
