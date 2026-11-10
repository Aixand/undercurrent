#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check bounded MCP catalog recovery without hiding other adapter failures."""

from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
import mcp_probe


def failure(category):
    return {"result": {"isError": True, "structuredContent": {
        "error": {"product_category": category}}}}


class PaginationTests(unittest.TestCase):
    @patch.object(mcp_probe.time, "sleep")
    def test_expired_cursor_restarts_from_a_fresh_first_page(self, sleep):
        client = Mock()
        old = {"next_cursor": "old", "items": [{"id": "old-session"}]}
        fresh = {"next_cursor": "fresh", "items": [{"id": "first"}]}
        following = {"items": [{"id": "second"}]}
        client.call.side_effect = [old, fresh]
        client.call_raw.side_effect = [failure("cursor_expired"), {
            "result": {"structuredContent": {"data": following}}}]

        self.assertEqual(mcp_probe.pagination_pair(client, {"probe": "test"}),
                         (fresh, following))
        self.assertEqual(client.call.call_count, 2)
        self.assertEqual([call.kwargs["cursor"] for call in client.call_raw.call_args_list],
                         ["old", "fresh"])
        sleep.assert_called_once_with(0.05)

    @patch.object(mcp_probe.time, "sleep")
    def test_other_product_errors_fail_without_retry(self, sleep):
        client = Mock()
        client.call.return_value = {"next_cursor": "cursor"}
        client.call_raw.return_value = failure("unauthorized")
        with self.assertRaises(AssertionError):
            mcp_probe.pagination_pair(client, {})
        self.assertEqual(client.call_raw.call_count, 1)
        sleep.assert_not_called()

    @patch.object(mcp_probe.time, "sleep")
    def test_persistently_changing_catalog_fails_after_five_attempts(self, sleep):
        client = Mock()
        client.call.return_value = {"next_cursor": "cursor"}
        client.call_raw.return_value = failure("cursor_expired")
        with self.assertRaisesRegex(AssertionError, "five traversals"):
            mcp_probe.pagination_pair(client, {})
        self.assertEqual(client.call_raw.call_count, 5)
        self.assertEqual(sleep.call_count, 4)

    def test_protocol_error_is_not_retried(self):
        client = Mock()
        client.call.return_value = {"next_cursor": "cursor"}
        client.call_raw.return_value = {"error": {"code": -32603}}
        with self.assertRaises(AssertionError):
            mcp_probe.pagination_pair(client, {})
        self.assertEqual(client.call_raw.call_count, 1)


if __name__ == "__main__":
    unittest.main()
