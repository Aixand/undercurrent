import unittest
from unittest.mock import AsyncMock

from undercurrent import Client


class RenameTests(unittest.IsolatedAsyncioTestCase):
    async def test_rename_keeps_session_identity_and_supplies_mutation_key(self):
        client = Client()
        client._request = AsyncMock(return_value={"session": {"id": "one", "name": "codex-app-server-work-01"}})
        session = await client.rename("one", "codex-app-server-work-01")
        self.assertEqual(session.id, "one")
        self.assertEqual(session.name, "codex-app-server-work-01")
        args, values = client._request.call_args
        self.assertEqual(args, ("rename_session",))
        self.assertEqual(values["session_id"], "one")
        self.assertEqual(values["name"], session.name)
        self.assertTrue(values["mutation"]["idempotency_key"])

    async def test_invalid_name_does_not_reach_bridge(self):
        client = Client()
        client._request = AsyncMock()
        for name in [None, "", " name", "name "]:
            with self.subTest(name=name), self.assertRaises(ValueError):
                await client.rename("one", name)
        client._request.assert_not_awaited()
