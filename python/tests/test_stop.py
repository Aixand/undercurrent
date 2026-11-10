import unittest
from unittest.mock import AsyncMock

from undercurrent import Client


class StopTests(unittest.IsolatedAsyncioTestCase):
    async def test_graceful_only_is_explicitly_forwarded_to_the_daemon(self):
        client = Client()
        client._request = AsyncMock(return_value={"session":{"id":"original"}})
        await client.stop("original", graceful_only=True, expected_revision=7)
        args, values = client._request.call_args
        self.assertEqual(args,("stop_session",))
        self.assertEqual(values['session_id'],'original')
        self.assertTrue(values['graceful_only'])
        self.assertFalse(values['force'])
        self.assertEqual(values['expected_revision'],7)

    async def test_existing_stop_wire_shape_is_preserved(self):
        client = Client()
        client._request = AsyncMock(return_value={"session":{"id":"original"}})
        await client.stop("original")
        self.assertNotIn('graceful_only',client._request.call_args.kwargs)

    async def test_force_cannot_be_combined_with_graceful_only(self):
        client = Client()
        client._request = AsyncMock()
        with self.assertRaises(ValueError):
            await client.stop('original',force=True,graceful_only=True)
        client._request.assert_not_awaited()
