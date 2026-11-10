import unittest

from undercurrent import Client


class UpdateLabelsTests(unittest.IsolatedAsyncioTestCase):
    async def test_atomic_patch_targets_one_session_and_preserves_other_attributes(self):
        client = Client()
        calls = []
        async def request(operation, **values):
            calls.append((operation, values))
            return {"session": {"id": "one", "labels": {"app": "example-app", **values["set"]}}}
        client._request = request
        session = await client.update_labels("one", labels={"aixand.my_terminal": "true"})
        self.assertEqual(session.raw["labels"], {"app": "example-app", "aixand.my_terminal": "true"})
        self.assertEqual(calls[0][0], "update_session_labels")
        self.assertEqual(calls[0][1]["session_id"], "one")
        self.assertEqual(calls[0][1]["remove"], [])
        self.assertTrue(calls[0][1]["mutation"]["idempotency_key"])
        await client.update_labels(session, remove=["aixand.my_terminal"])
        self.assertEqual(calls[1][1]["set"], {})
        self.assertEqual(calls[1][1]["remove"], ["aixand.my_terminal"])

    async def test_invalid_patch_is_rejected_before_bridge_io(self):
        client = Client()
        async def request(*args, **kwargs):
            self.fail("invalid label patch reached bridge IO")
        client._request = request
        for options in [
            {"labels": {"app": "a"}, "remove": ["app"]},
            {"labels": {"x": "bad\nvalue"}},
            {"remove": "app"},
            {"remove": [""]},
            {"remove": ["bad\nkey"]},
            {"remove": [None]},
            {"remove": [[]]},
            {"remove": [str(n) for n in range(65)]},
        ]:
            with self.assertRaises(ValueError):
                await client.update_labels("one", **options)
