import unittest
from undercurrent import Client

class StartTests(unittest.IsolatedAsyncioTestCase):
    async def test_metadata_launch_and_explicit_detail_without_summary_exposure(self):
        client = Client()
        calls = []
        async def request(operation, **values):
            calls.append((operation, values))
            if operation == "start_session":
                return {"session": {"id": "test", "labels": values["labels"]}}
            return {"summary": {"id": "test"}, "configuration": {"metadata": {"private": "context"}}}
        client._request = request
        session = await client.start(["/bin/sh"], labels={"app": "example-app"}, metadata={"private": "context"})
        self.assertEqual(calls[-1][1]["metadata"], {"private": "context"})
        self.assertNotIn("metadata", session.raw)
        self.assertEqual(await client.metadata(session), {"private": "context"})
        self.assertEqual(calls[-1][1]["projection"], {"configuration": True})
        await client.inspect(session)
        self.assertNotIn("projection", calls[-1][1])

    async def test_list_requests_public_labels_without_configuration(self):
        client = Client()
        calls = []
        async def request(operation, **values):
            calls.append(values)
            return {"items": [{"id": "test", "labels": {"app": "example-app"}}], "next_cursor": None}
        client._request = request
        rows = await client.list(labels={"app": "example-app"})
        self.assertEqual(rows[0].raw["labels"], {"app": "example-app"})
        self.assertEqual(calls[0]["projection"], "with_labels")
        self.assertNotIn("configuration", calls[0])

    async def test_invalid_metadata_rejected_before_io(self):
        client = Client()
        async def request(*args, **kwargs):
            self.fail("invalid metadata reached bridge IO")
        client._request = request
        for value in ({"private": "x" * 4097}, {"private": "secret\nvalue"}, {"private": {"nested": True}}, {"": "value"}):
            with self.assertRaises(ValueError) as caught:
                await client.start(["/bin/sh"], metadata=value)
            self.assertNotIn("secret", str(caught.exception))

    async def test_binding_default_and_explicit_modes(self):
        client = Client()
        calls = []
        async def request(operation, **values):
            calls.append((operation, values))
            return {"session": {"id": "test"}}
        client._request = request
        for profile in (None, "shell"):
            for option in ({}, {"profile_binding": "fixed"}, {"profile_binding": "foreground_adaptive"}):
                await client.start(["/bin/sh"], profile=profile, **option)
                operation, values = calls[-1]
                self.assertEqual(operation, "start_session")
                self.assertEqual(values["profile"], profile)
                self.assertEqual(values["profile_binding"], option.get("profile_binding", "fixed"))
        count = len(calls)
        for invalid in (None, True, "adaptive", "", [], {}):
            with self.assertRaises(ValueError):
                await client.start(["/bin/sh"], profile_binding=invalid)
        self.assertEqual(len(calls), count)
