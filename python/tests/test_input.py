import unittest
from undercurrent import Client

class InputTests(unittest.IsolatedAsyncioTestCase):
    async def test_modes_and_submission(self):
        client = Client()
        calls = []
        async def request(operation, **values):
            calls.append(values)
            return dict(session_id="test", delivered_at_millis=0, delivery_id=None, duplicate=False, foreground_revision=1)
        client._request = request
        for size in (1024, 1025, 16384):
            text = ("x\ny" * size)[:size]
            for paste, kind in ((None, "auto_text"), (True, "paste"), (False, "text")):
                for submit in (False, True):
                    await client.send_input("test", text, paste=paste, submit=submit)
                    expected = [{"kind": kind, "text": text}]
                    if submit:
                        expected.append({"kind": "key", "key": {"key": "enter"}})
                    self.assertEqual(calls[-1]["items"], expected)
        with self.assertRaises(TypeError):
            await client.send_input("test", "text", paste="yes")
