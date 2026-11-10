import json
import unittest
from types import SimpleNamespace

from undercurrent import Client
from undercurrent._transport import Bridge, DEFAULT_REQUEST_BYTES


class Sink:
    def __init__(self):
        self.data = None

    def write(self, data):
        self.data = data

    async def drain(self):
        pass


class LimitTests(unittest.IsolatedAsyncioTestCase):
    def test_request_options_survive_reconnection_and_reject_invalid_values(self):
        client = Client(request_bytes=64 * 1024 * 1024)
        replacement = Client(**client.connection_options)
        self.assertEqual(replacement.connection_options, client.connection_options)
        for value in (0, -1, True, 1.5, "1024", 2**32 - 1838):
            with self.assertRaises(ValueError):
                Client(request_bytes=value)

    async def test_bridge_preserves_large_escaped_and_unicode_input(self):
        bridge = Bridge()
        sink = Sink()
        bridge._process = SimpleNamespace(stdin=sink)
        text = "\0" * (5 * 1024 * 1024 - 3) + "界"
        await bridge._write({"text": text})
        self.assertGreater(len(sink.data), 30 * 1024 * 1024 - 20)
        self.assertEqual(json.loads(sink.data), {"text": text})
        self.assertEqual(bridge.max_line, DEFAULT_REQUEST_BYTES + 65536)

    async def test_line_boundary_rejects_before_writing_any_prefix(self):
        bridge = Bridge(request_bytes=1024)
        sink = Sink()
        bridge._process = SimpleNamespace(stdin=sink)
        # JSON string quotes and its terminating newline occupy three bytes.
        await bridge._write("x" * (bridge.max_line - 3))
        self.assertEqual(len(sink.data), bridge.max_line)
        sink.data = None
        with self.assertRaises(ValueError):
            await bridge._write("x" * (bridge.max_line - 2))
        self.assertIsNone(sink.data)
