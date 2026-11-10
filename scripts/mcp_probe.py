# SPDX-License-Identifier: Apache-2.0
"""Standard-library MCP client and installed-adapter verification.

Only exercise a caller-owned disposable daemon home. No client registrations,
authentication changes, or external agent services are involved.
"""

from __future__ import annotations

import json
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time
import uuid


class Client:
    def __init__(self, binary, home, *, read_only=False, protocol="2025-11-25"):
        self.errors = tempfile.TemporaryFile(mode="w+t")
        self.process = subprocess.Popen(
            [str(binary), "--home", str(home), *(["--read-only"] if read_only else [])],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.errors,
            text=True, encoding="utf-8", bufsize=1,
        )
        self.inbox = queue.Queue()
        self.pending = {}
        self.sequence = 0
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        self.protocol = protocol
        try:
            if protocol != "2026-07-28":
                reply = self.request("initialize", {
                    "protocolVersion": protocol, "capabilities": {},
                    "clientInfo": {"name": "undercurrent-qualification", "version": "1"},
                })
                assert reply["result"]["serverInfo"]["name"] == "uc-mcp", reply
                assert reply["result"]["protocolVersion"] == protocol, reply
                self.notify("notifications/initialized", {})
        except BaseException:
            self.close()
            raise

    def _read(self):
        try:
            for line in self.process.stdout:
                self.inbox.put(json.loads(line))
        except BaseException as error:
            self.inbox.put(error)
        finally:
            self.inbox.put(None)

    def _write(self, message):
        if self.protocol == "2026-07-28":
            message.setdefault("params", {})["_meta"] = {
                "io.modelcontextprotocol/protocolVersion": self.protocol,
                "io.modelcontextprotocol/clientInfo": {
                    "name": "undercurrent-qualification", "version": "1"
                },
                "io.modelcontextprotocol/clientCapabilities": {},
            }
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def begin(self, method, params):
        self.sequence += 1
        self._write({"jsonrpc": "2.0", "id": self.sequence, "method": method, "params": params})
        return self.sequence

    def receive(self, request_id, timeout=20):
        deadline = time.monotonic() + timeout
        while request_id not in self.pending:
            item = self.inbox.get(timeout=max(0.01, deadline - time.monotonic()))
            if item is None:
                self.errors.seek(0)
                raise RuntimeError("MCP stdout closed: " + self.errors.read()[-3000:])
            if isinstance(item, BaseException):
                raise item
            if "id" in item:
                self.pending[item["id"]] = item
        return self.pending.pop(request_id)

    def request(self, method, params):
        return self.receive(self.begin(method, params))

    def notify(self, method, params):
        self._write({"jsonrpc": "2.0", "method": method, "params": params})

    def call_raw(self, tool_name, **arguments):
        return self.request("tools/call", {"name": tool_name, "arguments": arguments})

    def call(self, tool_name, **arguments):
        reply = self.call_raw(tool_name, **arguments)
        assert "error" not in reply, reply
        result = reply["result"]
        assert not result.get("isError", False), result
        return result["structuredContent"]["data"]

    def close(self):
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
            raise AssertionError("MCP adapter failed to exit after stdin closed") from None
        finally:
            self.reader.join(timeout=2)
            self.process.stdout.close()
            self.errors.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def pagination_pair(client, labels):
    """Restart a read-only catalog traversal when a concurrent revision expires it."""
    for attempt in range(5):
        page = client.call("list_sessions", labels=labels, page_size=1)
        assert page["next_cursor"], page
        reply = client.call_raw("list_sessions", labels=labels, page_size=1, cursor=page["next_cursor"])
        assert "error" not in reply, reply
        result = reply["result"]
        if not result.get("isError", False):
            return page, result["structuredContent"]["data"]
        error = result["structuredContent"]["error"]
        assert error.get("product_category") == "cursor_expired", result
        assert attempt < 4, f"Catalog did not stabilize after five traversals: {result}"
        time.sleep(0.05)


def exercise(binary, home):
    """Use a running disposable daemon; retain only terminal session records."""
    run = "mcp-probe-" + uuid.uuid4().hex[:10]
    sessions = []
    with Client(binary, home) as client:
        tools = client.request("tools/list", {})["result"]["tools"]
        assert len(tools) == 9
        status = client.call("get_status")
        assert status["connected"], status
        try:
            # A real PTY child provides observable acknowledgements, not just echo.
            session = client.call("start_session", name=run, program=sys.executable,
                arguments=["-I", "-B", "-u", "-c",
                    "import sys,time; print('mcp-ready',flush=True); value=sys.stdin.readline().strip(); print('mcp-accepted:'+value,flush=True); time.sleep(120)"],
                cwd=str(home), labels={"mcp-probe": run})["session"]
            session_id = session["id"]
            sessions.append(session_id)

            def await_output(marker):
                deadline = time.monotonic() + 15
                while True:
                    output = client.call("read_output", session_id=session_id, include_bytes=True)
                    if marker in output.get("text", ""):
                        assert bytes(output["bytes"]).decode(errors="replace") == output["text"]
                        return output
                    assert time.monotonic() < deadline, output
                    time.sleep(0.05)

            await_output("mcp-ready")
            detail = client.call("inspect_session", session_id=session_id)
            stale = client.call_raw("send_input", session_id=session_id, delivery_id=uuid.uuid4().hex,
                expected_revision=0, items=[{"kind": "text", "text": "wrong"}])
            assert stale["result"]["isError"], stale
            receipt = client.call("send_input", session_id=session_id, delivery_id=uuid.uuid4().hex,
                items=[{"kind": "text", "text": "hello"}, {"kind": "key", "key": "enter"}])
            assert receipt["session_id"] == session_id
            output = await_output("mcp-accepted:hello")
            assert "wrong" not in output["text"]
            first = client.call("read_output", session_id=session_id, max_bytes=4, include_bytes=True)
            following = client.call("read_output", session_id=session_id, from_offset=first["to_offset"], include_bytes=True)
            assert first["to_offset"] == following["from_offset"]
            assert bytes(first["bytes"] + following["bytes"]).decode(errors="replace").startswith(output["text"])
            assert client.call("read_screen", session_id=session_id, max_bytes=64)["total_bytes"] > 0

            wait = client.call("wait_for_state", session_id=session_id, lifecycle=["running"], timeout_seconds=2)
            assert wait["outcome"] == "matched", wait
            wait = client.call("wait_for_state", session_id=session_id, lifecycle=["exited"], timeout_seconds=1)
            assert wait["outcome"] == "timeout", wait
            wait = client.call("wait_for_state", session_id=session_id, lifecycle=["running"],
                min_revision=2**63, timeout_seconds=1)
            assert wait["outcome"] == "timeout", wait

            # Four waiting slots cannot consume the eight finite-operation slots.
            waits = [client.begin("tools/call", {"name": "wait_for_state", "arguments": {
                "session_id": session_id, "lifecycle": ["exited"], "timeout_seconds": 30,
            }}) for _ in range(4)]
            time.sleep(0.2)
            overloaded = client.call_raw("wait_for_state", session_id=session_id, lifecycle=["exited"], timeout_seconds=1)
            assert overloaded["result"]["structuredContent"]["error"]["category"] == "overloaded", overloaded
            assert client.call("inspect_session", session_id=session_id)["summary"]["id"] == session_id
            for request in waits:
                client.notify("notifications/cancelled", {"requestId": request, "reason": "probe finished"})
            time.sleep(0.3)
            assert client.call("wait_for_state", session_id=session_id, lifecycle=["running"], timeout_seconds=1)["outcome"] == "matched"

            # Second worker makes catalog pagination observable on the same client.
            other = client.call("start_session", name=run+"-second", program=sys.executable,
                arguments=["-I", "-B", "-c", "import time; time.sleep(120)"],
                cwd=str(home), labels={"mcp-probe": run})["session"]
            sessions.append(other["id"])
            assert client.call("wait_for_state", session_id=other["id"],
                lifecycle=["running"], timeout_seconds=10)["outcome"] == "matched"
            page, following = pagination_pair(client, {"mcp-probe": run})
            assert {page["items"][0]["id"], following["items"][0]["id"]} == set(sessions)

            # A different adapter connection sees the same daemon-owned process.
            with Client(binary, home, read_only=True) as observer:
                assert len(observer.request("tools/list", {})["result"]["tools"]) == 6
                assert observer.call("inspect_session", session_id=session_id)["summary"]["lifecycle"]["state"] == "running"
                denied = observer.call_raw("stop_session", session_id=session_id)
                assert "error" in denied or denied["result"].get("isError"), denied
                # EOF while a wait is active must release it and let the adapter exit.
                observer.begin("tools/call", {"name": "wait_for_state", "arguments": {
                    "session_id": session_id, "lifecycle": ["exited"], "timeout_seconds": 60,
                }})
                time.sleep(0.1)
            assert client.call("inspect_session", session_id=session_id)["summary"]["lifecycle"]["state"] == "running"
            stale_stop = client.call_raw("stop_session", session_id=session_id, mode="force", expected_revision=0)
            assert stale_stop["result"]["isError"], stale_stop
            assert client.call("inspect_session", session_id=session_id)["summary"]["lifecycle"]["state"] == "running"
            # Windows cannot send the runtime's POSIX graceful signal. A default
            # stop must fail before admission, leaving explicit stopping usable.
            if sys.platform == "win32":
                unsupported = client.call_raw("stop_session", session_id=other["id"])
                assert unsupported["result"]["isError"], unsupported
                error = unsupported["result"]["structuredContent"]["error"]
                assert error["category"] == "unsupported_mode", error
                assert error["effect_certainty"] == "known_absent", error
                assert client.call("inspect_session", session_id=other["id"])["summary"]["lifecycle"]["state"] == "running"
                client.call("stop_session", session_id=other["id"], mode="graceful_then_force")
            else:
                # Default stop uses graceful_only without forced escalation.
                client.call("stop_session", session_id=other["id"])
            assert client.call("wait_for_state", session_id=other["id"], lifecycle=["stopped", "exited", "failed"], timeout_seconds=10)["outcome"] == "matched"
        finally:
            for session_id in sessions:
                client.call("stop_session", session_id=session_id, mode="force")
                ended = client.call("wait_for_state", session_id=session_id,
                    lifecycle=["stopped", "exited", "failed"], timeout_seconds=10)
                assert ended["outcome"] == "matched", ended
    return {"tools": len(tools), "pty_input": True, "pagination": True,
            "bounded_waits": True, "read_only": True,
            "graceful_only_supported": sys.platform != "win32", "sessions": sessions}
