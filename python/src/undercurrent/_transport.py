"""Bounded async stdio connection to our private public-SDK bridge."""
import asyncio
from contextlib import asynccontextmanager, suppress
import json
import os
from pathlib import Path
import shutil
import uuid

from .errors import UndercurrentError

DEFAULT_REQUEST_BYTES = 32 * 1024 * 1024 - 1838


class Bridge:
    def __init__(self, *, home=None, executable=None, request_timeout=30.0,
                 request_bytes=DEFAULT_REQUEST_BYTES):
        if isinstance(request_bytes, bool) or not isinstance(request_bytes, int) or not 0 < request_bytes <= 2**32 - 1 - 1838:
            raise ValueError("request_bytes must be a positive integer fitting a framed request")
        self.request_bytes = request_bytes
        # The private envelope needs room beyond the product JSON. Keep receive
        # capacity for ordinary responses even when callers restrict requests.
        self.max_line = max(4 * 1024 * 1024, request_bytes + 65536)
        selected_home = home or os.environ.get("UNDERCURRENT_HOME", str(Path.home()/".aixand"/"undercurrent"))
        if os.name == "nt":
            # The named-pipe identity includes the configured UTF-16 spelling.
            # Preserve absolute paths, including their case and separators.
            self.home = os.path.expanduser(os.fspath(selected_home))
            if not os.path.isabs(self.home):
                self.home = os.path.abspath(self.home)
        else:
            self.home = str(Path(selected_home).expanduser().resolve())
        self.executable = executable or os.environ.get("UNDERCURRENT_PYTHON_BRIDGE", "uc-python-bridge")
        self.request_timeout = request_timeout
        self._process = None
        self._reader = None
        self._pending = {}
        self._streams = {}
        self._write_lock = asyncio.Lock()
        self._connect_lock = asyncio.Lock()
        self._slots = asyncio.Semaphore(16)
        self._closed = False
        self._failure = None

    async def connect(self):
        async with self._connect_lock:
            if self._closed:
                raise UndercurrentError("Client is closed")
            if self._failure:
                raise self._failure
            if self._process is not None:
                return
            executable = shutil.which(str(self.executable))
            if executable is None:
                raise UndercurrentError("uc-python-bridge was not found; install the bridge or set UNDERCURRENT_PYTHON_BRIDGE")
            self.executable = executable
            self._process = await asyncio.create_subprocess_exec(
                executable, "--request-bytes", str(self.request_bytes),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL, limit=self.max_line,
                env=os.environ | {"UNDERCURRENT_HOME": self.home},
            )
            try:
                async with asyncio.timeout(self.request_timeout):
                    message = json.loads(await self._process.stdout.readline())
                if message.get("type") != "ready" or message.get("protocol") != "0.1.0-beta":
                    raise UndercurrentError.from_wire(message.get("error", {"message": "Incompatible Python bridge"}))
            except BaseException:
                # Cancellation during the handshake also owns a spawned child;
                # reap it before abandoning startup, even though no reader exists yet.
                await self.close()
                raise
            self._reader = asyncio.create_task(self._read(), name="undercurrent-bridge-reader")

    async def _write(self, message):
        data = (json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
        if len(data) > self.max_line:
            raise ValueError(f"Bridge request exceeds {self.max_line} bytes")
        async with self._write_lock:
            if self._failure:
                raise self._failure
            if self._closed or self._process is None:
                raise UndercurrentError("Client is closed")
            self._process.stdin.write(data)
            await self._process.stdin.drain()

    async def _read(self):
        failure = UndercurrentError("Python bridge disconnected; mutation effects may be uncertain", category="Unavailable")
        try:
            while line := await self._process.stdout.readline():
                message = json.loads(line)
                kind, key = message["type"], message.get("id")
                if kind == "fatal":
                    raise UndercurrentError.from_wire(message["error"])
                if kind in ("result", "error"):
                    future = self._pending.get(key)
                    if future is not None and not future.done():
                        if kind == "error":
                            future.set_exception(UndercurrentError.from_wire(message["error"]))
                        else:
                            future.set_result(message["result"])
                elif kind in ("item", "stream_error") and key in self._streams:
                    item = message.get("item") if kind == "item" else UndercurrentError.from_wire(message["error"])
                    # This reader also routes replies for other calls. Waiting on
                    # one full queue would stall them; overflow fails the connection.
                    self._streams[key].put_nowait(item)
        except asyncio.CancelledError:
            pass
        except Exception as error:
            failure = error if isinstance(error, UndercurrentError) else UndercurrentError("Invalid Python bridge response")
        finally:
            self._failure = failure
            for future in self._pending.values():
                if not future.done(): future.set_exception(failure)
            for queue in self._streams.values():
                # Discard stale observations rather than returning readiness after disconnection.
                while not queue.empty(): queue.get_nowait()
                queue.put_nowait(failure)

    async def call(self, op, body, *, key=None):
        """Correlate one reply without replaying requests after failure.

        Connection, slot admission, and writing precede the reply timer; callers
        needing a total budget must supply an outer timeout. See
        [deadline boundaries](../../../docs/sdk/python/recovery.md#deadlines-and-cancellation).
        """
        await self.connect()
        async with self._slots:
            key = key or uuid.uuid4().hex
            future = asyncio.get_running_loop().create_future()
            self._pending[key] = future
            try:
                await self._write({"id": key, "op": op, "body": body, "timeout": self.request_timeout})
                # A timed-out/cancelled mutation is never replayed automatically.
                async with asyncio.timeout(self.request_timeout + 1):
                    return await future
            finally:
                # Removing correlation does not cancel the Rust worker. It may
                # still apply a mutation; _read ignores its eventual late reply.
                self._pending.pop(key, None)
                if not future.done(): future.cancel()

    @asynccontextmanager
    async def subscribe(self, spec):
        key = uuid.uuid4().hex
        queue = asyncio.Queue(maxsize=2)
        # The first item can follow activation before call() resumes. Register
        # its destination now so the shared reader cannot discard that item.
        self._streams[key] = queue
        try:
            await self.call("subscribe", spec, key=key)

            async def items():
                while True:
                    item = await queue.get()
                    if isinstance(item, Exception): raise item
                    # ACK releases the bridge's next item before user processing;
                    # applications must persist their own processing checkpoint.
                    # See [delivery](../../../docs/sdk/python/streams.md#backpressure-and-connection-recovery).
                    await self._write({"id": key, "op": "ack"})
                    yield item

            yield items()
        finally:
            self._streams.pop(key, None)
            with suppress(Exception):
                await self._write({"id": key, "op": "unsubscribe"})

    async def close(self):
        self._closed = True
        if self._process is not None:
            if self._process.stdin: self._process.stdin.close()
            # Keep the reader alive while waiting: bridge workers may still
            # write replies, and blocked stdout can delay the child's exit.
            try:
                async with asyncio.timeout(2): await self._process.wait()
            except TimeoutError:
                with suppress(ProcessLookupError): self._process.kill()
                await self._process.wait()
        if self._reader is not None:
            self._reader.cancel()
            with suppress(asyncio.CancelledError): await self._reader
