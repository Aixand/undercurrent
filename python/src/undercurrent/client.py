"""Async process operations. All process and observation authority remains in Undercurrent."""
import asyncio
from contextlib import aclosing, suppress
import hashlib
import math
import os
import unicodedata
from collections.abc import Mapping
from pathlib import Path
import uuid

from ._transport import Bridge, DEFAULT_REQUEST_BYTES
from .errors import OutputGap, SessionEnded, SessionRemoved, UndercurrentError
from .models import InputReceipt, OutputPage, Session, SessionUpdate

TERMINAL = {"stopped", "exited", "failed"}
STATES = {
    "lifecycle": {"starting", "running", "stopping", *TERMINAL},
    "execution": {"active", "quiescent", "unknown"},
    "interaction": {"none", "available", "requested"},
    "constraint": {"clear", "present"},
    "health": {"healthy", "unhealthy", "unknown", "no_check"},
}


def seconds(value, name, *, zero=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0 or (value == 0 and not zero):
        raise ValueError(f"{name} must be a finite {'nonnegative' if zero else 'positive'} number")
    return value


def session_id(value):
    return value.id if isinstance(value, Session) else str(value)


def attributes(value, kind):
    """Copy bounded domain attributes without echoing private values in errors."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{kind} must be a string mapping")
    if len(value) > 64:
        raise ValueError(f"{kind} accepts at most 64 entries")
    result = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key:
            raise ValueError(f"{kind} keys must be nonempty strings")
        if not isinstance(item, str):
            raise ValueError(f"{kind} values must be strings")
        for text, maximum in ((key, 64), (item, 256 if kind == "labels" else 4096)):
            try:
                valid = len(text.encode("utf-8")) <= maximum and not any(
                    unicodedata.category(char) == "Cc" for char in text)
            except UnicodeEncodeError:
                valid = False
            if not valid:
                raise ValueError(f"Invalid {kind} key or value (UTF-8 byte limit {maximum}; no control characters)")
        result[key] = item
    return result


class Client:
    """One async connection; closing it detaches without stopping sessions.

    A matching external daemon and uc-python-bridge must already be installed.
    The client never starts a daemon or compiles executables implicitly.

    Share only within one event loop. Finish or cancel calls and close iterators
    before closing the client; a closed instance cannot be reused. See
    [connection ownership](../../../docs/sdk/python/operations.md#share-and-close).
    """
    def __init__(self, *, home=None, bridge=None, request_timeout=30.0,
                 request_bytes=DEFAULT_REQUEST_BYTES):
        seconds(request_timeout, "request_timeout")
        if request_timeout > 300: raise ValueError("request_timeout must not exceed 300 seconds")
        self._bridge = Bridge(home=home, executable=bridge, request_timeout=request_timeout,
                              request_bytes=request_bytes)

    @property
    def home(self): return self._bridge.home

    @property
    def connection_options(self):
        return {"home": self.home, "bridge": str(self._bridge.executable),
                "request_timeout": self._bridge.request_timeout,
                "request_bytes": self._bridge.request_bytes}

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *_): await self.close()
    async def close(self): await self._bridge.close()

    async def connect(self): await self._bridge.connect()

    async def _request(self, operation, **values):
        result = await self._bridge.call("request", {"operation": operation, "input": values})
        if result["operation"] != operation:
            raise UndercurrentError("Bridge response operation mismatch")
        return result["result"]

    async def start(self, command, *, name=None, cwd=None, env=None, profile=None,
                    columns=120, rows=40, detection=True, labels=None, idempotency_key=None,
                    profile_binding="fixed", metadata=None):
        """Start a native session and return its initial summary.

        profile_binding defaults to fixed for compatibility. Select
        foreground_adaptive to follow the detected foreground application.

        Save a caller-chosen unique name before sending if a lost response must
        be reconciled. An idempotency key alone cannot recover a start across
        bridge or daemon restart. See
        [start recovery](../../../docs/sdk/python/recovery.md#start).
        """
        if isinstance(command, (str, bytes)) or not command or not all(isinstance(part, str) for part in command):
            raise ValueError("command must be a nonempty sequence of argument strings")
        if profile_binding not in ("fixed", "foreground_adaptive"):
            raise ValueError("profile_binding must be fixed or foreground_adaptive")
        labels, metadata = attributes(labels, "labels"), attributes(metadata, "metadata")
        cwd = str(Path(cwd or os.getcwd()).expanduser().resolve())
        result = await self._request("start_session", launch_mode="native",
            name=name or f"python-{uuid.uuid4().hex}",
            command={"program": command[0], "arguments": list(command[1:])},
            working_directory=cwd, terminal={"columns": columns, "rows": rows},
            environment={"schema_version": "0.1.0-beta", "variables": dict(env or {}), "file": None},
            profile=profile, profile_binding=profile_binding, labels=labels, metadata=metadata,
            execution={"detection_enabled": detection, "restart": {"mode": "never"}},
            mutation={"idempotency_key": idempotency_key or uuid.uuid4().hex})
        return Session(result["session"])

    async def list(self, *, labels=None, ids=None, name=None):
        labels = attributes(labels, "labels")
        result, cursor = [], None
        while True:
            page = await self._request("list_sessions", filter={"labels": dict(labels or {}), "ids": list(ids or []), "name": name},
                projection="with_labels", page_size=100, cursor=cursor)
            result.extend(Session(row) for row in page["items"])
            cursor = page["next_cursor"]
            if cursor is None: return result

    async def inspect(self, session):
        detail = await self._request("get_session_detail", session_id=session_id(session))
        return Session(detail["summary"])

    async def rename(self, session, name):
        """Change a session's display name while preserving its ID and process.

        A lost response can leave the rename applied. Inspect the saved session
        ID to determine its current name before retrying.
        """
        if not isinstance(name, str) or not name or name != name.strip():
            raise ValueError("name must be a nonempty string without surrounding whitespace")
        result = await self._request("rename_session", session_id=session_id(session), name=name,
            mutation={"idempotency_key": uuid.uuid4().hex})
        return Session(result["session"])

    async def metadata(self, session):
        """Read private durable metadata explicitly; summaries never include it."""
        detail = await self._request("get_session_detail", session_id=session_id(session),
                                     projection={"configuration": True})
        configuration = detail.get("configuration")
        if configuration is None:
            raise UndercurrentError("Configuration detail was not returned")
        return dict(configuration["metadata"])

    async def update_labels(self, session, *, labels=None, remove=None):
        """Persist a partial label update without restarting the session.

        Unmentioned labels are preserved. Private metadata is unchanged.
        """
        labels = attributes(labels, "labels")
        if remove is None:
            remove = []
        if (not isinstance(remove, (list, tuple)) or len(remove) > 64
                or any(not isinstance(key, str) for key in remove)):
            raise ValueError("remove must be a list or tuple of at most 64 label keys")
        removals = attributes(dict.fromkeys(remove, ""), "labels")
        if labels.keys() & removals.keys():
            raise ValueError("A label cannot be set and removed in the same update")
        result = await self._request("update_session_labels", session_id=session_id(session),
            set=labels, remove=list(removals), mutation={"idempotency_key": uuid.uuid4().hex})
        return Session(result["session"])

    async def send_input(self, session, text, *, submit=True, delivery_id=None,
                         expected_revision=None, preserve_observation=False, paste=None):
        """Write prompt text and optional Enter as one ordered delivery.

        paste=None follows the effective profile; True forces bracketed paste,
        False forces literal text. A receipt confirms terminal writes only,
        not application acceptance or task completion.
        """
        if not isinstance(text, str): raise TypeError("text must be a string")
        if paste is not None and not isinstance(paste, bool): raise TypeError("paste must be bool or None")
        kind = "auto_text" if paste is None else "paste" if paste else "text"
        items = [{"kind": kind, "text": text}]
        if submit: items.append({"kind": "key", "key": {"key": "enter"}})
        result = await self._request("send_terminal_input", session_id=session_id(session), items=items,
            delivery_id=delivery_id or uuid.uuid4().hex, expected_revision=expected_revision,
            observation_policy="preserve" if preserve_observation else "force_active")
        return InputReceipt(**result)

    async def send_key(self, session, key, *, delivery_id=None):
        key = {"key": key} if isinstance(key, str) else key
        return InputReceipt(**await self._request("send_terminal_input", session_id=session_id(session),
            items=[{"kind": "key", "key": key}], delivery_id=delivery_id or uuid.uuid4().hex))

    async def stop(self, session, *, force=False, graceful_only=False, expected_revision=None):
        """Stop an exact session; graceful_only refuses automatic force escalation."""
        if force and graceful_only:
            raise ValueError("force and graceful_only are mutually exclusive")
        options = {"graceful_only": True} if graceful_only else {}
        result = await self._request("stop_session", session_id=session_id(session), force=force,
            expected_revision=expected_revision, mutation={"idempotency_key": uuid.uuid4().hex}, **options)
        return Session(result["session"])

    async def read_output(self, session, *, from_offset=0, max_bytes=65536):
        page = await self._request("read_terminal_output", session_id=session_id(session),
            from_offset=from_offset, max_bytes=max_bytes)
        if page["result"] == "gap": raise OutputGap(page["available_from"])
        data = bytes(page["bytes"])
        if hashlib.sha256(data).hexdigest() != page["payload_digest_sha256"]:
            raise UndercurrentError("Output digest mismatch")
        return OutputPage(data, page["from_offset"], page["next_offset"], page["has_more"])

    async def read_screen(self, session):
        screen = await self._request("read_terminal_screen", session_id=session_id(session))
        return bytes(screen["contents"])

    async def events(self, session, *, after_sequence=None):
        """Yield event and gap envelopes from the retained publication journal.

        None starts live; a saved nonzero global event sequence requests replay.
        Handle gaps explicitly and save checkpoints after application processing.
        Use contextlib.aclosing when leaving iteration early. See
        [event recovery](../../../docs/sdk/python/streams.md#consume-durable-session-events).
        """
        spec = {"stream": "session_events", "input": {"session_ids": [session_id(session)], "after_sequence": after_sequence}}
        async with self._bridge.subscribe(spec) as stream:
            async for item in stream: yield item

    async def watch(self, session):
        """Yield snapshots and updates for an exact ID; expose every resynchronization.

        Updates can coalesce intermediate revisions. Reset continuity assumptions
        on resynchronized=True, including the initial snapshot. Use
        contextlib.aclosing when leaving iteration early. See
        [watch semantics](../../../docs/sdk/python/streams.md#watch-the-current-session-summary).
        """
        target = session_id(session)
        spec = {"stream": "session_watch", "input": {"query": {
            "filter": {"ids": [target]}, "projection": "operational", "page_size": 1, "cursor": None}}}
        async with self._bridge.subscribe(spec) as stream:
            async for item in stream:
                kind, value = item["item"], item["value"]
                if kind == "gap":
                    yield SessionUpdate(await self.inspect(target), resynchronized=True)
                elif kind == "session_snapshot":
                    match = next((row for row in value["items"] if row["id"] == target), None)
                    if match is None: raise SessionRemoved(f"Session {target} is absent")
                    yield SessionUpdate(Session(match), resynchronized=True)
                elif kind == "session_delta_batch":
                    for delta in value["deltas"]:
                        if delta["session_id"] != target: continue
                        if delta["kind"] == "removed": raise SessionRemoved(f"Session {target} was removed")
                        if delta["summary"] is not None: yield SessionUpdate(Session(delta["summary"]))

    async def wait(self, session, *, timeout=300.0, after_revision=None, **conditions):
        return await self.wait_for_stability(session, stable_for=0, timeout=timeout,
            after_revision=after_revision, **conditions)

    async def _event_page(self, session, *, after_sequence=None, tail=False):
        return await self._request("read_session_events", session_ids=[session_id(session)],
            after_sequence=after_sequence, tail=tail, include_observation=True,
            limit=1 if tail else 100)

    async def _audit_stability(self, session, cursor, revision, lifecycle_revision, selected):
        """Check event-time observations that a coalesced query watch can omit."""
        uninterrupted = True
        while True:
            page = await self._event_page(session, after_sequence=cursor)
            # A gap or absent historical observation cannot establish that the
            # predicate held between matching snapshots; invalidate this dwell.
            if page.get("gap") is not None: uninterrupted = False
            for event in page["items"]:
                if event["session_revision"] <= revision: continue
                observation = event.get("observation")
                if observation is None:
                    uninterrupted = False
                    continue
                historical = Session({**observation, "id": session_id(session),
                    "revision": event["session_revision"]})
                if (observation["lifecycle_revision"] != lifecycle_revision
                        or any(getattr(historical, axis) not in values for axis, values in selected.items())):
                    uninterrupted = False
            next_cursor = page["checkpoint"]
            if not page["has_more"]: return uninterrupted, next_cursor
            if next_cursor is None or next_cursor == cursor:
                raise UndercurrentError("Event replay did not advance its checkpoint")
            cursor = next_cursor

    async def wait_for_stability(self, session, *, stable_for=2.0, timeout=300.0,
                                 after_revision=None, **conditions):
        """Wait for observed predicates to hold; this is not task-success inference.

        OR within an axis, AND across axes. Reset on contradiction, resync, or
        process lifecycle revision changes. Validate live state before returning
        a dwell result, so a disconnected daemon cannot imply stable readiness.

        after_revision is an inclusive floor. Event auditing covers retained
        published observations; publication can lag durable changes. See
        [stability limits](../../../docs/sdk/python/state.md#how-the-stability-timer-works).
        """
        seconds(stable_for, "stable_for", zero=True)
        seconds(timeout, "timeout")
        if not conditions: raise ValueError("At least one state condition is required")
        selected = {}
        for axis, values in conditions.items():
            if axis not in STATES: raise ValueError(f"Unknown state axis: {axis}")
            values = {values} if isinstance(values, str) else set(values)
            if not values or not values <= STATES[axis]: raise ValueError(f"Invalid {axis} condition")
            selected[axis] = values
        if after_revision is not None and (isinstance(after_revision, bool) or not isinstance(after_revision, int) or after_revision < 0):
            raise ValueError("after_revision must be a nonnegative integer")

        def matches(current):
            match = all(getattr(current, axis) in values for axis, values in selected.items())
            if current.lifecycle in TERMINAL and not match: raise SessionEnded(current)
            return match and (after_revision is None or current.revision >= after_revision)

        loop = asyncio.get_running_loop()
        since, current, previous = None, None, None
        dwell_revision = None
        next_item = None
        async with asyncio.timeout(timeout), aclosing(self.watch(session)) as stream:
            try:
                # Capture the published event checkpoint before starting the dwell
                # so replay can reveal transitions coalesced out of watch updates.
                cursor = (await self._event_page(session, tail=True))["checkpoint"] if stable_for else None
                next_item = asyncio.create_task(anext(stream))
                while True:
                    delay = None if since is None else max(0, since + stable_for - loop.time())
                    done, _ = await asyncio.wait({next_item}, timeout=delay)
                    if done:
                        update = next_item.result()
                        current = update.session
                        stamp = current.raw.get("lifecycle_revision")
                        if update.resynchronized or stamp != previous: since = None
                        previous = stamp
                        if matches(current):
                            if stable_for == 0: return current
                            if since is None:
                                since, dwell_revision = loop.time(), current.revision
                        else: since = None
                        next_item = asyncio.create_task(anext(stream))
                    else:
                        # No timer-only return using a possibly disconnected snapshot.
                        verified = await self.inspect(session)
                        uninterrupted, cursor = await self._audit_stability(
                            session, cursor, dwell_revision, previous, selected)
                        if matches(verified) and verified.raw.get("lifecycle_revision") == previous and uninterrupted:
                            # Drain an already delivered contradictory event before success.
                            if not next_item.done(): return verified
                        else:
                            since = loop.time() if matches(verified) else None
                            dwell_revision = verified.revision
                            previous = verified.raw.get("lifecycle_revision")
            finally:
                if next_item is not None:
                    next_item.cancel()
                    with suppress(asyncio.CancelledError, StopAsyncIteration): await next_item
