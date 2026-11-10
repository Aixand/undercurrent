import asyncio
import unittest

from undercurrent import Client, Session, SessionEnded, SessionUpdate, UndercurrentError


def snapshot(*, interaction="available", lifecycle="running", revision=1, stamp=1):
    return Session({"id": "stable-id", "name": "agent", "revision": revision, "lifecycle_revision": 1,
        "lifecycle": {"state": lifecycle}, "health": {"status": "no_check"},
        "foreground": {
            "execution": {"state": "unknown", "basis": {"committed_at_millis": 1}},
            "interaction": {"state": interaction, "basis": {"committed_at_millis": stamp}},
            "constraint": {"state": "clear", "basis": {"committed_at_millis": 1}},
        }})


class ObservedClient(Client):
    def __init__(self):
        self.queue = asyncio.Queue()
        self.current = snapshot()
        self.closed = False
        self.failure = None
        self.journal = []

    async def inspect(self, _):
        if self.failure: raise self.failure
        return self.current

    async def watch(self, _):
        try:
            while True: yield await self.queue.get()
        finally:
            self.closed = True

    def publish(self, current=None, *, resync=False):
        self.current = current or self.current
        self.record(self.current)
        self.queue.put_nowait(SessionUpdate(self.current, resync))

    def record(self, current):
        self.journal.append({"sequence": len(self.journal) + 1, "session_revision": current.revision,
                             "observation": current.raw})

    async def _event_page(self, session, *, after_sequence=None, tail=False):
        rows = self.journal[-1:] if tail else [row for row in self.journal if row["sequence"] > (after_sequence or 0)]
        return {"items": rows, "checkpoint": len(self.journal), "has_more": False, "gap": None}


class WaitTests(unittest.IsolatedAsyncioTestCase):
    async def test_transient_ready_then_busy_restarts_dwell(self):
        client = ObservedClient()
        client.publish()
        task = asyncio.create_task(client.wait_for_stability("stable-id", interaction="available", stable_for=.12, timeout=1))
        await asyncio.sleep(.07)
        client.publish(snapshot(interaction="none", revision=2, stamp=2))
        await asyncio.sleep(.03)
        client.publish(snapshot(revision=3, stamp=3))
        await asyncio.sleep(.07)
        self.assertFalse(task.done())
        self.assertEqual((await task).revision, 3)
        self.assertTrue(client.closed)

    async def test_unrelated_revision_does_not_restart_timer(self):
        client = ObservedClient()
        client.publish()
        task = asyncio.create_task(client.wait_for_stability("id", interaction="available", stable_for=.12, timeout=1))
        for revision in range(2, 8):
            await asyncio.sleep(.03)
            client.publish(snapshot(revision=revision))
        self.assertTrue(task.done())
        await task

    async def test_gap_restarts_timer_even_when_state_matches(self):
        client = ObservedClient()
        client.publish()
        task = asyncio.create_task(client.wait_for_stability("id", interaction="available", stable_for=.12, timeout=1))
        await asyncio.sleep(.08)
        client.publish(resync=True)
        await asyncio.sleep(.06)
        self.assertFalse(task.done())
        await task

    async def test_initial_snapshot_and_multiple_axes(self):
        client = ObservedClient()
        client.publish()
        result = await client.wait("id", lifecycle=("running", "starting"), interaction="available", timeout=1)
        self.assertEqual(result.id, "stable-id")
        self.assertTrue(client.closed)

    async def test_revision_floor_ignores_old_ready_snapshot(self):
        client = ObservedClient()
        client.publish()
        task = asyncio.create_task(client.wait("id", interaction="available", after_revision=3, timeout=1))
        await asyncio.sleep(.02)
        self.assertFalse(task.done())
        client.publish(snapshot(revision=3))
        self.assertEqual((await task).revision, 3)

    async def test_coalesced_away_and_back_state_restarts_timer(self):
        client = ObservedClient()
        client.publish()
        task = asyncio.create_task(client.wait_for_stability("id", interaction="available", stable_for=.12, timeout=1))
        await asyncio.sleep(.08)
        # The watch never delivered the intervening non-ready revision.
        client.record(snapshot(interaction="none", revision=2, stamp=2))
        client.publish(snapshot(revision=3, stamp=3))
        await asyncio.sleep(.06)
        self.assertFalse(task.done())
        await task

    async def test_evidence_refreshes_do_not_prevent_stability(self):
        client = ObservedClient()
        client.publish()
        task = asyncio.create_task(client.wait_for_stability("id", interaction="available", stable_for=.12, timeout=1))
        for revision in range(2, 8):
            await asyncio.sleep(.03)
            client.publish(snapshot(revision=revision, stamp=revision))
        self.assertTrue(task.done())
        await task

    async def test_journal_retention_gap_restarts_dwell(self):
        client = ObservedClient()
        client.publish()
        original = client._event_page
        lost = True

        async def pages(session, **kwargs):
            nonlocal lost
            page = await original(session, **kwargs)
            if not kwargs.get("tail") and lost:
                lost = False
                page["gap"] = {"missing_from": 1, "missing_through": 2, "first_available": 3}
            return page

        client._event_page = pages
        task = asyncio.create_task(client.wait_for_stability("id", interaction="available", stable_for=.08, timeout=1))
        await asyncio.sleep(.12)
        self.assertFalse(task.done())
        await task

    async def test_disconnected_daemon_cannot_satisfy_dwell_timer(self):
        client = ObservedClient()
        client.publish()
        client.failure = UndercurrentError("connection lost")
        with self.assertRaises(UndercurrentError):
            await client.wait_for_stability("id", interaction="available", stable_for=.01, timeout=1)

    async def test_terminal_state_errors_promptly_and_closes_watch(self):
        client = ObservedClient()
        client.publish(snapshot(lifecycle="exited", interaction="none"))
        with self.assertRaises(SessionEnded):
            await client.wait("id", lifecycle="running", interaction="available")
        self.assertTrue(client.closed)

    async def test_timeout_and_cancellation_close_watch(self):
        client = ObservedClient()
        with self.assertRaises(TimeoutError):
            await client.wait("id", interaction="available", timeout=.01)
        self.assertTrue(client.closed)
        client = ObservedClient()
        task = asyncio.create_task(client.wait("id", interaction="available"))
        await asyncio.sleep(.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertTrue(client.closed)

    async def test_bad_conditions_fail_before_io(self):
        for values in ({}, {"interaction": "ready"}, {"execution": []}, {"turn": "complete"}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                await ObservedClient().wait("id", **values)
        for duration in (float("nan"), float("inf"), -1, True):
            with self.subTest(duration=duration), self.assertRaises(ValueError):
                await ObservedClient().wait_for_stability("id", stable_for=duration, interaction="available")
