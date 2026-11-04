"""Consumer concurrency, handoff and interruption invariants."""

import asyncio
import unittest

from module.durable_queue import DurableQueue
from module.task_store import TaskStore


async def eventually(predicate):
    for _ in range(150):
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("Condition did not become true")


class QueueTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = TaskStore(":memory:")
        self.queue = None

    async def asyncTearDown(self):
        if self.queue:
            await self.queue.stop()
        self.store.close()

    def add(self, key, kind="download"):
        return self.store.enqueue({"kind": kind, "group_id": key}, key)

    async def test_download_and_upload_use_separate_consumers_and_keep_id(self):
        downloads, uploads = [], []
        gate = asyncio.Event()

        async def handle(row):
            if row["payload"]["kind"] == "download":
                downloads.append(row["id"])
                self.store.handoff(row["id"], row["payload"], token=row["claim_token"])
            else:
                uploads.append(row["id"])
                await gate.wait()
                return {"uploaded": True}

        first = self.add("one")
        second = self.add("two")
        self.queue = DurableQueue(
            self.store, handle, concurrency=1, upload_concurrency=1
        )
        self.queue.start()
        await eventually(lambda: len(downloads) == 2 and bool(uploads))
        self.assertEqual(self.store.get(first["id"])["state"], "running")
        gate.set()
        await eventually(lambda: self.store.stats()["completed"] == 2)
        self.assertEqual(set(uploads), {first["id"], second["id"]})

    async def test_pause_then_immediate_resume_does_not_overlap_claims(self):
        row = self.add("pause")
        entered = asyncio.Event()
        active, peak, calls = 0, 0, 0

        async def handle(claim):
            nonlocal active, peak, calls
            active += 1
            peak = max(peak, active)
            calls += 1
            entered.set()
            try:
                if calls == 1:
                    await asyncio.sleep(10)
            finally:
                active -= 1

        self.queue = DurableQueue(self.store, handle, concurrency=2)
        self.queue.start()
        await entered.wait()
        self.store.pause(row["id"])
        self.store.resume(row["id"])
        await eventually(lambda: self.store.get(row["id"])["state"] == "completed")
        self.assertEqual(peak, 1)
        self.assertEqual(calls, 2)

    async def test_network_retry_and_permanent_error_terminal(self):
        row = self.add("retry")
        calls = 0

        async def handle(claim):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ConnectionError("temporary")
            return {"ok": True}

        self.queue = DurableQueue(self.store, handle, base_delay=0.001)
        self.queue.start()
        await eventually(lambda: self.store.get(row["id"])["state"] == "completed")
        self.assertEqual(calls, 2)

    async def test_shutdown_recovers_work_without_cancelling_paused(self):
        row = self.add("shutdown")
        paused = self.add("paused")
        self.store.pause(paused["id"])
        started = asyncio.Event()

        async def handle(claim):
            started.set()
            await asyncio.sleep(10)

        self.queue = DurableQueue(self.store, handle)
        self.queue.start()
        await started.wait()
        await self.queue.stop()
        self.assertEqual(self.store.get(row["id"])["state"], "queued")
        self.assertEqual(self.store.get(row["id"])["attempt"], 0)
        self.assertEqual(self.store.get(paused["id"])["state"], "paused")
