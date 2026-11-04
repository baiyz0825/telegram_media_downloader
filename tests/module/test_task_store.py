"""Task persistence, concurrency and scheduler regression tests."""

import os
import tempfile
import threading
import unittest

from module.task_store import TaskStore


class TaskStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.temp.name, "tasks.sqlite")
        self.store = TaskStore(self.path)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def enqueue(self, key="a", **kwargs):
        return self.store.enqueue(
            {"group_id": 42, "node": {"chat_id": -1001}}, key, **kwargs
        )

    def test_idempotency_and_payload(self):
        first = self.enqueue()
        duplicate = self.store.enqueue({"changed": True}, "a")
        self.assertEqual(first["id"], duplicate["id"])
        self.assertEqual(first["payload"], duplicate["payload"])
        self.assertEqual(self.store.stats()["total"], 1)

    def test_priority_and_atomic_claim(self):
        self.enqueue("low")
        high = self.enqueue("high", priority=10)
        self.assertEqual(self.store.claim()["id"], high["id"])
        self.assertEqual(self.store.claim()["attempt"], 1)
        self.assertIsNone(self.store.claim())

    def test_two_connections_never_claim_same_task(self):
        for i in range(40):
            self.enqueue(str(i))
        second = TaskStore(self.path, recover_running=False)
        claimed = []
        lock = threading.Lock()

        def consume(store):
            while True:
                row = store.claim()
                if row is None:
                    return
                with lock:
                    claimed.append(row["id"])

        threads = [
            threading.Thread(target=consume, args=(store,))
            for store in (self.store, second)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        second.close()
        self.assertEqual(len(claimed), 40)
        self.assertEqual(len(set(claimed)), 40)

    def test_retry_delay_and_budget(self):
        task = self.enqueue(max_attempts=2)
        self.store.claim()
        self.store.fail(task["id"], "offline", "network", retry_delay=60)
        waiting = self.store.get(task["id"])
        self.assertEqual(waiting["state"], "retry_wait")
        self.assertEqual(waiting["error_kind"], "network")
        self.assertIsNone(self.store.claim(now=waiting["next_run"] - 1))
        self.assertEqual(self.store.claim(now=waiting["next_run"])["attempt"], 2)
        self.store.fail(task["id"], "offline", "network", retry_delay=60)
        self.assertEqual(self.store.get(task["id"])["state"], "failed")
        self.assertIsNone(self.store.claim(now=waiting["next_run"] + 100))

    def test_pause_cancel_progress_and_resume(self):
        task = self.enqueue()
        ident = task["id"]
        self.store.claim()
        self.store.update_progress(ident, 128, 512, "/tmp/a.part")
        self.assertTrue(self.store.pause(ident))
        self.assertFalse(self.store.complete(ident))
        self.assertFalse(self.store.fail(ident, "interrupted"))
        self.assertIsNone(self.store.claim())
        self.assertTrue(self.store.resume(ident))
        self.assertEqual(self.store.claim()["downloaded_bytes"], 128)
        self.assertTrue(self.store.cancel(ident))
        self.assertFalse(self.store.complete(ident))
        self.assertEqual(self.store.get(ident)["state"], "cancelled")

    def test_restart_recovers_running_but_preserves_paused(self):
        first = self.enqueue("first")
        second = self.enqueue("second")
        self.store.claim()
        self.store.pause(second["id"])
        self.store.update_progress(first["id"], 200, 1000)
        self.store.close()
        self.store = TaskStore(self.path)
        recovered = self.store.get(first["id"])
        self.assertEqual(recovered["state"], "queued")
        self.assertEqual(recovered["attempt"], 0)
        self.assertEqual(recovered["downloaded_bytes"], 200)
        self.assertEqual(self.store.get(second["id"])["state"], "paused")

    def test_completion_survives_restart(self):
        row = self.enqueue()
        self.store.claim()
        self.store.complete(row["id"], {"path": "/tmp/file"})
        self.store.close()
        self.store = TaskStore(self.path)
        self.assertEqual(self.store.get(row["id"])["result"], {"path": "/tmp/file"})
        self.assertIsNone(self.store.claim())

    def test_stale_consumer_cannot_change_new_claim(self):
        task = self.enqueue()
        old = self.store.claim()["claim_token"]
        self.store.pause(task["id"])
        self.store.resume(task["id"])
        new = self.store.claim()["claim_token"]
        self.assertNotEqual(old, new)
        self.assertFalse(self.store.complete(task["id"], token=old))
        self.assertFalse(self.store.fail(task["id"], "old", token=old))
        self.assertFalse(self.store.update_progress(task["id"], 10, token=old))
        self.assertFalse(self.store.checkpoint(task["id"], "uploading", token=old))
        self.assertTrue(
            self.store.checkpoint(
                task["id"],
                "uploading",
                path="file",
                md5="a" * 32,
                result={"downloaded": True},
                token=new,
            )
        )
        self.assertEqual(self.store.get(task["id"])["stage"], "uploading")
        self.assertTrue(self.store.complete(task["id"], token=new))

    def test_owner_filter(self):
        self.store.enqueue({"from_user_id": 123}, "first")
        self.store.enqueue({"node": {"from_user_id": 456}}, "second")
        self.assertEqual(self.store.count_tasks(owner_id=123), 1)
        self.assertEqual(self.store.stats(owner_id=456)["total"], 1)
        self.assertEqual(len(self.store.list_tasks(owner_id=789)), 0)

    def test_consumer_kinds_and_handoff(self):
        task = self.store.enqueue({"kind": "download", "group_id": "g"}, "file")
        self.assertIsNone(self.store.claim(kinds=["upload"]))
        download = self.store.claim(kinds=["download", "scan"])
        token = download["claim_token"]
        self.store.checkpoint(
            task["id"],
            "downloaded",
            "/tmp/file",
            "a" * 32,
            {"download_done": True},
            token=token,
        )
        self.assertFalse(
            self.store.handoff(task["id"], download["payload"], token="stale")
        )
        self.assertTrue(
            self.store.handoff(task["id"], download["payload"], token=token)
        )
        self.assertFalse(self.store.complete(task["id"], token=token))
        self.assertIsNone(self.store.claim(kinds=["download", "scan"]))
        upload = self.store.claim(kinds=["upload"])
        self.assertEqual(upload["id"], task["id"])
        self.assertEqual(upload["attempt"], 1)
        self.assertEqual(upload["path"], "/tmp/file")
        self.assertEqual(upload["md5"], "a" * 32)
        self.assertEqual(upload["result"], {"download_done": True})
        self.assertTrue(self.store.complete(task["id"], token=upload["claim_token"]))

    def test_equal_priority_groups_rotate_without_starvation(self):
        for i in range(3):
            self.store.enqueue(
                {"group_id": "large", "kind": "download"}, "large-%d" % i
            )
        small = self.store.enqueue({"group_id": "small", "kind": "download"}, "small")
        self.assertEqual(self.store.claim()["group_id"], "large")
        self.assertEqual(self.store.claim()["id"], small["id"])
        urgent = self.store.enqueue({"group_id": "large"}, "urgent", priority=10)
        self.assertEqual(self.store.claim()["id"], urgent["id"])

    def test_pagination_search_stats_and_bulk(self):
        for key in ("a", "b", "c"):
            self.enqueue(key)
        self.store.enqueue({"group_id": "other", "file_name": "literal_100%"}, "d")
        self.assertEqual(len(self.store.list_tasks(limit=2, offset=2)), 2)
        self.assertEqual(self.store.stats(42)["queued"], 3)
        self.assertEqual(self.store.count_tasks(search="literal_100%"), 1)
        self.assertEqual(self.store.count_tasks(search="literalX100"), 0)
        self.assertEqual(self.store.pause_all(), 4)
        self.assertEqual(self.store.resume_all(), 4)

    def test_verified_file_index_size_and_hash(self):
        self.store.upsert_file("first", 100, 123, "A" * 32, "unique")
        self.store.upsert_file("second", 100, 456, "b" * 32)
        self.assertEqual(len(self.store.find_files(100)), 2)
        self.assertEqual(len(self.store.find_files(100, md5="a" * 32)), 1)
        self.assertEqual(self.store.find_files(101), [])
        self.assertEqual(self.store.get_file("first")["mtime_ns"], 123)
        self.store.upsert_file("first", 200, 789, "c" * 32)
        self.assertEqual(len(self.store.find_files(100)), 1)
        with self.assertRaises(ValueError):
            self.store.upsert_file("bad", 2, 1, "bad")


if __name__ == "__main__":
    unittest.main()
