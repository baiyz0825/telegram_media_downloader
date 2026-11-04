"""Content safety and real partial-file recovery without Telegram access."""

import asyncio
import errno
import os
import tempfile
import unittest
from pathlib import Path

from module.task_store import TaskStore
from module.transfer import (
    CHUNK_SIZE,
    FileIndex,
    IncompleteTransfer,
    TaskInterrupted,
    atomic_publish,
    classify_error,
    file_md5,
    stream_to_file,
)


class TransferTest(unittest.IsolatedAsyncioTestCase):
    async def test_resume_discards_torn_chunk_and_preserves_prefix(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "partial"
            path.write_bytes(b"a" * CHUNK_SIZE + b"torn")
            offsets = []

            class Client:
                async def stream_media(self, message, offset=0):
                    offsets.append(offset)
                    yield b"b" * CHUNK_SIZE
                    yield b"end"

            progress = []

            async def update(current, total):
                progress.append(current)

            async def control():
                pass

            await stream_to_file(
                Client(), None, str(path), 2 * CHUNK_SIZE + 3, update, control
            )
            self.assertEqual(offsets, [1])
            self.assertEqual(
                path.read_bytes(), b"a" * CHUNK_SIZE + b"b" * CHUNK_SIZE + b"end"
            )
            self.assertEqual(progress[-1], 2 * CHUNK_SIZE + 3)

    async def test_pause_keeps_partial_and_short_stream_is_retryable(self):
        with tempfile.TemporaryDirectory() as root:
            path = str(Path(root) / "partial")

            class Client:
                async def stream_media(self, message, offset=0):
                    yield b"a" * CHUNK_SIZE

            async def control():
                pass

            async def progress(current, total):
                raise TaskInterrupted()

            with self.assertRaises(TaskInterrupted):
                await stream_to_file(
                    Client(), None, path, CHUNK_SIZE * 2, progress, control
                )
            self.assertEqual(os.path.getsize(path), CHUNK_SIZE)

            async def progress(current, total):
                pass

            class EmptyClient:
                async def stream_media(self, message, offset=0):
                    if False:
                        yield b""

            with self.assertRaises(IncompleteTransfer):
                await stream_to_file(
                    EmptyClient(), None, path, CHUNK_SIZE * 2, progress, control
                )

    async def test_stalled_stream_times_out(self):
        class Client:
            async def stream_media(self, message, offset=0):
                await asyncio.sleep(10)
                yield b"x"

        async def callback(*args):
            pass

        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(asyncio.TimeoutError):
                await stream_to_file(
                    Client(),
                    None,
                    str(Path(root) / "part"),
                    1,
                    callback,
                    callback,
                    idle_timeout=0.01,
                )

    def test_content_index_never_skips_merely_same_size_or_changed_content(self):
        with tempfile.TemporaryDirectory() as root:
            store = TaskStore(":memory:")
            self.addCleanup(store.close)
            original = Path(root) / "original.bin"
            other = Path(root) / "same-size.bin"
            original.write_bytes(b"abcd")
            other.write_bytes(b"efgh")
            index = FileIndex(store, [root])
            digest = index.record(str(original), "telegram-id")
            index.scan()
            self.assertIsNone(index.find(4))
            self.assertEqual(index.find(4, digest), str(original))
            timestamp = original.stat().st_mtime_ns
            original.write_bytes(b"xxxx")
            os.utime(original, ns=(timestamp, timestamp))
            self.assertIsNone(index.find(4, unique_id="telegram-id"))
            self.assertEqual(other.read_bytes(), b"efgh")

    def test_atomic_publish_preserves_conflicting_destination(self):
        with tempfile.TemporaryDirectory() as root:
            source, target = Path(root) / "source", Path(root) / "target.mp4"
            source.write_bytes(b"new")
            target.write_bytes(b"old")
            published = atomic_publish(str(source), str(target))
            self.assertEqual(target.read_bytes(), b"old")
            self.assertEqual(Path(published).read_bytes(), b"new")
            self.assertFalse(source.exists())

    def test_error_policy(self):
        self.assertTrue(classify_error(ConnectionResetError()).retry)
        self.assertTrue(classify_error(asyncio.TimeoutError()).retry)
        self.assertEqual(classify_error(OSError(errno.ENOSPC, "full")).kind, "storage")
        self.assertFalse(classify_error(TypeError("bug")).retry)

        class FloodWait(Exception):
            value = 37

        decision = classify_error(FloodWait())
        self.assertEqual((decision.kind, decision.delay), ("rate_limit", 37))
