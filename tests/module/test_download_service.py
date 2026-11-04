"""Download pipeline integration with real SQLite and local content."""

import asyncio
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from module.app import TaskNode
from module.cloud_drive import CloudDriveConfig
from module.download_service import DownloadService
from module.task_store import TaskStore
from module.transfer import CHUNK_SIZE, file_md5


class FakeClient:
    def __init__(self, content):
        self.content = content
        self.stream_calls = 0
        self.message_calls = 0
        self.fail_first = False
        self.message = SimpleNamespace(
            id=17,
            empty=False,
            text=None,
            caption=None,
            caption_entities=None,
            chat=SimpleNamespace(id=-100, title="source"),
            video=SimpleNamespace(
                file_size=len(content), file_unique_id="unique-video"
            ),
        )

    async def get_messages(self, *args):
        self.message_calls += 1
        return self.message

    async def stream_media(self, message, offset=0):
        self.stream_calls += 1
        if self.fail_first and self.stream_calls == 1:
            error = type("FileReferenceExpired", (Exception,), {})
            raise error("expired")
        for start in range(offset * CHUNK_SIZE, len(self.content), CHUNK_SIZE):
            yield self.content[start : start + CHUNK_SIZE]


class DownloadServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.target = self.root / "media" / "video.mp4"
        self.target.parent.mkdir()
        self.client = FakeClient(b"new!")
        self.store = TaskStore(self.root / "tasks.db")
        self.app = SimpleNamespace(
            task_store=self.store,
            save_path=str(self.target.parent),
            media_directories=[],
            temp_save_path=str(self.root / "temp"),
            max_download_task=1,
            max_upload_task=1,
            retry_base_delay=0,
            max_retries=3,
            request_timeout=1,
            idle_timeout=1,
            media_types=["video"],
            file_formats={"video": ["mp4"]},
            enable_download_txt=False,
            chat_download_config={},
            after_upload_telegram_delete=False,
            cloud_drive_config=CloudDriveConfig(
                enable_upload_file=False, after_upload_file_delete=False
            ),
            set_download_id=Mock(),
        )
        self.metadata = AsyncMock(return_value=(str(self.target), "ignored", "mp4"))
        self.filter = Mock(return_value=True)
        self.node = TaskNode(chat_id=-100, task_id=91)
        self.node.is_running = True
        self.service = self.make_service()
        self.report_patch = patch(
            "module.pyrogram_extension.report_bot_download_status", new=AsyncMock()
        )
        self.report_patch.start()

    def make_service(self):
        return DownloadService(
            self.app, self.client, self.metadata, AsyncMock(), self.filter, AsyncMock()
        )

    async def asyncTearDown(self):
        self.report_patch.stop()
        self.store.close()
        self.temp.cleanup()

    async def execute_download(self):
        task = self.service.enqueue(self.client.message, self.node)
        claimed = self.store.claim(kinds=["download"])
        await self.service.queue.execute(claimed)
        return self.store.get(task["id"])

    async def test_caption_is_queue_display_name_when_media_name_is_missing(self):
        self.client.message.caption = "一段能说明内容的消息简介"
        row = self.service.enqueue(self.client.message, self.node)
        self.assertEqual(row["payload"]["display_name"], "一段能说明内容的消息简介")

    async def test_caption_replaces_generated_filename_in_queue_display(self):
        self.client.message.caption = "一段能说明内容的消息简介"
        self.client.message.video.file_name = "20261023_094616_85c652.mp4"
        row = self.service.enqueue(self.client.message, self.node)
        self.assertEqual(row["payload"]["display_name"], "一段能说明内容的消息简介.mp4")

    async def test_download_upload_handoff_survives_process_restart(self):
        self.app.cloud_drive_config.enable_upload_file = True
        task = await self.execute_download()
        self.assertEqual((task["state"], task["payload"]["kind"]), ("queued", "upload"))
        self.assertEqual(Path(task["path"]).read_bytes(), b"new!")
        self.store.close()
        self.store = TaskStore(self.root / "tasks.db")
        self.app.task_store = self.store
        self.service = self.make_service()

        async def upload(config, *args):
            config.last_upload_verified = True
            return True

        with patch("module.cloud_drive.CloudDrive.rclone_upload_file", new=upload):
            await self.service.queue.execute(self.store.claim(kinds=["upload"]))
        completed = self.store.get(task["id"])
        self.assertEqual(completed["state"], "completed")
        self.assertTrue(completed["result"]["cloud_verified"])
        self.assertEqual(self.client.stream_calls, 1)
        self.assertEqual(Path(completed["path"]).read_bytes(), b"new!")

    async def test_same_size_different_content_keeps_both_files(self):
        self.target.write_bytes(b"old!")
        self.service.index.scan()
        completed = await self.execute_download()
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(self.target.read_bytes(), b"old!")
        self.assertEqual(Path(completed["path"]).read_bytes(), b"new!")
        self.assertNotEqual(completed["path"], str(self.target))

    async def test_verified_telegram_identity_reuses_existing_file(self):
        self.target.write_bytes(b"new!")
        self.service.index.record(str(self.target), "unique-video")
        completed = await self.execute_download()
        self.assertEqual(completed["state"], "completed")
        self.assertTrue(completed["result"]["skipped"])
        self.assertFalse(completed["result"]["owned"])
        self.assertEqual(self.client.stream_calls, 0)

    async def test_expired_reference_refetches_message_on_retry(self):
        self.client.fail_first = True
        waiting = await self.execute_download()
        self.assertEqual(waiting["state"], "retry_wait")
        self.assertEqual(waiting["error_kind"], "file_reference")
        await self.service.queue.execute(self.store.claim(kinds=["download"]))
        self.assertEqual(self.store.get(waiting["id"])["state"], "completed")
        self.assertEqual(self.client.message_calls, 2)
        self.assertEqual(self.client.stream_calls, 2)

    async def test_format_filter_skips_stream(self):
        self.filter.return_value = False
        completed = await self.execute_download()
        self.assertTrue(completed["result"]["skipped"])
        self.assertEqual(completed["result"]["reason"], "file format filter")
        self.assertEqual(self.client.stream_calls, 0)

    async def test_preexisting_unindexed_identical_file_is_never_owned(self):
        # A user can add a file after startup's index scan has finished.
        self.target.write_bytes(b"new!")
        completed = await self.execute_download()
        self.assertEqual(completed["state"], "completed")
        self.assertFalse(completed["result"]["owned"])
        self.assertEqual(self.target.read_bytes(), b"new!")

    async def test_shared_file_remains_until_other_upload_finishes(self):
        self.app.cloud_drive_config.enable_upload_file = True
        self.app.cloud_drive_config.after_upload_file_delete = True
        first = await self.execute_download()
        second_node = TaskNode(chat_id=-100, task_id=92)
        second = self.service.enqueue(self.client.message, second_node)
        await self.service.queue.execute(self.store.claim(kinds=["download"]))
        self.assertFalse(self.store.get(second["id"])["result"]["owned"])

        async def upload(config, *args):
            config.last_upload_verified = True
            return True

        with patch("module.cloud_drive.CloudDrive.rclone_upload_file", new=upload):
            await self.service.queue.execute(self.store.claim(kinds=["upload"]))
            self.assertEqual(self.store.get(first["id"])["state"], "completed")
            self.assertTrue(
                Path(first["path"]).exists(),
                "Another queued upload still needs the same file",
            )
            await self.service.queue.execute(self.store.claim(kinds=["upload"]))
        self.assertEqual(self.store.get(second["id"])["state"], "completed")

    async def test_cancelling_background_waits_for_file_operation_to_exit(self):
        entered, release, finished = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )

        def blocking_file_operation():
            entered.set()
            release.wait(3)
            finished.set()

        operation = asyncio.create_task(
            self.service.background(blocking_file_operation)
        )
        while not entered.is_set():
            await asyncio.sleep(0.001)
        operation.cancel()
        try:
            await asyncio.sleep(0.01)
            self.assertFalse(
                operation.done(),
                "Cancelled claim must still own its file operation until the thread exits",
            )
        finally:
            release.set()
            await asyncio.gather(operation, return_exceptions=True)
            while not finished.is_set():
                await asyncio.sleep(0.001)

    async def test_terminal_notification_is_not_cancelled_after_db_completion(self):
        notified = asyncio.Event()

        async def slow_notification(*args):
            await asyncio.sleep(0.35)
            notified.set()

        row = self.service.enqueue(self.client.message, self.node)
        with patch(
            "module.pyrogram_extension.report_bot_download_status",
            new=slow_notification,
        ):
            self.service.queue.start()
            try:
                await asyncio.wait_for(notified.wait(), timeout=2)
                self.assertEqual(self.store.get(row["id"])["state"], "completed")
            finally:
                await self.service.queue.stop()

    async def test_verified_checkpoint_can_handoff_when_source_message_was_deleted(
        self,
    ):
        self.app.cloud_drive_config.enable_upload_file = True
        self.target.write_bytes(b"new!")
        row = self.service.enqueue(self.client.message, self.node)
        claim = self.store.claim(kinds=["download"])
        digest = file_md5(str(self.target))
        self.store.checkpoint(
            row["id"],
            "verified",
            path=str(self.target),
            md5=digest,
            result={
                "path": str(self.target),
                "md5": digest,
                "owned": True,
                "skipped": False,
            },
            token=claim["claim_token"],
        )
        self.store.close()
        self.store = TaskStore(self.root / "tasks.db")
        self.app.task_store = self.store
        self.service = self.make_service()
        self.client.get_messages = AsyncMock(return_value=SimpleNamespace(empty=True))
        await self.service.queue.execute(self.store.claim(kinds=["download"]))
        recovered = self.store.get(row["id"])
        self.assertEqual(
            (recovered["state"], recovered["payload"]["kind"]), ("queued", "upload")
        )
        self.assertEqual(self.target.read_bytes(), b"new!")

    async def test_audio_upload_sends_message_before_source_can_be_deleted(self):
        self.node.upload_telegram_chat_id = 555
        self.app.after_upload_telegram_delete = True
        self.app.hide_file_name = False
        self.app.media_types = ["audio"]
        self.client.message.video = None
        self.client.message.audio = SimpleNamespace(
            file_size=4,
            file_unique_id="audio-id",
            duration=1,
            title="song",
            performer="artist",
        )
        for kind in ("photo", "document", "voice", "video_note"):
            setattr(self.client.message, kind, None)
        self.client.send_audio = AsyncMock(return_value=SimpleNamespace(id=1234))
        task = await self.execute_download()
        with patch(
            "module.pyrogram_extension.process_caption", new=AsyncMock(return_value="")
        ):
            await self.service.queue.execute(self.store.claim(kinds=["upload"]))
        self.client.send_audio.assert_awaited_once()
        self.assertEqual(self.store.get(task["id"])["state"], "completed")


if __name__ == "__main__":
    unittest.main()
