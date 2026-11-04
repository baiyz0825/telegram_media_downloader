"""Cloud uploads require process success and remote verification before deletion."""

import asyncio
import hashlib
import json
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch
from zipfile import ZipFile

from module.cloud_drive import CloudDrive, CloudDriveConfig


class CloudDriveTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.temp.name, 'file "$(touch injected)".txt')
        with open(self.path, "wb") as stream:
            stream.write(b"verified payload")
        self.digest = hashlib.md5(b"verified payload").hexdigest()
        self.config = CloudDriveConfig(
            enable_upload_file=True, remote_dir="remote:media"
        )

    def tearDown(self):
        self.temp.cleanup()

    def metadata(self, md5=None, size=16):
        return (
            0,
            json.dumps({"Size": size, "Hashes": {"MD5": md5} if md5 else {}}).encode(),
        )

    async def upload(self, results):
        with patch.object(
            CloudDrive, "_run_rclone", AsyncMock(side_effect=results)
        ) as runner:
            result = await CloudDrive.rclone_upload_file(
                self.config, self.temp.name, self.path
            )
        return result, runner

    async def test_success_requires_exit_and_remote_hash(self):
        success, runner = await self.upload(
            [(0, b""), (0, b"100%"), self.metadata(self.digest)]
        )
        self.assertTrue(success)
        self.assertFalse(os.path.exists(self.path))
        self.assertEqual(runner.call_args_list[1].args[1], "copyto")
        self.assertEqual(runner.call_args_list[1].args[2], self.path)
        self.assertEqual(self.config.total_upload_success_file_count, 1)
        self.assertTrue(self.config.last_upload_verified)

    async def test_progress_text_cannot_delete_on_nonzero_exit(self):
        success, _ = await self.upload([(0, b""), (1, b"Transferred: 100%, 1 / 1")])
        self.assertFalse(success)
        self.assertTrue(os.path.exists(self.path))

    async def test_verification_flag_resets_for_next_upload(self):
        self.config.last_upload_verified = True
        success, _ = await self.upload([(1, b"mkdir failed")])
        self.assertFalse(success)
        self.assertFalse(self.config.last_upload_verified)

    async def test_size_mismatch_retains_source(self):
        success, _ = await self.upload(
            [(0, b""), (0, b""), self.metadata(self.digest, 1)]
        )
        self.assertFalse(success)
        self.assertTrue(os.path.exists(self.path))

    async def test_md5_mismatch_retains_source(self):
        success, _ = await self.upload([(0, b""), (0, b""), self.metadata("0" * 32)])
        self.assertFalse(success)
        self.assertTrue(os.path.exists(self.path))

    async def test_no_remote_hash_retains_source(self):
        success, _ = await self.upload([(0, b""), (0, b""), self.metadata()])
        self.assertTrue(success)
        self.assertTrue(os.path.exists(self.path))
        self.assertFalse(self.config.last_upload_verified)

    async def test_remote_inspection_failure_retains_source(self):
        success, _ = await self.upload([(0, b""), (0, b""), (1, b"not found")])
        self.assertFalse(success)
        self.assertTrue(os.path.exists(self.path))

    async def test_exec_uses_literal_args_and_reaps_cancelled_child(self):
        started = asyncio.Event()

        class BlockingOutput:
            async def __aiter__(self):
                started.set()
                await asyncio.Event().wait()
                yield b""

        proc = Mock(returncode=None, stdout=BlockingOutput())
        proc.wait = AsyncMock(return_value=0)
        with patch(
            "module.cloud_drive.asyncio.create_subprocess_exec",
            AsyncMock(return_value=proc),
        ) as create:
            task = asyncio.create_task(
                CloudDrive._run_rclone(self.config, "copyto", self.path, "remote:file")
            )
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(
            create.call_args.args,
            (self.config.rclone_path, "copyto", self.path, "remote:file"),
        )
        proc.terminate.assert_called_once()
        proc.wait.assert_awaited()
        self.assertTrue(os.path.exists(self.path))

    def test_zip_uses_unique_file_and_relative_member(self):
        archive = CloudDrive.zip_file(self.path)
        with ZipFile(archive) as reader:
            self.assertEqual(reader.namelist(), [os.path.basename(self.path)])
            self.assertEqual(reader.read(reader.namelist()[0]), b"verified payload")


if __name__ == "__main__":
    unittest.main()
