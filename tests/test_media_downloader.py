"""Unittest module for media downloader."""
import asyncio
import os
import platform
import queue
import sys
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime
from typing import List, Union

import mock
import pyrogram

from media_downloader import (
    _can_download,
    _check_config,
    _get_media_meta,
    _is_exist,
    add_download_task,
    app,
    download_all_chat,
    download_media,
    download_task,
    main,
    save_msg_to_file,
    worker,
)
from module.app import Application, DownloadStatus, TaskNode
from module.cloud_drive import CloudDriveConfig
from module.pyrogram_extension import (
    get_extension,
    record_download_status,
    reset_download_cache,
)

from .test_common import (
    Chat,
    Date,
    MockAudio,
    MockDocument,
    MockMessage,
    MockPhoto,
    MockVideo,
    MockVideoNote,
    MockVoice,
    get_extension,
    platform_generic_path,
)

MOCK_DIR: str = "/root/project"
TEST_DIRECTORY = tempfile.TemporaryDirectory()
if platform.system() == "Windows":
    MOCK_DIR = "\\root\\project"
MOCK_CONF = {
    "api_id": 123,
    "api_hash": "hasw5Tgawsuj67",
    "chat": [{"chat_id": 8654123, "last_read_message_id": 0, "ids_to_retry": [1, 2]}],
    "media_types": ["audio", "voice", "document", "photo", "video", "video_note"],
    "file_formats": {"audio": ["all"], "voice": ["all"], "video": ["all"]},
    "save_path": MOCK_DIR,
    "file_name_prefix": ["message_id", "caption", "file_name"],
}

event_str = "asyncio.AbstractEventLoop.run_forever"
if sys.version_info > (3, 8):
    event_str = "asyncio.ProactorEventLoop.run_forever"


def os_remove(_: str):
    pass


def is_exist(file: str):
    if os.path.basename(file).find("313 - sucess_exist_down.mp4") != -1:
        return True
    elif os.path.basename(file).find("422 - exception.mov") != -1:
        raise Exception
    return False


def os_get_file_size(file: str) -> int:
    if os.path.basename(file).find("311 - failed_down.mp4") != -1:
        return 0
    elif os.path.basename(file).find("312 - sucess_down.mp4") != -1:
        return 1024
    elif os.path.basename(file).find("313 - sucess_exist_down.mp4") != -1:
        return 1024
    return 0


def rest_app(conf: dict):
    config_test = os.path.join(TEST_DIRECTORY.name, "config_test.yaml")
    data_test = os.path.join(TEST_DIRECTORY.name, "data_test.yaml")
    if os.path.exists(config_test):
        os.remove(config_test)
    if os.path.exists(data_test):
        os.remove(data_test)
    app.total_download_task = 0
    app.is_running = True
    app.chat_download_config: dict = {}
    # app.already_download_ids_set = set()
    app.save_path = os.path.abspath(".")
    app.api_id: str = ""
    app.api_hash: str = ""
    app.media_types: List[str] = []
    app.file_formats: dict = {}
    app.proxy: dict = {}
    app.restart_program = False
    app.config: dict = {}
    app.app_data: dict = {}
    app.file_path_prefix: List[str] = ["chat_title", "media_datetime"]
    app.file_name_prefix: List[str] = ["message_id", "file_name"]
    app.file_name_prefix_split: str = " - "
    app.log_file_path = os.path.join(os.path.abspath("."), "log")
    app.cloud_drive_config = CloudDriveConfig()
    app.hide_file_name = False
    app.caption_name_dict: dict = {}
    app.max_concurrent_transmissions: int = 1
    app.web_host: str = "localhost"
    app.web_port: int = 5000
    app.config_file = config_test
    app.app_data_file = data_test
    app.task_store = None
    app.download_service = None
    app.config = conf
    app.assign_config(conf)
    app.assign_app_data(conf)


def mock_manage_duplicate_file(file_path: str) -> str:
    return file_path


def raise_keyboard_interrupt():
    raise KeyboardInterrupt


async def new_upload_telegram_chat(
    client: pyrogram.Client,
    upload_user: pyrogram.Client,
    app: Application,
    node: TaskNode,
    message: pyrogram.types.Message,
    download_status: DownloadStatus,
    file_name: str = None,
):
    pass


def raise_exception():
    raise Exception


def load_config():
    raise ValueError("error load config")


class MyQueue:
    def __init__(self, queue_list_obj):
        self._queue = queue.Queue()
        for item in queue_list_obj:
            self._queue.put(item)

    async def get(self):
        if self._queue.empty():
            raise Exception
        return self._queue.get()


class MockEventLoop:
    def __init__(self):
        pass

    def run_until_complete(self, *args, **kwargs):
        return {"api_id": 1, "api_hash": "asdf", "ids_to_retry": [1, 2, 3]}


class MockAsync:
    def __init__(self):
        pass

    def get_event_loop(self):
        return MockEventLoop()


async def async_get_media_meta(chat_id, message, message_media, _type):
    result = await _get_media_meta(chat_id, message, message_media, _type)
    return result


async def async_download_media(
    client, message, media_types, file_formats, chat_id=-123
):
    node = TaskNode(chat_id=chat_id)
    return await download_media(client, message, media_types, file_formats, node)


def mock_move_to_download_path(temp_download_path: str, download_path: str):
    pass


def mock_check_download_finish(media_size: int, download_path: str, ui_file_name: str):
    pass


async def new_fetch_message(client: pyrogram.Client, message: pyrogram.types.Message):
    return message


async def get_chat_history(client, *args, **kwargs):
    items = [
        MockMessage(
            id=1213,
            media=True,
            voice=MockVoice(
                mime_type="audio/ogg",
                date=datetime(2019, 7, 25, 14, 53, 50),
            ),
        ),
        MockMessage(
            id=1214,
            media=False,
            text="test message 1",
        ),
        MockMessage(
            id=1215,
            media=False,
            text="test message 2",
        ),
        MockMessage(
            id=1216,
            media=False,
            text="test message 3",
        ),
    ]
    for item in items:
        yield item


class MockClient:
    def __init__(self, *args, **kwargs):
        pass

    def __aiter__(self):
        return self

    async def start(self):
        pass

    async def stop(self):
        pass

    async def get_messages(self, *args, **kwargs):
        if kwargs["message_ids"] == 7:
            return MockMessage(
                id=7,
                media=True,
                chat_id=123456,
                chat_title="123456",
                date=datetime.now(),
                video=MockVideo(
                    file_name="sample_video.mov",
                    mime_type="video/mov",
                ),
            )
        elif kwargs["message_ids"] == 8:
            return MockMessage(
                id=8,
                media=True,
                chat_id=234567,
                chat_title="234567",
                date=datetime.now(),
                video=MockVideo(
                    file_name="sample_video.mov",
                    mime_type="video/mov",
                ),
            )
        elif kwargs["message_ids"] == [1, 2]:
            return [
                MockMessage(
                    id=1,
                    media=True,
                    chat_id=234568,
                    chat_title="234568",
                    date=datetime.now(),
                    video=MockVideo(
                        file_name="sample_video.mov",
                        mime_type="video/mov",
                    ),
                ),
                MockMessage(
                    id=2,
                    media=True,
                    chat_id=234568,
                    chat_title="234568",
                    date=datetime.now(),
                    video=MockVideo(
                        file_name="sample_video2.mov",
                        mime_type="video/mov",
                    ),
                ),
            ]
        elif kwargs["message_ids"] == 313:
            return MockMessage(
                id=313,
                media=True,
                video=MockVideo(
                    file_name="sucess_exist_down.mp4",
                    mime_type="video/mp4",
                    file_size=1024,
                ),
            )
        elif kwargs["message_ids"] == 312:
            return MockMessage(
                id=312,
                media=True,
                video=MockVideo(
                    file_name="sucess_down.mp4",
                    mime_type="video/mp4",
                    file_size=1024,
                ),
            )
        elif kwargs["message_ids"] == 311:
            return MockMessage(
                id=311,
                media=True,
                video=MockVideo(
                    file_name="failed_down.mp4",
                    mime_type="video/mp4",
                    file_size=1024,
                ),
            )
        return []

    async def download_media(self, *args, **kwargs):
        mock_message = args[0]
        if mock_message.id in [7, 8]:
            raise pyrogram.errors.exceptions.bad_request_400.BadRequest
        elif mock_message.id == 9:
            raise pyrogram.errors.exceptions.unauthorized_401.Unauthorized
        elif mock_message.id == 11:
            raise TypeError
        elif mock_message.id == 420:
            raise pyrogram.errors.exceptions.flood_420.FloodWait(value=420)
        elif mock_message.id == 421:
            raise Exception
        return kwargs["file_name"]

    async def edit_message_text(self, *args, **kwargs):
        return True


def check_for_updates(_: dict = None):
    pass


@mock.patch("media_downloader.get_extension", new=get_extension)
@mock.patch("module.pyrogram_extension.get_extension", new=get_extension)
@mock.patch("media_downloader.fetch_message", new=new_fetch_message)
@mock.patch("media_downloader.get_chat_history_v2", new=get_chat_history)
@mock.patch("media_downloader.RETRY_TIME_OUT", new=0)
class MediaDownloaderTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.new_event_loop()
        app.loop = cls.loop
        rest_app(MOCK_CONF)

    def test_generated_filename_uses_caption_with_default_prefix(self):
        rest_app(MOCK_CONF)
        previous_prefix = app.file_name_prefix
        try:
            app.file_name_prefix = ["message_id", "file_name"]
            message = MockMessage(
                id=21,
                media=True,
                chat_title="test-caption",
                caption="A detailed caption",
                video=MockVideo(
                    file_name="20261023_094616_85c652.mp4",
                    mime_type="video/mp4",
                ),
            )
            target, _, _ = self.loop.run_until_complete(
                async_get_media_meta(-123, message, message.video, "video")
            )
            self.assertEqual(os.path.basename(target), "21 - A detailed caption.mp4")
        finally:
            app.file_name_prefix = previous_prefix

    # @mock.patch("media_downloader.app.save_path", new=MOCK_DIR)
    def test_get_media_meta(self):
        rest_app(MOCK_CONF)
        app.save_path = MOCK_DIR
        # Test Voice notes
        message = MockMessage(
            id=1,
            media=True,
            chat_title="test1",
            date=datetime(2019, 7, 25, 14, 53, 50),
            voice=MockVoice(
                mime_type="audio/ogg",
                date=datetime(2019, 7, 25, 14, 53, 50),
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.voice, "voice")
        )

        self.assertEqual(
            (
                platform_generic_path(
                    "/root/project/test1/2019_07/1 - voice_2019-07-25T14_53_50.ogg"
                ),
                platform_generic_path(
                    os.path.join(
                        app.temp_save_path, "test1/1 - voice_2019-07-25T14_53_50.ogg"
                    )
                ),
                "ogg",
            ),
            result,
        )

        # Test photos
        message = MockMessage(
            id=2,
            media=True,
            date=datetime(2019, 8, 5, 14, 35, 12),
            chat_title="test2",
            photo=MockPhoto(
                date=datetime(2019, 8, 5, 14, 35, 12), file_unique_id="ADAVKJYIFV"
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.photo, "photo")
        )
        self.assertEqual(
            (
                platform_generic_path("/root/project/test2/2019_08/2 - ADAVKJYIFV.jpg"),
                platform_generic_path(
                    os.path.join(app.temp_save_path, "test2/2 - ADAVKJYIFV.jpg")
                ),
                None,
            ),
            result,
        )

        message = MockMessage(
            id=2,
            media=True,
            date=datetime(2019, 8, 5, 14, 35, 12),
            chat_title="test2",
            media_group_id="AAA213213",
            caption="#home #book",
            photo=MockPhoto(
                date=datetime(2019, 8, 5, 14, 35, 12), file_unique_id="ADAVKJYIFV"
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.photo, "photo")
        )
        self.assertEqual(
            (
                platform_generic_path(
                    "/root/project/test2/2019_08/2 - #home #book - ADAVKJYIFV.jpg"
                ),
                platform_generic_path(
                    os.path.join(
                        app.temp_save_path, "test2/2 - #home #book - ADAVKJYIFV.jpg"
                    )
                ),
                None,
            ),
            result,
        )

        # Test Documents
        message = MockMessage(
            id=3,
            media=True,
            chat_title="test2",
            document=MockDocument(
                file_name="sample_document.pdf",
                mime_type="application/pdf",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.document, "document")
        )
        self.assertEqual(
            (
                platform_generic_path("/root/project/test2/0/3 - sample_document.pdf"),
                platform_generic_path(
                    os.path.join(app.temp_save_path, "test2/3 - sample_document.pdf")
                ),
                "pdf",
            ),
            result,
        )

        before_file_name_prefix_split = app.file_name_prefix_split
        app.file_name_prefix_split = "-"

        message = MockMessage(
            id=3,
            media=True,
            chat_title="test2",
            media_group_id="BBB213213",
            caption="#work",
            document=MockDocument(
                file_name="sample_document.pdf",
                mime_type="application/pdf",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.document, "document")
        )
        self.assertEqual(
            (
                platform_generic_path(
                    "/root/project/test2/0/3-#work-sample_document.pdf"
                ),
                platform_generic_path(
                    os.path.join(
                        app.temp_save_path, "test2/3-#work-sample_document.pdf"
                    )
                ),
                "pdf",
            ),
            result,
        )

        app.file_name_prefix_split = before_file_name_prefix_split
        # Test audio
        message = MockMessage(
            id=4,
            media=True,
            date=datetime(2021, 8, 5, 14, 35, 12),
            chat_title="test2",
            audio=MockAudio(
                file_name="sample_audio.mp3",
                mime_type="audio/mp3",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.audio, "audio")
        )
        self.assertEqual(
            (
                platform_generic_path(
                    "/root/project/test2/2021_08/4 - sample_audio.mp3"
                ),
                platform_generic_path(
                    os.path.join(app.temp_save_path, "test2/4 - sample_audio.mp3")
                ),
                "mp3",
            ),
            result,
        )

        # Test Video 1
        message = MockMessage(
            id=5,
            media=True,
            date=datetime(2022, 8, 5, 14, 35, 12),
            chat_title="test2",
            video=MockVideo(
                mime_type="video/mp4",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.video, "video")
        )
        self.assertEqual(
            (
                platform_generic_path("/root/project/test2/2022_08/5.mp4"),
                platform_generic_path(os.path.join(app.temp_save_path, "test2/5.mp4")),
                "mp4",
            ),
            result,
        )

        # Test Video 2
        message = MockMessage(
            id=5,
            media=True,
            date=datetime(2022, 8, 5, 14, 35, 12),
            chat_title="test2",
            video=MockVideo(
                file_name="test.mp4",
                mime_type="video/mp4",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.video, "video")
        )
        self.assertEqual(
            (
                platform_generic_path("/root/project/test2/2022_08/5 - test.mp4"),
                platform_generic_path(
                    os.path.join(app.temp_save_path, "test2/5 - test.mp4")
                ),
                "mp4",
            ),
            result,
        )

        # Test Video 3: not exist chat_title
        message = MockMessage(
            id=5,
            media=True,
            dis_chat=True,
            date=datetime(2022, 8, 5, 14, 35, 12),
            video=MockVideo(
                file_name="test.mp4",
                mime_type="video/mp4",
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.video, "video")
        )

        self.assertEqual(
            (
                platform_generic_path("/root/project/-123/2022_08/5 - test.mp4"),
                platform_generic_path(
                    os.path.join(app.temp_save_path, "-123/5 - test.mp4")
                ),
                "mp4",
            ),
            result,
        )

        # Test VideoNote
        message = MockMessage(
            id=6,
            media=True,
            date=datetime(2019, 7, 25, 14, 53, 50),
            chat_title="test2",
            video_note=MockVideoNote(
                mime_type="video/mp4",
                date=datetime(2019, 7, 25, 14, 53, 50),
            ),
        )
        result = self.loop.run_until_complete(
            async_get_media_meta(-123, message, message.video_note, "video_note")
        )
        self.assertEqual(
            (
                platform_generic_path(
                    "/root/project/test2/2019_07/6 - video_note_2019-07-25T14_53_50.mp4"
                ),
                platform_generic_path(
                    os.path.join(
                        app.temp_save_path,
                        "test2/6 - video_note_2019-07-25T14_53_50.mp4",
                    )
                ),
                "mp4",
            ),
            result,
        )

    @mock.patch("media_downloader.app.save_path", new=MOCK_DIR)
    @mock.patch("media_downloader.asyncio.sleep", return_value=None)
    @mock.patch("media_downloader.logger")
    @mock.patch("media_downloader._is_exist", new=is_exist)
    @mock.patch(
        "media_downloader._move_to_download_path", new=mock_move_to_download_path
    )
    @mock.patch(
        "media_downloader._check_download_finish", new=mock_check_download_finish
    )
    def test_download_media(self, mock_logger, patch_sleep):
        reset_download_cache()
        rest_app(MOCK_CONF)
        client = MockClient()
        app.hide_file_name = True
        message = MockMessage(
            id=5,
            media=True,
            video=MockVideo(
                file_name="sample_video.mp4",
                mime_type="video/mp4",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["mp4"]}, -123
            )
        )
        self.assertEqual(
            (
                DownloadStatus.SuccessDownload,
                platform_generic_path("/root/project/-123/0/5 - sample_video.mp4"),
            ),
            result,
        )

        message = MockMessage(
            id=6,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual(
            (
                DownloadStatus.SuccessDownload,
                platform_generic_path("/root/project/-123/0/6 - sample_video.mov"),
            ),
            result,
        )

        # Test re-fetch message success
        message = MockMessage(
            id=7,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)
        mock_logger.warning.assert_called_with(
            "Message[7]: file reference expired, refetching..."
        )

        # Test re-fetch message failure
        message = MockMessage(
            id=8,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)
        mock_logger.error.assert_called_with(
            "Message[8]: file reference expired for 3 retries, download skipped."
        )

        # Test other exception
        message = MockMessage(
            id=9,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)

        # Check no media
        message = MockMessage(
            id=10,
            media=None,
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.SkipDownload, None), result)

        # Test timeout
        message = MockMessage(
            id=11,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)
        mock_logger.error.assert_called_with(
            "Message[11]: Timing out after 3 reties, download skipped."
        )

        # Test file name with out suffix
        message = MockMessage(
            id=12,
            media=True,
            video=MockVideo(
                file_name="sample_video",
                mime_type="video/mp4",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual(
            (
                DownloadStatus.SuccessDownload,
                platform_generic_path("/root/project/-123/0/12 - sample_video.mp4"),
            ),
            result,
        )

        # Test FloodWait 420
        message = MockMessage(
            id=420,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)
        mock_logger.warning.assert_called_with("Message[{}]: FlowWait {}", 420, 420)

        # Test other Exception
        message = MockMessage(
            id=421,
            media=True,
            video=MockVideo(
                file_name="sample_video.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)

        # Test other Exception
        message = MockMessage(
            id=422,
            media=True,
            video=MockVideo(
                file_name="422 - exception.mov",
                mime_type="video/mov",
            ),
        )
        result = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["all"]}
            )
        )
        self.assertEqual((DownloadStatus.FailedDownload, None), result)

    @mock.patch("media_downloader.HookClient", new=MockClient)
    @mock.patch("media_downloader.asyncio.Queue.put")
    def test_download_task(self, moc_put):
        rest_app(MOCK_CONF)
        client = MockClient()
        app.chat_download_config[8654123].download_filter = "id != 1213"
        self.loop.run_until_complete(download_all_chat(client))
        moc_put.assert_called()

    def test_durable_enqueue_does_not_duplicate_group_total(self):
        rest_app(MOCK_CONF)
        service = mock.Mock()
        service.enqueue.return_value = {"id": 1, "state": "queued"}
        message = MockMessage(id=25)
        node = TaskNode(chat_id=-123)
        with mock.patch.object(app, "download_service", service):
            self.assertTrue(
                self.loop.run_until_complete(add_download_task(message, node))
            )
            self.assertTrue(
                self.loop.run_until_complete(add_download_task(message, node))
            )
        self.assertEqual(node.total_task, 1)
        self.assertEqual(service.enqueue.call_count, 2)

    def test_existing_nonzero_wrong_size_is_downloaded_again(self):
        rest_app(MOCK_CONF)
        reset_download_cache()
        message = MockMessage(
            id=314,
            media=True,
            video=MockVideo(
                file_name="truncated.mp4", mime_type="video/mp4", file_size=1024
            ),
        )
        with mock.patch("media_downloader._is_exist", return_value=True), mock.patch(
            "media_downloader.os.path.getsize", return_value=512
        ), mock.patch("media_downloader._check_download_finish") as verify, mock.patch(
            "media_downloader._move_to_download_path"
        ), mock.patch(
            "media_downloader.asyncio.sleep", new=mock.AsyncMock()
        ):
            status, _ = self.loop.run_until_complete(
                async_download_media(
                    MockClient(), message, ["video"], {"video": ["mp4"]}
                )
            )
        self.assertEqual(status, DownloadStatus.SuccessDownload)
        verify.assert_called_once()

    def test_can_download(self):
        file_formats = {
            "audio": ["mp3"],
            "video": ["mp4"],
            "document": ["all"],
        }
        result = _can_download("audio", file_formats, "mp3")
        self.assertEqual(result, True)

        result1 = _can_download("audio", file_formats, "ogg")
        self.assertEqual(result1, False)

        result2 = _can_download("document", file_formats, "pdf")
        self.assertEqual(result2, True)

        result3 = _can_download("document", file_formats, "epub")
        self.assertEqual(result3, True)

    def test_is_exist(self):
        this_dir = os.path.dirname(os.path.abspath(__file__))
        result = _is_exist(os.path.join(this_dir, "__init__.py"))
        self.assertEqual(result, True)

        result1 = _is_exist(os.path.join(this_dir, "init.py"))
        self.assertEqual(result1, False)

        result2 = _is_exist(this_dir)
        self.assertEqual(result2, False)

    @mock.patch("media_downloader.os.makedirs")
    @mock.patch("builtins.open", new_callable=mock.mock_open)
    def test_save_msg_to_file(self, mock_open, mock_makedirs):
        rest_app(MOCK_CONF)
        app.enable_download_txt = True
        app.temp_save_path = "/tmp"
        app.date_format = "%Y_%m"

        message = MockMessage(
            id=123,
            dis_chat=True,
            chat=Chat(chat_id=456, chat_title="Test Chat"),
            date=datetime(2023, 5, 15, 10, 30, 0),
            text="This is a test message",
        )

        expected_file_path = platform_generic_path(
            "/root/project/Test Chat/2023_05/123.txt"
        )

        result = self.loop.run_until_complete(save_msg_to_file(app, 456, message))

        self.assertEqual(result, (DownloadStatus.SuccessDownload, expected_file_path))
        mock_makedirs.assert_called_once_with(
            os.path.dirname(expected_file_path), exist_ok=True
        )
        mock_open.assert_called_once_with(expected_file_path, "w", encoding="utf-8")
        mock_open().write.assert_called_once_with("This is a test message")

    @mock.patch("media_downloader.RETRY_TIME_OUT", new=0)
    @mock.patch("media_downloader.os.path.getsize", new=os_get_file_size)
    @mock.patch("media_downloader.os.remove", new=os_remove)
    @mock.patch("media_downloader._is_exist", new=is_exist)
    @mock.patch(
        "media_downloader._move_to_download_path", new=mock_move_to_download_path
    )
    def test_issues_311(self):
        # see https://github.com/Dineshkarthik/telegram_media_downloader/issues/311
        rest_app(MOCK_CONF)

        client = MockClient()
        # 1. test `TimeOutError`
        message = MockMessage(
            id=311,
            media=True,
            video=MockVideo(
                file_name="failed_down.mp4",
                mime_type="video/mp4",
                file_size=1024,
            ),
        )

        media_size = getattr(message.video, "file_size")
        self.assertEqual(media_size, 1024)

        res = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["mp4"]}
            )
        )
        self.assertEqual(res, (DownloadStatus.FailedDownload, None))

        # 2. test sucess download
        rest_app(MOCK_CONF)
        message = MockMessage(
            id=312,
            media=True,
            video=MockVideo(
                file_name="sucess_down.mp4",
                mime_type="video/mp4",
                file_size=1024,
            ),
        )

        res = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["mp4"]}
            )
        )

        self.assertEqual(
            res,
            (
                DownloadStatus.SuccessDownload,
                platform_generic_path("/root/project/-123/0/312 - sucess_down.mp4"),
            ),
        )

        rest_app(MOCK_CONF)
        # 3. test already download
        message = MockMessage(
            id=313,
            media=True,
            video=MockVideo(
                file_name="sucess_exist_down.mp4",
                mime_type="video/mp4",
                file_size=1024,
            ),
        )

        res = self.loop.run_until_complete(
            async_download_media(
                client, message, ["video", "photo"], {"video": ["mp4"]}
            )
        )

        self.assertEqual(res, (DownloadStatus.SkipDownload, None))

    def _main_lifecycle(self, startup_error=None, bot=False):
        rest_app(MOCK_CONF)
        app.bot_token = "test-token" if bot else ""
        events = []
        service = mock.Mock()
        service.queue.start.side_effect = lambda: events.append("workers_start")
        service.queue.stop = mock.AsyncMock(
            side_effect=lambda: events.append("workers_stop")
        )
        service.background = mock.AsyncMock(
            side_effect=lambda *_: events.append("index")
        )
        with ExitStack() as stack:
            for target, event in (
                ("start_server", "client_start"),
                ("stop_server", "client_stop"),
                ("start_download_bot", "bot_start"),
                ("stop_download_bot", "bot_stop"),
            ):
                stack.enter_context(
                    mock.patch(
                        "media_downloader." + target,
                        new=mock.AsyncMock(
                            side_effect=lambda *_, name=event: events.append(name)
                        ),
                    )
                )
            stack.enter_context(
                mock.patch("media_downloader.HookClient", new=MockClient)
            )
            stack.enter_context(mock.patch("media_downloader.TaskStore"))
            stack.enter_context(
                mock.patch("media_downloader.DownloadService", return_value=service)
            )
            stack.enter_context(mock.patch("media_downloader.init_web"))
            stack.enter_context(
                mock.patch("media_downloader.set_max_concurrent_transmissions")
            )
            stack.enter_context(
                mock.patch("media_downloader.download_all_chat", new=mock.AsyncMock())
            )
            stack.enter_context(
                mock.patch(
                    "media_downloader._exec_loop",
                    side_effect=lambda: events.append("wait"),
                )
            )
            stack.enter_context(
                mock.patch.object(app, "pre_run", side_effect=startup_error)
            )
            save = stack.enter_context(
                mock.patch.object(
                    app,
                    "update_config",
                    side_effect=lambda: events.append("config_save"),
                )
            )
            logs = stack.enter_context(mock.patch("media_downloader.logger"))
            main()
            save.assert_called_once()
        app.download_service = None
        app.task_store = None
        app.bot_token = ""
        return events, logs

    def test_main_with_bot(self):
        events, _ = self._main_lifecycle(bot=True)
        self.assertEqual(
            events,
            [
                "client_start",
                "bot_start",
                "index",
                "workers_start",
                "wait",
                "workers_stop",
                "bot_stop",
                "client_stop",
                "config_save",
            ],
        )

    def test_keyboard_interrupt(self):
        events, logs = self._main_lifecycle(startup_error=KeyboardInterrupt)
        logs.info.assert_any_call("KeyboardInterrupt")
        self.assertNotIn("workers_start", events)
        self.assertEqual(events[-1], "config_save")

    def test_other_exception(self):
        events, logs = self._main_lifecycle(
            startup_error=RuntimeError("startup failed")
        )
        logs.exception.assert_called_once()
        self.assertNotIn("workers_start", events)
        self.assertEqual(events[-1], "config_save")

    @mock.patch("media_downloader._load_config", new=load_config)
    @mock.patch("media_downloader.logger")
    def test_check_config(self, mock_logger):
        _check_config()
        mock_logger.exception.assert_called_with("load config error: error load config")

    def test_check_config_suc(self):
        app.update_config()
        self.assertEqual(_check_config(), True)

    # @mock.patch(
    #     "media_downloader.queue",
    #     new=MyQueue(
    #         [
    #             (
    #                 MockMessage(
    #                     id=312,
    #                     media=True,
    #                     chat_id=8654123,
    #                     chat_title="8654123",
    #                     video=MockVideo(
    #                         file_name="sucess_down.mp4",
    #                         mime_type="video/mp4",
    #                         file_size=1024,
    #                     ),
    #                 ),
    #                 TaskNode(chat_id=8654123, upload_telegram_chat_id=123456),
    #             ),
    #             (
    #                 MockMessage(
    #                     id=333,
    #                     media=True,
    #                     chat_id=8654123,
    #                     chat_title="8654123",
    #                     text="123",
    #                 ),
    #                 TaskNode(chat_id=8654123, upload_telegram_chat_id=123456),
    #             ),
    #         ]
    #     ),
    # )
    # @mock.patch("media_downloader.app.set_download_id", new=new_set_download_id)
    # @mock.patch("media_downloader.upload_telegram_chat", new=new_upload_telegram_chat)
    # @mock.patch("media_downloader.os.remove")
    # @mock.patch(
    #     "media_downloader._move_to_download_path", new=mock_move_to_download_path
    # )
    # @mock.patch("media_downloader.os.path.getsize", new=os_get_file_size)
    # def test_upload_telegram_chat(self, mock_remove):
    #     rest_app(MOCK_CONF)
    #     client = MockClient()
    #     app.chat_download_config[8654123].last_read_message_id = 0
    #     self.loop.run_until_complete(worker(client))
    #     mock_remove.assert_called_with(
    #         platform_generic_path("/root/project/8654123/0/312 - sucess_down.mp4")
    #     )

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()
        TEST_DIRECTORY.cleanup()
