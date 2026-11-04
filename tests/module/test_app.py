"""test app"""

import os
import sys
import tempfile
import unittest
from unittest import mock

import module.app
from module.app import Application, ChatDownloadConfig, DownloadStatus

sys.path.append("..")  # Adds higher directory to python modules path.


class AppTestCase(unittest.TestCase):
    def test_app(self):
        app = Application("", "")
        self.addCleanup(app.loop.close)
        self.assertEqual(app.save_path, os.path.join(os.path.abspath("."), "downloads"))
        self.assertEqual(app.proxy, {})
        self.assertEqual(app.restart_program, False)

        app.chat_download_config[123] = ChatDownloadConfig()
        app.chat_download_config[123].last_read_message_id = 13
        app.chat_download_config[123].node.download_status[
            6
        ] = DownloadStatus.Downloading
        app.chat_download_config[123].ids_to_retry.append(7)
        # download success
        app.chat_download_config[123].node.download_status[
            8
        ] = DownloadStatus.SuccessDownload
        app.chat_download_config[123].finish_task += 1
        # download success
        app.chat_download_config[123].node.download_status[
            10
        ] = DownloadStatus.SuccessDownload
        app.chat_download_config[123].finish_task += 1
        # not exist message
        app.chat_download_config[123].node.download_status[
            13
        ] = DownloadStatus.SuccessDownload
        app.config["chat"] = [{"chat_id": 123, "last_read_message_id": 5}]

        app.update_config(False)

        self.assertEqual(
            app.chat_download_config[123].last_read_message_id + 1,
            app.config["chat"][0]["last_read_message_id"],
        )
        self.assertEqual(
            [6, 7],
            app.app_data["chat"][0]["ids_to_retry"],
        )

    def test_update_config(self):
        app = Application("", "")
        self.addCleanup(app.loop.close)
        with tempfile.TemporaryDirectory() as directory:
            app.config_file = os.path.join(directory, "config.yaml")
            app.app_data_file = os.path.join(directory, "data.yaml")
            app.config["chat"] = [{"chat_id": 123, "last_read_message_id": 0}]
            app.chat_download_config[123] = ChatDownloadConfig()
            app.update_config()
            with open(app.config_file, encoding="utf-8") as stream:
                config = module.app._yaml.load(stream)
            with open(app.app_data_file, encoding="utf-8") as stream:
                data = module.app._yaml.load(stream)
            self.assertEqual(config["chat"][0]["chat_id"], 123)
            self.assertEqual(data["chat"][0]["chat_id"], 123)
            self.assertEqual(
                sorted(os.listdir(directory)), ["config.yaml", "data.yaml"]
            )

    def test_atomic_yaml_failure_keeps_previous_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "config.yaml")
            Application._atomic_yaml(path, {"previous": True})
            with mock.patch(
                "module.app.os.replace", side_effect=OSError("disk failure")
            ):
                with self.assertRaises(OSError):
                    Application._atomic_yaml(path, {"previous": False})
            with open(path, encoding="utf-8") as stream:
                self.assertEqual(module.app._yaml.load(stream), {"previous": True})
            self.assertEqual(os.listdir(directory), ["config.yaml"])
