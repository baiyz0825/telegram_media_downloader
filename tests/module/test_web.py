"""Console security and queue controls, without a Telegram connection."""

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from module import web
from module.download_stat import DownloadState, set_download_state
from module.task_store import TaskStore


class WebTestCase(unittest.TestCase):
    def setUp(self):
        self.store = TaskStore(":memory:")
        self.app = web.get_flask_app()
        self.app.config.update(
            TESTING=True,
            LOGIN_DISABLED=True,
            SECRET_KEY="test-only-random-session-secret",
        )
        self.application = SimpleNamespace(
            task_store=self.store, chat_download_config={}
        )
        self.patch = mock.patch.object(web, "_application", self.application)
        self.patch.start()
        self.users_patch = mock.patch.object(web, "web_login_users", {})
        self.users_patch.start()
        self.client = self.app.test_client()
        self.client.get("/")
        with self.client.session_transaction() as session:
            self.csrf = session["csrf_token"]
        self.headers = {"X-CSRF-Token": self.csrf}

    def tearDown(self):
        self.users_patch.stop()
        self.patch.stop()
        self.store.close()
        self.app.config["LOGIN_DISABLED"] = False
        set_download_state(DownloadState.Downloading)

    def enqueue(self, name, key):
        return self.store.enqueue(
            {"file_name": name, "chat_id": 42, "group_id": "batch"}, key
        )

    def test_csrf_required_even_without_login(self):
        row = self.enqueue("video.mp4", "video")
        response = self.client.post("/api/tasks/%s/cancel" % row["id"])
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.store.get(row["id"])["state"], "queued")
        response = self.client.post(
            "/api/tasks/%s/cancel" % row["id"], headers=self.headers
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.store.get(row["id"])["state"], "cancelled")

    def test_search_pagination_and_task_controls(self):
        first = self.enqueue('a "quoted".mp4', "a")
        second = self.enqueue("第二个.mp4", "b")
        page = self.client.get("/api/tasks?limit=1&page=1").get_json()
        self.assertEqual(page["total"], 2)
        self.assertEqual(len(page["items"]), 1)
        result = self.client.get(
            "/api/tasks", query_string={"search": '"quoted"'}
        ).get_json()
        self.assertEqual(result["items"][0]["id"], first["id"])
        for action, expected in (
            ("pause", "paused"),
            ("resume", "queued"),
            ("cancel", "cancelled"),
            ("retry", "queued"),
        ):
            response = self.client.post(
                "/api/tasks/%s/%s" % (first["id"], action), headers=self.headers
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(self.store.get(first["id"])["state"], expected)
        response = self.client.post(
            "/api/tasks/%s/promote" % second["id"], headers=self.headers
        )
        self.assertEqual(response.status_code, 200)
        self.assertGreater(self.store.get(second["id"])["priority"], 0)

    def test_bulk_controls_and_input_validation(self):
        row = self.enqueue("video.mp4", "video")
        for action, expected in (("pause", "paused"), ("resume", "queued")):
            response = self.client.post(
                "/api/tasks/bulk/" + action, headers=self.headers
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(self.store.get(row["id"])["state"], expected)
        self.assertEqual(self.client.get("/api/tasks?page=no").status_code, 400)
        self.assertEqual(self.client.get("/api/tasks?state=invalid").status_code, 400)
        self.assertEqual(
            self.client.post("/api/tasks/999/cancel", headers=self.headers).status_code,
            404,
        )
        self.assertEqual(
            self.client.post(
                "/api/tasks/bulk/destroy", headers=self.headers
            ).status_code,
            400,
        )

    def test_legacy_json_handles_quotes_and_empty_files(self):
        data = {
            42: {
                1: {
                    "total_size": 0,
                    "down_byte": 0,
                    "file_name": 'a"<x>.mp4',
                    "download_speed": 0,
                }
            }
        }
        with mock.patch.object(web, "get_download_result", return_value=data):
            response = self.client.get("/get_download_list?already_down=false")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()[0]["filename"], 'a"<x>.mp4')
        self.assertEqual(response.get_json()[0]["download_progress"], 0)

    def test_resume_one_after_bulk_pause_keeps_other_tasks_paused(self):
        first = self.enqueue("first.mp4", "first")
        second = self.enqueue("second.mp4", "second")
        self.client.post("/api/tasks/bulk/pause", headers=self.headers)
        self.assertTrue(self.client.get("/api/stats").get_json()["paused"])
        set_download_state(DownloadState.StopDownload)
        self.client.post("/api/tasks/%s/resume" % first["id"], headers=self.headers)
        self.assertEqual(web.get_download_state(), DownloadState.Downloading)
        self.assertEqual(self.store.get(second["id"])["state"], "paused")
        self.assertFalse(self.client.get("/api/stats").get_json()["paused"])

    def test_authentication_rejects_unknown_user_and_requires_password(self):
        self.app.config["LOGIN_DISABLED"] = False
        web.web_login_users["root"] = "console-password"
        self.assertIsNone(web.load_user("intruder"))
        self.assertEqual(self.client.get("/api/tasks").status_code, 401)
        self.assertEqual(
            self.client.post(
                "/login", json={"password": "wrong"}, headers=self.headers
            ).status_code,
            401,
        )
        self.assertEqual(
            self.client.post(
                "/login", json={"password": "console-password"}, headers=self.headers
            ).status_code,
            200,
        )
        self.assertEqual(self.client.get("/api/tasks").status_code, 200)
        with self.client.session_transaction() as session:
            self.assertNotEqual(session["csrf_token"], self.csrf)

    def test_secret_is_persistent_and_rejects_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            first = web._load_session_secret(directory)
            self.assertGreaterEqual(len(first), 32)
            self.assertEqual(first, web._load_session_secret(directory))
            path = Path(directory) / ".web_session_secret"
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            path.write_text("tdl", encoding="utf-8")
            with self.assertRaises(ValueError):
                web._load_session_secret(directory)

    def test_upload_speed_uses_active_transfers(self):
        values = {
            1: SimpleNamespace(upload_size=3, total_size=10, upload_speed=500),
            2: SimpleNamespace(upload_size=10, total_size=10, upload_speed=999),
        }
        node = SimpleNamespace(upload_stat_dict=values)
        self.application.chat_download_config = {42: SimpleNamespace(node=node)}
        self.assertEqual(web._upload_speed(), 500)

    def test_stats_reports_network_and_configured_limits(self):
        self.application.download_service = SimpleNamespace(
            client=SimpleNamespace(is_connected=True)
        )
        self.application.max_download_task = 4
        self.application.max_upload_task = 2
        response = self.client.get("/api/stats")
        self.assertEqual(response.status_code, 200)
        network = response.get_json()["network"]
        self.assertTrue(network["connected"])
        self.assertEqual(network["download_limit"], 4)
        self.assertEqual(network["upload_limit"], 2)


if __name__ == "__main__":
    unittest.main()
