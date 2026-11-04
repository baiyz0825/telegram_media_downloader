"""Telegram adapter for durable scanning, downloads and uploads."""

import asyncio
import os
import time

from module.app import ChatDownloadConfig, DownloadStatus, TaskNode, TaskType
from module.durable_queue import DurableQueue, node_payload
from module.transfer import (
    FileIndex,
    TaskInterrupted,
    atomic_publish,
    file_md5,
    stream_to_file,
)


class DownloadService:
    """Keep runtime clients outside SQLite and rebuild them from task payloads."""

    def __init__(self, app, client, metadata, scan, can_download, save_text):
        self.app = app
        self.client = client
        self.store = app.task_store
        self.metadata = metadata
        self.scan = scan
        self.can_download = can_download
        self.save_text = save_text
        self.nodes = {}
        self.publish_lock = asyncio.Lock()
        self.index = FileIndex(self.store, [app.save_path] + app.media_directories)
        self.queue = DurableQueue(
            self.store,
            self.handle,
            app.max_download_task,
            app.max_upload_task,
            self.terminal,
            app.retry_base_delay,
        )

    async def background(self, function, *args, **kwargs):
        # Cancelling a Future cannot stop its thread. Wait for filesystem work
        # to close handles before releasing locks or allowing a resumed claim.
        from functools import partial

        future = asyncio.get_running_loop().run_in_executor(
            None, partial(function, *args, **kwargs)
        )
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            await asyncio.shield(future)
            raise

    def remember(self, node):
        node.task_store = self.store
        self.nodes[str(node.task_id)] = node

    def restore_node(self, payload):
        group = payload["group_id"]
        if group in self.nodes:
            return self.nodes[group]
        settings = dict(payload["node"])
        settings["task_type"] = TaskType(settings["task_type"])
        node = TaskNode(**settings)
        node.language = payload.get("language")
        node.reply_chat_id = payload.get("reply_chat_id")
        if node.from_user_id:
            from module.bot import _bot

            node.bot = _bot.bot
            node.upload_user = (
                _bot.bot if payload.get("use_bot_upload") else _bot.client
            )
            if payload.get("use_bot_client"):
                node.client = _bot.bot
            _bot.add_task_node(node)
        node.is_running = True
        self.remember(node)
        return node

    def enqueue(self, message, node):
        """Commit before acknowledging; repeated scans cannot duplicate a row."""
        self.remember(node)
        payload = node_payload(node)
        payload.update(
            kind="download",
            chat_id=node.chat_id,
            source_chat_id=message.chat.id if message.chat else node.chat_id,
            message_id=message.id,
            source_name=getattr(message.chat, "title", None) or str(node.chat_id),
        )
        payload["caption"] = getattr(message, "caption", None)
        payload["text"] = getattr(message, "text", None)
        media = next(
            (
                getattr(message, kind, None)
                for kind in self.app.media_types
                if getattr(message, kind, None) is not None
            ),
            None,
        )
        payload["file_name"] = (
            getattr(media, "file_name", None) or "消息 #%s" % message.id
        )
        if node.task_type != TaskType.Download and not node.has_protected_content:
            payload["kind"] = "forward"
        key = "download:%s:%s:%s" % (
            payload["group_id"],
            payload["source_chat_id"],
            message.id,
        )
        return self.store.enqueue(
            payload,
            key,
            priority=10 if node.bot else 0,
            max_attempts=self.app.max_retries,
        )

    def enqueue_scan(self, config, node):
        self.remember(node)
        payload = node_payload(node)
        payload.update(
            kind="scan",
            chat_id=node.chat_id,
            source_name=str(node.chat_id),
            last_read_message_id=config.last_read_message_id,
            ids_to_retry=list(config.ids_to_retry),
            scan_cursor=0,
            scanned_count=0,
        )
        scan_key = "scan:%s" % node.task_id
        if not node.bot:
            scan_key += ":%s" % time.time_ns()
        return self.store.enqueue(
            payload,
            scan_key,
            priority=10 if node.bot else 0,
            max_attempts=self.app.max_retries,
        )

    async def handle(self, row):
        from module.language import use_language

        with use_language(row["payload"].get("language")):
            return await self._handle(row)

    async def _handle(self, row):
        node = self.restore_node(row["payload"])
        if node.is_stop_transmission:
            self.store.cancel(row["id"])
            raise TaskInterrupted("Task group stopped")
        if row["payload"]["kind"] == "scan":
            config = (
                self.app.chat_download_config.get(node.chat_id) or ChatDownloadConfig()
            )
            config.last_read_message_id = row["payload"].get("last_read_message_id", 0)
            config.ids_to_retry = row["payload"].get("ids_to_retry", [])
            config.download_filter = node.download_filter or config.download_filter
            # Scan retries are safe: enqueue deduplicates by group + source ID.
            await self.scan(node.client or self.client, config, node, row)
            return {"scanned": True}
        if row["payload"]["kind"] == "upload":
            return await self.upload(row, node)
        if row["payload"]["kind"] == "forward":
            return await self.forward(row, node)
        return await self.download(row, node)

    async def get_message(self, row, node):
        client = node.client or self.client
        if row["payload"].get("reply_to_message_id") and node.reply_to_message is None:
            node.reply_to_message = await asyncio.wait_for(
                self.client.get_messages(
                    row["payload"]["reply_to_chat_id"],
                    row["payload"]["reply_to_message_id"],
                ),
                self.app.request_timeout,
            )
        message = await asyncio.wait_for(
            client.get_messages(
                row["payload"]["source_chat_id"], row["payload"]["message_id"]
            ),
            self.app.request_timeout,
        )
        if not message or message.empty:
            raise ValueError("Source message was deleted or is inaccessible")
        return client, message

    async def download(self, row, node):
        from module.download_stat import update_download_status

        saved = row.get("result") or {}
        path = saved.get("path")
        if path and row.get("md5") and os.path.isfile(path):
            if await self.background(file_md5, path) == row["md5"]:
                # A crash between verified and handoff must not require the
                # original message to still exist, particularly for cloud upload.
                if (
                    node.upload_telegram_chat_id
                    or self.app.cloud_drive_config.enable_upload_file
                ):
                    self.store.handoff(
                        row["id"], row["payload"], token=row["claim_token"]
                    )
                return saved
        client, message = await self.get_message(row, node)
        if node.download_filter:
            from module.pyrogram_extension import set_meta_data
            from utils.meta_data import MetaData

            metadata = MetaData()
            set_meta_data(metadata, message, message.caption or message.text)
            self.app.download_filter.set_meta_data(metadata)
            if not self.app.download_filter.exec(node.download_filter):
                return {"skipped": True, "reason": "download filter"}
        if message.caption and self.app.is_match_advertisement(message.caption):
            return {"skipped": True, "reason": "advertisement"}
        media = next(
            (
                getattr(message, kind, None)
                for kind in self.app.media_types
                if getattr(message, kind, None) is not None
            ),
            None,
        )
        if media is None:
            if self.app.enable_download_txt and message.text:
                status, path = await self.save_text(self.app, node.chat_id, message)
                if (
                    status != DownloadStatus.Success
                    or not path
                    or not os.path.isfile(path)
                ):
                    return {
                        "skipped": True,
                        "reason": "text already exists or disabled",
                    }
                digest = await self.background(file_md5, path)
                stat = os.stat(path)
                self.store.upsert_file(path, stat.st_size, stat.st_mtime_ns, digest)
                result = {"path": path, "md5": digest, "skipped": False, "owned": True}
                self.store.update_progress(
                    row["id"],
                    stat.st_size,
                    stat.st_size,
                    path,
                    token=row["claim_token"],
                )
                self.store.checkpoint(
                    row["id"],
                    "verified",
                    path=path,
                    md5=digest,
                    result=result,
                    token=row["claim_token"],
                )
                if (
                    node.upload_telegram_chat_id
                    or self.app.cloud_drive_config.enable_upload_file
                ):
                    self.store.handoff(
                        row["id"], row["payload"], token=row["claim_token"]
                    )
                return result
            return {"skipped": True, "reason": "unsupported media"}
        kind = next(
            kind
            for kind in self.app.media_types
            if getattr(message, kind, None) is media
        )
        target, _, extension = await self.metadata(node.chat_id, message, media, kind)
        row["payload"]["file_name"] = os.path.basename(target)
        self.store.refresh_payload(row["id"], row["payload"], token=row["claim_token"])
        if not self.can_download(kind, self.app.file_formats, extension):
            return {"skipped": True, "reason": "file format filter"}
        size = getattr(media, "file_size", 0) or 0
        unique_id = getattr(media, "file_unique_id", None)
        existing = None
        owned = False
        digest = None
        async with self.publish_lock:
            if unique_id:
                existing = await self.background(self.index.find, size, None, unique_id)
            if existing:
                path = existing
                digest = await self.background(file_md5, path)
                self.store.checkpoint(
                    row["id"],
                    "verified",
                    path=path,
                    md5=digest,
                    result={
                        "path": path,
                        "md5": digest,
                        "skipped": True,
                        "owned": False,
                    },
                    token=row["claim_token"],
                )
        if not existing:
            # Task ID makes partial filenames independent from captions, title
            # changes and concurrent requests for the same original filename.
            partial = os.path.join(
                self.app.temp_save_path, "queue", "%s.part" % row["id"]
            )
            checkpoint = row.get("result") or {}
            saved = checkpoint.get("path")
            if saved and os.path.isfile(saved) and row.get("md5"):
                if await self.background(file_md5, saved) == row["md5"]:
                    path, digest = saved, row["md5"]
                    owned = checkpoint.get("owned", False)
                else:
                    saved = None
            else:
                saved = None
            if not saved:
                start = time.time()

                async def control():
                    await self.queue.control(row)

                async def progress(current, total):
                    self.store.update_progress(
                        row["id"], current, total, partial, token=row["claim_token"]
                    )
                    await update_download_status(
                        current,
                        total,
                        message.id,
                        target,
                        start,
                        node,
                        client,
                        obey_global_pause=False,
                    )

                self.store.checkpoint(
                    row["id"], "downloading", path=partial, token=row["claim_token"]
                )
                await stream_to_file(
                    client,
                    message,
                    partial,
                    size,
                    progress,
                    control,
                    self.app.idle_timeout,
                )
                digest = await self.background(file_md5, partial)
                # Hash comparison is required even for same-name/same-size media.
                async with self.publish_lock:
                    path = await self.background(
                        self.index.find, size or os.path.getsize(partial), digest
                    )
                    if path:
                        await self.background(os.remove, partial)
                    else:
                        path, owned = await self.background(
                            atomic_publish, partial, target, True
                        )
                        stat = os.stat(path)
                        self.store.upsert_file(
                            path, stat.st_size, stat.st_mtime_ns, digest, unique_id
                        )
            stat = os.stat(path)
            self.store.upsert_file(
                path, stat.st_size, stat.st_mtime_ns, digest, unique_id
            )
        result = {"path": path, "md5": digest, "skipped": not owned, "owned": owned}
        self.store.update_progress(
            row["id"],
            os.path.getsize(path),
            os.path.getsize(path),
            path,
            token=row["claim_token"],
        )
        self.store.checkpoint(
            row["id"],
            "verified",
            path=path,
            md5=digest,
            result=result,
            token=row["claim_token"],
        )
        if (
            node.upload_telegram_chat_id
            or self.app.cloud_drive_config.enable_upload_file
        ):
            self.store.handoff(row["id"], row["payload"], token=row["claim_token"])
        return result

    async def upload(self, row, node):
        """Upload owns separate consumers; user media roots are never deleted."""
        from module.pyrogram_extension import _upload_signal_message, process_caption

        path = row["path"]
        if not path or not os.path.isfile(path):
            raise FileNotFoundError("Verified local file is missing")
        if await self.background(file_md5, path) != row["md5"]:
            raise ValueError("Local file changed after download verification")
        result = dict(row.get("result") or {})
        # Keep the source until the upload and its DB checkpoint both succeed.
        if node.upload_telegram_chat_id:
            if not result.get("telegram_uploaded"):
                client, message = await self.get_message(row, node)
                caption = await process_caption(
                    client,
                    self.app,
                    node.upload_telegram_chat_id,
                    message.caption,
                    message.caption_entities,
                )
                await _upload_signal_message(
                    client,
                    node.upload_user or client,
                    self.app,
                    node,
                    node.upload_telegram_chat_id,
                    message,
                    path,
                    caption,
                )
                result["telegram_uploaded"] = True
        elif not result.get("cloud_uploaded"):
            # Disable adapter deletion; cleanup happens only after DB completion.
            import copy

            from module.cloud_drive import CloudDrive
            from module.pyrogram_extension import update_cloud_upload_stat

            config = copy.copy(self.app.cloud_drive_config)
            config.after_upload_file_delete = False
            if config.upload_adapter != "rclone":
                ok = await self.background(
                    CloudDrive.aligo_upload_file, config, self.app.save_path, path
                )
            else:
                ok = await CloudDrive.rclone_upload_file(
                    config,
                    self.app.save_path,
                    path,
                    update_cloud_upload_stat,
                    (node, row["payload"]["message_id"], path),
                )
            if not ok:
                raise ConnectionError("Cloud upload or remote verification failed")
            result["cloud_uploaded"] = True
            result["cloud_verified"] = bool(config.last_upload_verified)
        self.store.checkpoint(
            row["id"], "uploaded", result=result, token=row["claim_token"]
        )
        return result

    async def forward(self, row, node):
        """Persist each copied message independently of an in-memory album cache."""
        from module.pyrogram_extension import process_caption

        client, message = await self.get_message(row, node)
        if node.download_filter:
            from module.pyrogram_extension import set_meta_data
            from utils.meta_data import MetaData

            metadata = MetaData()
            set_meta_data(metadata, message, message.caption or message.text)
            self.app.download_filter.set_meta_data(metadata)
            if not self.app.download_filter.exec(node.download_filter):
                return {"skipped": True, "reason": "download filter"}
        if message.caption and self.app.is_match_advertisement(message.caption):
            return {"skipped": True, "reason": "advertisement"}
        destination = node.upload_telegram_chat_id
        reply_to = node.reply_to_message
        if reply_to:
            destination = reply_to.chat.id
        if message.media:
            node.has_protected_content = True
            return await self.download(row, node)
        caption = await process_caption(
            client, self.app, destination, message.caption, message.caption_entities
        )
        if message.text:
            text = await process_caption(
                client, self.app, destination, message.text, message.entities
            )
            sent = await (node.upload_user or client).send_message(
                destination,
                text,
                parse_mode="html",
                message_thread_id=node.topic_id or None,
                reply_to_message_id=reply_to.id if reply_to else None,
            )
        else:
            raise ValueError("Forward task has no text or supported media")
        return {"forwarded": True, "destination_message_id": sent.id}

    async def terminal(self, row):
        from module.language import use_language

        with use_language(row["payload"].get("language")):
            return await self._terminal(row)

    async def _terminal(self, row):
        """Report only after the entire pipeline reaches its final state."""
        from module.pyrogram_extension import report_bot_download_status

        node = self.restore_node(row["payload"])
        if row["payload"]["kind"] == "scan":
            if row["state"] == "failed":
                node.is_running = True
                if node.bot:
                    await node.bot.send_message(
                        node.reply_chat_id or node.from_user_id,
                        "扫描失败：%s。使用 /queue 查看原因并重试。" % row["error"],
                        parse_mode=None,
                    )
            return
        result = row.get("result") or {}
        status = (
            DownloadStatus.FailedDownload
            if row["state"] == "failed"
            else (
                DownloadStatus.SkipDownload
                if result.get("skipped")
                else DownloadStatus.SuccessDownload
            )
        )
        node.download_status[row["payload"]["message_id"]] = status
        if not node.bot:
            self.app.set_download_id(node, row["payload"]["message_id"], status)
        await report_bot_download_status(
            node.bot, node, status, row["downloaded_bytes"]
        )
        # Do not delete indexed media belonging to the user or another task.
        delete = (
            result.get("telegram_uploaded") and self.app.after_upload_telegram_delete
        ) or (
            result.get("cloud_verified")
            and self.app.cloud_drive_config.after_upload_file_delete
        )
        if row["state"] == "completed" and result.get("owned") and delete:
            path = result.get("path")
            async with self.publish_lock:
                if (
                    path
                    and not self.store.path_in_use(path, excluding=row["id"])
                    and os.path.isfile(path)
                    and await self.background(file_md5, path) == row["md5"]
                ):
                    await self.background(os.remove, path)
