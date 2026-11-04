"""SQLite-backed consumers, independent from the Telegram transport.

Only JSON payloads enter the queue. A claim token fences stale consumers after
pause/resume; cooperative cancellation also closes streams and subprocesses.
"""

import asyncio

from loguru import logger

from module.transfer import TaskInterrupted, classify_error


class DurableQueue:
    """Run bounded asynchronous consumers against a durable task store."""

    def __init__(
        self,
        store,
        handler,
        concurrency=5,
        upload_concurrency=2,
        on_terminal=None,
        base_delay=3,
    ):
        self.store = store
        self.handler = handler
        self.concurrency = concurrency
        self.upload_concurrency = upload_concurrency
        self.on_terminal = on_terminal
        self.base_delay = base_delay
        self.running = True
        self.tasks = []
        self.active = {}  # A resumed row cannot run before its old claim exits.

    async def control(self, row):
        current = self.store.get(row["id"])
        if (
            not current
            or current["state"] != "running"
            or current["claim_token"] != row["claim_token"]
        ):
            raise TaskInterrupted("Task paused, cancelled or superseded")

    def start(self):
        """Downloads and uploads have separate capacity limits."""
        loop = asyncio.get_running_loop()
        for _ in range(self.concurrency):
            self.tasks.append(loop.create_task(self.consume(("download",))))
        self.tasks.append(loop.create_task(self.consume(("scan",))))
        for _ in range(self.upload_concurrency):
            self.tasks.append(loop.create_task(self.consume(("upload", "forward"))))

    async def consume(self, kinds):
        while self.running:
            row = self.store.claim(kinds=kinds)
            if row is None:
                await asyncio.sleep(0.2)
                continue
            # Another worker may have claimed a row immediately after resume.
            # Wait for the old coroutine's file handles to close before using it.
            old = self.active.get(row["id"])
            if old:
                await asyncio.gather(old, return_exceptions=True)
            operation = asyncio.create_task(self.execute(row))
            self.active[row["id"]] = operation
            try:
                while not operation.done():
                    await asyncio.sleep(0.15)
                    current = self.store.get(row["id"])
                    if (
                        not current
                        or current["state"] in ("paused", "cancelled")
                        or current["claim_token"] != row["claim_token"]
                    ):
                        operation.cancel()
                        break
                await asyncio.gather(operation, return_exceptions=True)
            finally:
                if self.active.get(row["id"]) is operation:
                    self.active.pop(row["id"], None)

    async def execute(self, row):
        """Every claim either hands off, reaches a terminal state or schedules retry."""
        token = row["claim_token"]
        terminal = False
        try:
            await self.control(row)
            result = await self.handler(row)
            # A handler can atomically hand this same row to the upload pool.
            current = self.store.get(row["id"])
            if current["state"] == "running" and current["claim_token"] == token:
                terminal = self.store.complete(row["id"], result, token=token)
        except (TaskInterrupted, asyncio.CancelledError):
            # Control state and partial file are retained; shutdown recovery puts
            # running claims back in the queue without consuming a retry.
            return
        except Exception as error:
            decision = classify_error(error, row["attempt"], self.base_delay)
            self.store.fail(
                row["id"],
                str(error),
                decision.kind,
                decision.delay if decision.retry else None,
                token=token,
            )
            logger.warning("Task {} {}: {}", row["id"], decision.kind, error)
            terminal = self.store.get(row["id"])["state"] == "failed"
        if terminal and self.on_terminal:
            try:
                await self.on_terminal(self.store.get(row["id"]))
            except Exception:
                logger.exception("Task result notification failed")

    async def stop(self):
        """Cancel active work first, then reset only interrupted running claims."""
        self.running = False
        for operation in list(self.active.values()):
            operation.cancel()
        await asyncio.gather(*list(self.active.values()), return_exceptions=True)
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.active.clear()
        self.store.recover_running()


def node_payload(node):
    """Persist primitive TaskNode settings, never clients or Python messages."""
    keys = (
        "chat_id",
        "from_user_id",
        "reply_message_id",
        "upload_telegram_chat_id",
        "has_protected_content",
        "download_filter",
        "limit",
        "start_offset_id",
        "end_offset_id",
        "task_id",
        "topic_id",
    )
    settings = {key: getattr(node, key) for key in keys}
    settings["task_type"] = node.task_type.value
    return {
        "group_id": str(node.task_id),
        "from_user_id": node.from_user_id,
        "reply_chat_id": getattr(node, "reply_chat_id", None) or node.from_user_id,
        "use_bot_client": node.client is not None,
        "use_bot_upload": getattr(node, "upload_as_bot", False),
        "reply_to_chat_id": node.reply_to_message.chat.id
        if node.reply_to_message
        else None,
        "reply_to_message_id": node.reply_to_message.id
        if node.reply_to_message
        else None,
        "language": getattr(node, "language", None),
        "node": settings,
    }
