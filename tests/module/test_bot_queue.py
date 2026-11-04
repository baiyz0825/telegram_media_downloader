"""Bot queue actions exercise a real database without connecting to Telegram."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from module import bot
from module.app import TaskNode, TaskType
from module.language import Language, _t, get_language, use_language
from module.task_store import TaskStore


@pytest.fixture
def queue_bot(tmp_path):
    previous = bot._bot
    instance = bot.DownloadBot()
    instance.admin_id = 99
    instance.config_path = str(tmp_path / "bot.yaml")
    store = TaskStore(str(tmp_path / "tasks.db"))
    instance.app = SimpleNamespace(task_store=store)
    bot._bot = instance
    try:
        yield instance, store
    finally:
        store.close()
        bot._bot = previous


def message(text="/queue", user_id=12):
    return SimpleNamespace(
        text=text,
        id=5,
        chat=SimpleNamespace(id=100),
        from_user=SimpleNamespace(id=user_id),
    )


def enqueue(store, owner=12, group="1"):
    return store.enqueue(
        {
            "from_user_id": owner,
            "group_id": group,
            "chat_id": -100,
            "file_name": "test.mp4",
        },
        f"{owner}:{group}",
    )


def test_actions_check_owner_before_mutation(queue_bot):
    _, store = queue_bot
    row = enqueue(store)
    result, error = bot.apply_queue_action(store, row["id"], "cancel", 13)
    assert result is None and "权限" in error
    assert store.get(row["id"])["state"] == "queued"
    result, error = bot.apply_queue_action(store, row["id"], "pause", 12)
    assert error is None and result["state"] == "paused"
    result, error = bot.apply_queue_action(store, row["id"], "resume", 12)
    assert error is None and result["state"] == "queued"
    result, _ = bot.apply_queue_action(store, row["id"], "priority", 12)
    assert result["priority"] >= 100
    result, _ = bot.apply_queue_action(store, row["id"], "cancel", 99)
    assert result["state"] == "cancelled"
    result, _ = bot.apply_queue_action(store, row["id"], "retry", 12)
    assert result["state"] == "queued"


def test_nested_owner_is_recognized(queue_bot):
    assert bot.can_manage_queue_task({"payload": {"node": {"from_user_id": 12}}}, 12)
    assert not bot.can_manage_queue_task({"payload": {}}, 12)
    assert bot.can_manage_queue_task({"payload": {}}, 99)


def test_queue_list_only_shows_callers_tasks(queue_bot):
    _, store = queue_bot
    enqueue(store, owner=12)
    other = enqueue(store, owner=13)
    client = SimpleNamespace(send_message=AsyncMock())
    asyncio.run(bot.queue_command(client, message()))
    args = client.send_message.await_args
    assert "#1" in args.args[1]
    assert f"#{other['id']}" not in args.args[1]


def test_callback_answers_and_edits_correct_chat(queue_bot):
    _, store = queue_bot
    row = enqueue(store)
    query = SimpleNamespace(
        data=f"queue:pause:{row['id']}",
        from_user=SimpleNamespace(id=12),
        message=SimpleNamespace(chat=SimpleNamespace(id=100), id=5),
        answer=AsyncMock(),
    )
    client = SimpleNamespace(edit_message_text=AsyncMock(), send_message=AsyncMock())
    asyncio.run(bot.on_query_handler(client, query))
    query.answer.assert_awaited_once()
    assert client.edit_message_text.await_args.args[:2] == (100, 5)
    assert store.get(row["id"])["state"] == "paused"


def test_legacy_stop_callback_cancels_before_edit(queue_bot):
    instance, store = queue_bot
    instance.add_task_node(TaskNode(1, from_user_id=12, task_id=1))
    row = enqueue(store)
    query = SimpleNamespace(
        data="stop_download task all",
        from_user=SimpleNamespace(id=12),
        message=SimpleNamespace(chat=SimpleNamespace(id=100), id=5),
        answer=AsyncMock(),
    )
    client = SimpleNamespace(
        edit_message_text=AsyncMock(side_effect=RuntimeError("deleted reply"))
    )
    with pytest.raises(RuntimeError):
        asyncio.run(bot.on_query_handler(client, query))
    assert store.get(row["id"])["state"] == "cancelled"
    assert client.edit_message_text.await_args.args[:2] == (100, 5)


def test_stop_all_is_scoped_by_type_and_owner(queue_bot):
    instance, store = queue_bot
    nodes = [
        TaskNode(1, from_user_id=12, task_id=1, task_type=TaskType.Download),
        TaskNode(1, from_user_id=12, task_id=2, task_type=TaskType.Forward),
        TaskNode(1, from_user_id=13, task_id=3, task_type=TaskType.Download),
    ]
    for node in nodes:
        instance.add_task_node(node)
        enqueue(store, owner=node.from_user_id, group=str(node.task_id))
    instance.stop_task("all", TaskType.Download, 12)
    assert [node.is_stop_transmission for node in nodes] == [True, False, False]
    assert [
        row["state"] for row in sorted(store.list_tasks(), key=lambda row: row["id"])
    ] == ["cancelled", "queued", "queued"]


def test_shutdown_keeps_persisted_jobs(queue_bot):
    instance, store = queue_bot
    instance.add_task_node(TaskNode(1, from_user_id=12, task_id=1))
    row = enqueue(store)
    instance.stop_task("all", cancel_persisted=False)
    assert store.get(row["id"])["state"] == "queued"


def test_filter_saved_to_real_bot_config(queue_bot):
    instance, _ = queue_bot
    instance.filter.check_filter = Mock(return_value=(True, None))
    client = SimpleNamespace(send_message=AsyncMock())
    asyncio.run(bot.add_filter(client, message("/add_filter file_size > 100")))
    restored = bot.DownloadBot()
    with open(instance.config_path, encoding="utf-8") as stream:
        restored.assign_config(instance._yaml.load(stream))
    assert restored.user_download_filters == {"12": "file_size > 100"}


def test_default_filter_reaches_download_configuration(queue_bot):
    instance, _ = queue_bot
    instance.user_download_filters = {"12": "file_size > 100"}
    instance.filter.check_filter = Mock(return_value=(True, None))
    instance.client = SimpleNamespace(
        get_chat=AsyncMock(return_value=SimpleNamespace(id=-100, title="source"))
    )
    instance.app.loop = Mock()
    instance.bot = object()
    instance.download_chat_task = Mock(return_value="scan coroutine")
    client = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(id=9)))
    with patch.object(bot, "parse_link", AsyncMock(return_value=(-100, None, None))):
        asyncio.run(
            bot.download_from_bot(client, message("/download https://t.me/source 1 0"))
        )
    configuration = instance.download_chat_task.call_args.args[1]
    assert configuration.download_filter == "file_size > 100"
    instance.app.loop.create_task.assert_called_once_with("scan coroutine")


def test_missing_link_message_is_reported_with_await(queue_bot):
    instance, _ = queue_bot
    instance.client = SimpleNamespace(
        get_chat=AsyncMock(return_value=SimpleNamespace(id=-100, title="source")),
        get_messages=AsyncMock(),
    )
    client = SimpleNamespace(send_message=AsyncMock())
    with patch.object(
        bot, "parse_link", AsyncMock(return_value=(-100, 1, None))
    ), patch.object(bot, "retry", AsyncMock(return_value=None)):
        asyncio.run(bot.download_from_link(client, message("https://t.me/source/1")))
    client.send_message.assert_awaited_once()


@pytest.mark.parametrize(
    "command",
    [
        "/download https://t.me/channel 10 1",
        "/download https://t.me/channel -1 0",
        "/download https://t.me/channel abc 0",
    ],
)
def test_invalid_ranges_reply_without_network(queue_bot, command):
    client = SimpleNamespace(send_message=AsyncMock())
    with patch.object(bot, "parse_link", AsyncMock()) as parse:
        asyncio.run(bot.download_from_bot(client, message(command)))
    client.send_message.assert_awaited_once()
    parse.assert_not_awaited()


def test_invalid_link_has_friendly_reply(queue_bot):
    client = SimpleNamespace(send_message=AsyncMock())
    with patch.object(bot, "parse_link", AsyncMock(return_value=(None, None, None))):
        asyncio.run(bot.download_from_bot(client, message("/download invalid 1 0")))
    assert "无法识别" in client.send_message.await_args.args[1]


def test_plain_text_summary_bounds_remote_content(queue_bot):
    _, store = queue_bot
    row = enqueue(store)
    row["payload"]["file_name"] = "<b>" + "x" * 5000
    row["error"] = "error " * 5000
    assert len(bot.queue_task_text(row)) < 250


def test_concurrent_handlers_keep_user_languages_isolated(queue_bot):
    instance, _ = queue_bot
    instance.user_languages = {"12": "ZH", "13": "RU"}
    baseline = get_language()

    async def sample_handler(client, update):
        before = _t("Success")
        await asyncio.sleep(0)
        return before, _t("Success"), get_language().name

    async def concurrent():
        handler = bot.localized_handler(sample_handler)
        return await asyncio.gather(
            handler(None, message(user_id=12)), handler(None, message(user_id=13))
        )

    assert asyncio.run(concurrent()) == [
        ("成功", "成功", "ZH"),
        ("Успешно", "Успешно", "RU"),
    ]
    assert get_language() is baseline


def test_language_context_restores_after_error():
    baseline = get_language()
    with pytest.raises(RuntimeError):
        with use_language("ZH"):
            assert _t("Success") == "成功"
            raise RuntimeError("handler failed")
    assert get_language() is baseline
    with use_language("invalid"):
        assert get_language() is baseline


def test_language_command_only_changes_user_and_chat_menu(queue_bot):
    instance, _ = queue_bot
    instance.user_languages = {"13": "RU"}
    baseline = get_language()
    client = SimpleNamespace(send_message=AsyncMock(), set_bot_commands=AsyncMock())
    update = message("/set_language zh")
    update.chat.id = update.from_user.id
    asyncio.run(bot.localized_handler(bot.set_language)(client, update))
    assert instance.user_languages == {"12": "ZH", "13": "RU"}
    assert get_language() is baseline
    scope = client.set_bot_commands.await_args.kwargs["scope"]
    assert isinstance(scope, bot.types.BotCommandScopeChat)
    assert scope.chat_id == 12
    commands = client.set_bot_commands.await_args.args[0]
    assert (
        next(
            command.description for command in commands if command.command == "download"
        )
        == "下载消息"
    )
    restored = bot.DownloadBot()
    with open(instance.config_path, encoding="utf-8") as stream:
        restored.assign_config(instance._yaml.load(stream))
    assert restored.user_languages == instance.user_languages


def test_group_language_menu_scoped_to_member(queue_bot):
    client = SimpleNamespace(send_message=AsyncMock(), set_bot_commands=AsyncMock())
    asyncio.run(bot.set_language(client, message("/set_language ru")))
    scope = client.set_bot_commands.await_args.kwargs["scope"]
    assert isinstance(scope, bot.types.BotCommandScopeChatMember)
    assert scope.chat_id == 100 and scope.user_id == 12


def test_queue_summary_shows_phase_and_retry_deadline(queue_bot):
    _, store = queue_bot
    row = enqueue(store)
    row.update(
        state="retry_wait",
        next_run=120,
        attempt=2,
        max_attempts=5,
        stage="uploading",
        phase="verified",
    )
    with patch.object(bot.time, "time", return_value=100), use_language(Language.ZH):
        text = bot.queue_task_text(row)
    assert "上传中" in text and "距离重试秒数: 20" in text and "2/5" in text


def listener(queue_bot):
    instance, _ = queue_bot
    instance.bot, instance.client = object(), object()
    instance.app.download_service = SimpleNamespace()
    node = TaskNode(
        -100,
        from_user_id=12,
        task_id=1000,
        task_type=TaskType.ListenForward,
        upload_telegram_chat_id=-200,
        download_filter="file_size > 100",
    )
    node.bot = instance.bot
    node.upload_user = instance.bot
    node.reply_chat_id = 12
    node.language = "RU"
    node.is_running = True
    instance.add_task_node(node)
    instance.listen_forward_chat[node.chat_id] = node
    return instance, node


def test_listener_roundtrip_restores_cursor_and_runtime_clients(queue_bot):
    instance, node = listener(queue_bot)
    instance.save_listener(node, cursor=100)
    restored = bot.DownloadBot()
    with open(instance.config_path, encoding="utf-8") as stream:
        restored.assign_config(instance._yaml.load(stream))
    restored.allowed_user_ids = [12]
    restored.bot, restored.client = object(), object()
    rebuilt_node = TaskNode(
        -100, from_user_id=12, task_id=1000, task_type=TaskType.ListenForward
    )
    rebuild = Mock(return_value=rebuilt_node)
    loop = Mock()
    loop.create_task.side_effect = lambda coroutine: coroutine.close()
    restored.app = SimpleNamespace(
        download_service=SimpleNamespace(restore_node=rebuild), loop=loop
    )
    restored.restore_listeners()
    payload = rebuild.call_args.args[0]
    assert payload["node"]["download_filter"] == "file_size > 100"
    assert payload["language"] == "RU" and payload["reply_chat_id"] == 12
    assert rebuilt_node.last_message_id == 100
    assert rebuilt_node.upload_user is restored.bot
    assert restored.listen_forward_chat[-100] is rebuilt_node
    loop.create_task.assert_called_once()


def test_listener_enqueue_failure_never_advances_cursor(queue_bot):
    instance, node = listener(queue_bot)
    instance.save_listener(node, cursor=10)
    instance.add_download_task = AsyncMock(side_effect=RuntimeError("database full"))

    async def history(*args, **kwargs):
        yield SimpleNamespace(id=11)

    with patch.object(bot, "get_chat_history_v2", history):
        cursor = asyncio.run(
            bot.check_new_messages(instance.client, node.chat_id, node, 10)
        )
    assert cursor == 10 and node.last_message_id == 10
    with open(instance.config_path, encoding="utf-8") as stream:
        saved = instance._yaml.load(stream)
    assert saved["listen_forward_configs"]["-100"]["last_message_id"] == 10


def test_listener_only_commits_cursor_for_successfully_enqueued_messages(queue_bot):
    instance, node = listener(queue_bot)
    instance.save_listener(node, cursor=10)
    instance.add_download_task = AsyncMock(
        side_effect=[True, RuntimeError("database full")]
    )

    async def history(*args, **kwargs):
        yield SimpleNamespace(id=11)
        yield SimpleNamespace(id=12)

    with patch.object(bot, "get_chat_history_v2", history):
        cursor = asyncio.run(
            bot.check_new_messages(instance.client, node.chat_id, node, 10)
        )
    assert cursor == 11 and node.last_message_id == 11
    assert instance.listen_forward_configs["-100"]["last_message_id"] == 11


def test_stopping_listener_disables_restart_but_shutdown_preserves_it(queue_bot):
    instance, node = listener(queue_bot)
    instance.save_listener(node, cursor=10)
    instance.stop_task(
        "all", task_type=TaskType.ListenForward, owner_id=12, cancel_persisted=False
    )
    assert instance.listen_forward_configs["-100"]["enabled"] is True
    node.is_stop_transmission = False
    instance.stop_task("all", task_type=TaskType.ListenForward, owner_id=12)
    assert instance.listen_forward_configs["-100"]["enabled"] is False
    assert node.is_stop_transmission
    with open(instance.config_path, encoding="utf-8") as stream:
        saved = instance._yaml.load(stream)
    assert saved["listen_forward_configs"]["-100"]["enabled"] is False


def test_listener_cursor_rolls_back_when_config_save_fails(queue_bot):
    instance, node = listener(queue_bot)
    instance.save_listener(node, cursor=10)
    with patch.object(instance, "update_config", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            instance.save_listener(node, cursor=11)
    assert instance.listen_forward_configs["-100"]["last_message_id"] == 10
    assert node.last_message_id == 10


def test_listener_empty_baseline_does_not_skip_first_new_message(queue_bot):
    instance, node = listener(queue_bot)
    instance.save_listener(node, cursor=0)
    instance.add_download_task = AsyncMock(return_value=True)

    async def history(*args, **kwargs):
        yield SimpleNamespace(id=1)

    with patch.object(bot, "get_chat_history_v2", history):
        cursor = asyncio.run(
            bot.check_new_messages(instance.client, node.chat_id, node, 0)
        )
    instance.add_download_task.assert_awaited_once()
    assert cursor == 1 and node.last_message_id == 1


def test_durable_download_handler_waits_for_scan_commit(queue_bot):
    instance, _ = queue_bot
    instance.app.download_service = object()
    instance.app.loop = Mock()
    instance.client = SimpleNamespace(
        get_chat=AsyncMock(return_value=SimpleNamespace(id=-100, title="source"))
    )
    instance.download_chat_task = AsyncMock(return_value=True)
    client = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(id=9)))
    with patch.object(bot, "parse_link", AsyncMock(return_value=(-100, None, None))):
        asyncio.run(
            bot.download_from_bot(client, message("/download https://t.me/source 1 0"))
        )
    instance.download_chat_task.assert_awaited_once()
    instance.app.loop.create_task.assert_not_called()


def test_unprotected_forward_uses_durable_scan(queue_bot):
    instance, _ = queue_bot
    instance.app.download_service = object()
    node = TaskNode(-100, has_protected_content=False, task_type=TaskType.Forward)
    with patch.object(
        bot, "get_forward_task_node", AsyncMock(return_value=node)
    ), patch.object(bot, "forward_msg", AsyncMock()) as forward:
        asyncio.run(
            bot.forward_message_impl(
                None, message("/forward https://t.me/src https://t.me/dst 1 0"), False
            )
        )
    forward.assert_awaited_once_with(node, 1)
