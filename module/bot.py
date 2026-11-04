"""Bot for media downloader"""

import asyncio
import os
import tempfile
import time
from datetime import datetime
from functools import wraps
from math import ceil
from typing import Callable, List, Union

import pyrogram
from loguru import logger
from pyrogram import types
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from ruamel import yaml

import utils
from module.app import (
    Application,
    ChatDownloadConfig,
    ForwardStatus,
    QueryHandler,
    QueryHandlerStr,
    TaskNode,
    TaskType,
    UploadStatus,
)
from module.filter import Filter
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import Language, _t, get_language, use_language
from module.pyrogram_extension import (
    check_user_permission,
    get_utf16_length,
    parse_link,
    proc_cache_forward,
    report_bot_forward_status,
    report_bot_status,
    retry,
    set_meta_data,
    upload_telegram_chat_message,
)
from utils.format import replace_date_time, validate_title
from utils.meta_data import MetaData

# pylint: disable = C0301, R0902


COMMAND_DEFINITIONS = (
    ("help", "Help"),
    ("get_info", "Get group and user info from message link"),
    ("download", "Download messages"),
    ("forward", "Forward messages"),
    ("listen_forward", "Listen for forwarded messages"),
    ("forward_to_comments", "Forward a specific media to a comment section"),
    ("add_filter", "Add download filter"),
    ("set_language", "Set language"),
    ("stop", "Stop bot download or forward"),
    ("queue", "Manage persistent download queue"),
)


def build_bot_commands():
    """Build menus and help from the same translated command registry."""
    return [
        types.BotCommand(command, _t(description))
        for command, description in COMMAND_DEFINITIONS
    ]


def localized_handler(callback):
    """Scope message/callback translations to the submitting user's preference."""

    @wraps(callback)
    async def inner(client, update):
        user_id = getattr(getattr(update, "from_user", None), "id", None)
        with use_language(_bot.user_language(user_id)):
            return await callback(client, update)

    return inner


class DownloadBot:
    """Download bot"""

    def __init__(self):
        self.bot = None
        self.client = None
        self.add_download_task: Callable = None
        self.download_chat_task: Callable = None
        self.app = None
        self.listen_forward_chat: dict = {}
        self.config: dict = {}
        self._yaml = yaml.YAML()
        self.config_path = os.path.join(os.path.abspath("."), "bot.yaml")
        self.download_command: dict = {}
        self.filter = Filter()
        self.bot_info = None
        self.task_node: dict = {}
        self.is_running = True
        self.allowed_user_ids: List[Union[int, str]] = []
        self.monitor_task = None

        meta = MetaData(datetime(2022, 8, 5, 14, 35, 12), 0, "", 0, 0, 0, "", 0)
        self.filter.set_meta_data(meta)

        self.download_filter: List[str] = []
        self.task_id: int = 0
        self.reply_task = None
        self.user_download_filters: dict = {}
        self.user_languages: dict = {}
        self.listen_forward_configs: dict = {}
        self.admin_id = None

    def gen_task_id(self) -> int:
        """Generate restart-safe group IDs rather than reusing small counters."""
        self.task_id = max(self.task_id + 1, time.time_ns())
        return self.task_id

    def user_language(self, user_id) -> Language:
        """Read a user's preference without changing the process-wide default."""
        name = self.user_languages.get(str(user_id))
        return Language.__members__.get(
            str(name).upper(), getattr(self.app, "language", Language.EN)
        )

    def save_listener(self, node: TaskNode, cursor=None, enabled=True):
        """Atomically persist a subscription after its messages enter SQLite.

        An interrupted save may replay messages, but enqueue keys make replay
        harmless. Advancing a cursor before enqueue would permanently lose work.
        """
        from module.durable_queue import node_payload

        key = str(node.chat_id)
        previous = self.listen_forward_configs.get(key)
        payload = node_payload(node)
        payload["use_bot_upload"] = node.upload_user is self.bot
        record = {
            "enabled": enabled,
            "cursor_initialized": True,
            "last_message_id": int(
                cursor
                if cursor is not None
                else (previous or {}).get("last_message_id", 0)
            ),
            "payload": payload,
        }
        self.listen_forward_configs[key] = record
        try:
            self.update_config()
        except Exception:
            if previous is None:
                self.listen_forward_configs.pop(key, None)
            else:
                self.listen_forward_configs[key] = previous
            raise
        node.last_message_id = record["last_message_id"]

    def restore_listeners(self):
        """Recreate enabled listeners using runtime clients, never serialized clients."""
        service = getattr(self.app, "download_service", None)
        if service is None:
            return
        for record in self.listen_forward_configs.values():
            if not record.get("enabled", True):
                continue
            try:
                payload = record["payload"]
                owner = payload.get("from_user_id")
                if owner not in self.allowed_user_ids:
                    logger.warning(
                        "Skipping listener whose owner is no longer allowed: {}", owner
                    )
                    continue
                node = service.restore_node(payload)
                node.upload_user = (
                    self.bot if payload.get("use_bot_upload") else self.client
                )
                node.upload_as_bot = bool(payload.get("use_bot_upload"))
                node.last_message_id = int(record.get("last_message_id", 0))
                node.is_running = True
                self.listen_forward_chat[node.chat_id] = node
                self.add_task_node(node)
            except Exception as error:
                # Preserve invalid/unreachable subscriptions for diagnosis and retry.
                logger.warning("Unable to restore listener: {}", error)
        if self.listen_forward_chat and self.monitor_task is None:
            self.monitor_task = self.app.loop.create_task(start_message_monitor())

    def add_task_node(self, node: TaskNode):
        """Add task node"""
        self.task_node[node.task_id] = node

    def remove_task_node(self, task_id: int):
        """Remove task node"""
        self.task_node.pop(task_id, None)

    def stop_task(
        self, task_id: str, task_type=None, owner_id=None, cancel_persisted=True
    ):
        """Stop only the selected task type and owner; shutdown may omit both."""
        for key, value in self.task_node.copy().items():
            if task_id != "all" and str(key) != str(task_id):
                continue
            if task_type is not None and value.task_type is not task_type:
                continue
            if owner_id is not None and value.from_user_id != owner_id:
                continue
            if (
                cancel_persisted
                and value.task_type is TaskType.ListenForward
                and getattr(self.app, "download_service", None) is not None
            ):
                # Persist disabled before stopping, so a restart cannot revive it.
                self.save_listener(value, enabled=False)
            value.stop_transmission()
            store = getattr(self.app, "task_store", None)
            if store is not None and cancel_persisted:
                # The database is authoritative: queued children must be cancelled too.
                offset = 0
                while True:
                    rows = store.list_tasks(group_id=str(key), limit=100, offset=offset)
                    for row in rows:
                        if row["state"] not in ("completed", "cancelled", "failed"):
                            store.cancel(row["id"])
                    if len(rows) < 100:
                        break
                    offset += len(rows)

    async def update_reply_message(self):
        """Update reply message"""
        while self.is_running:
            for key, value in self.task_node.copy().items():
                if value.is_running:
                    try:
                        await report_bot_status(self.bot, value)
                    except pyrogram.errors.FloodWait as error:
                        await asyncio.sleep(error.value)
                    except Exception as error:
                        # One unavailable reply target must not stop all task updates.
                        logger.warning("Unable to update bot task {}: {}", key, error)

            for key, value in self.task_node.copy().items():
                if value.is_running and value.is_finish():
                    self.remove_task_node(key)
            await asyncio.sleep(3)

    def assign_config(self, _config: dict):
        """assign config from str.

        Parameters
        ----------
        _config: dict
            application config dict

        Returns
        -------
        bool
        """

        self.download_filter = _config.get("download_filter", self.download_filter)
        self.user_download_filters = _config.get("user_download_filters", {})
        self.user_languages = _config.get("user_languages", {})
        self.listen_forward_configs = _config.get("listen_forward_configs", {})

        return True

    def update_config(self):
        """Update config from str."""
        self.config["download_filter"] = self.download_filter
        self.config["user_download_filters"] = self.user_download_filters
        self.config["user_languages"] = self.user_languages
        self.config["listen_forward_configs"] = self.listen_forward_configs
        directory = os.path.dirname(os.path.abspath(self.config_path))
        os.makedirs(directory, exist_ok=True)
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=directory, delete=False
            ) as yaml_file:
                temporary_path = yaml_file.name
                self._yaml.dump(self.config, yaml_file)
                yaml_file.flush()
                os.fsync(yaml_file.fileno())
            os.replace(temporary_path, self.config_path)
        finally:
            if temporary_path and os.path.exists(temporary_path):
                os.unlink(temporary_path)

    async def start(
        self,
        app: Application,
        client: pyrogram.Client,
        add_download_task: Callable,
        download_chat_task: Callable,
    ):
        """Start bot"""
        self.bot = pyrogram.Client(
            app.application_name + "_bot",
            api_hash=app.api_hash,
            api_id=app.api_id,
            bot_token=app.bot_token,
            workdir=app.session_file_path,
            proxy=app.proxy,
        )

        self.app = app
        self.client = client
        self.add_download_task = add_download_task
        self.download_chat_task = download_chat_task

        # load config
        if os.path.exists(self.config_path):
            with open(self.config_path, encoding="utf-8") as f:
                config = self._yaml.load(f.read())
                if config:
                    self.config = config
                    self.assign_config(self.config)

        await self.bot.start()

        self.bot_info = await self.bot.get_me()

        for allowed_user_id in self.app.allowed_user_ids:
            try:
                chat = await self.client.get_chat(allowed_user_id)
                self.allowed_user_ids.append(chat.id)
            except Exception as e:
                logger.warning(f"set allowed_user_ids error: {e}")

        admin = await self.client.get_me()
        self.admin_id = admin.id
        self.allowed_user_ids.append(admin.id)
        self.restore_listeners()

        await self.bot.set_bot_commands(build_bot_commands())

        self.bot.add_handler(
            MessageHandler(
                localized_handler(download_from_bot),
                filters=pyrogram.filters.command(["download"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                localized_handler(forward_messages),
                filters=pyrogram.filters.command(["forward"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                localized_handler(download_forward_media),
                filters=pyrogram.filters.media
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                localized_handler(download_from_link),
                filters=pyrogram.filters.regex(r"^https://t.me.*")
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                localized_handler(set_listen_forward_msg),
                filters=pyrogram.filters.command(["listen_forward"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                localized_handler(help_command),
                filters=pyrogram.filters.command(["help"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                localized_handler(get_info),
                filters=pyrogram.filters.command(["get_info"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                localized_handler(help_command),
                filters=pyrogram.filters.command(["start"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                localized_handler(set_language),
                filters=pyrogram.filters.command(["set_language"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                localized_handler(add_filter),
                filters=pyrogram.filters.command(["add_filter"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )

        self.bot.add_handler(
            MessageHandler(
                localized_handler(stop),
                filters=pyrogram.filters.command(["stop"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                localized_handler(queue_command),
                filters=pyrogram.filters.command(["queue"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )

        self.bot.add_handler(
            CallbackQueryHandler(
                localized_handler(on_query_handler),
                filters=pyrogram.filters.user(self.allowed_user_ids),
            )
        )

        try:
            await send_help_str(self.bot, admin.id)
        except Exception:
            pass

        self.reply_task = _bot.app.loop.create_task(_bot.update_reply_message())

        self.bot.add_handler(
            MessageHandler(
                localized_handler(forward_to_comments),
                filters=pyrogram.filters.command(["forward_to_comments"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )


_bot = DownloadBot()


async def start_download_bot(
    app: Application,
    client: pyrogram.Client,
    add_download_task: Callable,
    download_chat_task: Callable,
):
    """Start download bot"""
    await _bot.start(app, client, add_download_task, download_chat_task)


async def stop_download_bot():
    """Stop download bot"""
    _bot.update_config()
    _bot.is_running = False
    if _bot.reply_task:
        _bot.reply_task.cancel()
    # Shutdown interrupts transfers but must preserve queued work for restart.
    _bot.stop_task("all", cancel_persisted=False)
    if _bot.bot:
        await _bot.bot.stop()
    if _bot.monitor_task:
        _bot.monitor_task.cancel()
        _bot.monitor_task = None


async def send_help_str(client: pyrogram.Client, chat_id):
    """
    Sends a help string to the specified chat ID using the provided client.

    Parameters:
        client (pyrogram.Client): The Pyrogram client used to send the message.
        chat_id: The ID of the chat to which the message will be sent.

    Returns:
        str: The help string that was sent.

    Note:
        The help string includes information about the Telegram Media Downloader bot,
        its version, and the available commands.
    """

    update_keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Github",
                    url="https://github.com/tangyoha/telegram_media_downloader/releases",
                ),
                InlineKeyboardButton(
                    "Join us", url="https://t.me/TeegramMediaDownload"
                ),
            ]
        ]
    )
    latest_release_str = ""
    # try:
    #     latest_release = get_latest_release(_bot.app.proxy)

    #     latest_release_str = (
    #         f"{_t('New Version')}: [{latest_release['name']}]({latest_release['html_url']})\an"
    #         if latest_release
    #         else ""
    #     )
    # except Exception:
    #     latest_release_str = ""

    command_help = "\n".join(
        f"/{command} - {_t(description)}"
        for command, description in COMMAND_DEFINITIONS
    )
    msg = (
        f"`\n🤖 {_t('Telegram Media Downloader')}\n"
        f"🌐 {_t('Version')}: {utils.__version__}`\n"
        f"{latest_release_str}\n"
        f"{_t('Available commands:')}\n"
        f"{command_help}\n\n"
        # f"/add_replace_ad - {_t('Add replace advertisement filter')}\n"
        # f"/remove_replace_ad - {_t('Remove replace advertisement filter')}\n"
        # f"/add_filter_ad - {_t('Add filter advertisement filter')}\n"
        # f"/remove_filter_ad - {_t('Remove filter advertisement filter')}\n\n"
        # f"/set_add_ad - {_t('Set add advertisement')}\n\n"
        f"{_t('**Note**: 1 means the start of the entire chat')},"
        f"{_t('0 means the end of the entire chat')}\n"
        f"`[` `]` {_t('means optional, not required')}\n"
    )

    await client.send_message(chat_id, msg, reply_markup=update_keyboard)


async def help_command(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Sends a message with the available commands and their usage.

    Parameters:
        client (pyrogram.Client): The client instance.
        message (pyrogram.types.Message): The message object.

    Returns:
        None
    """

    await send_help_str(client, message.chat.id)


async def set_language(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Set the language of the bot.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """

    if len((message.text or "").split()) != 2:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /set_language en/ru/zh/ua"),
        )
        return

    language = message.text.split()[1]

    try:
        language = Language[language.upper()]
        _bot.user_languages[str(message.from_user.id)] = language.name
        _bot.update_config()
        # Chat scope keeps one user's menu preference away from other users.
        # In groups a member scope avoids changing the whole group's menu.
        scope = types.BotCommandScopeChat(message.chat.id)
        if message.chat.id != message.from_user.id:
            scope = types.BotCommandScopeChatMember(
                message.chat.id, message.from_user.id
            )
        with use_language(language):
            try:
                await client.set_bot_commands(build_bot_commands(), scope=scope)
            except Exception as error:
                logger.warning("Language saved, menu update failed: {}", error)
            await client.send_message(
                message.chat.id, f"{_t('Language set to')} {language.name}"
            )
    except KeyError:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /set_language en/ru/zh/ua"),
        )


async def get_info(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Async function that retrieves information from a group message link.
    """

    msg = _t("Invalid command format. Please use /get_info group_message_link")

    args = message.text.split()
    if len(args) != 2:
        await client.send_message(
            message.from_user.id,
            msg,
        )
        return

    chat_id, message_id, _ = await parse_link(_bot.client, args[1])

    entity = None
    if chat_id:
        entity = await _bot.client.get_chat(chat_id)

    if entity:
        if message_id:
            _message = await retry(_bot.client.get_messages, args=(chat_id, message_id))
            if _message:
                meta_data = MetaData()
                set_meta_data(meta_data, _message)
                msg = (
                    f"`\n"
                    f"{_t('Group/Channel')}\n"
                    f"├─ {_t('id')}: {entity.id}\n"
                    f"├─ {_t('first name')}: {entity.first_name}\n"
                    f"├─ {_t('last name')}: {entity.last_name}\n"
                    f"└─ {_t('name')}: {entity.username}\n"
                    f"{_t('Message')}\n"
                )

                for key, value in meta_data.data().items():
                    if key == "send_name":
                        msg += f"└─ {key}: {value or None}\n"
                    else:
                        msg += f"├─ {key}: {value or None}\n"

                msg += "`"
    await client.send_message(
        message.from_user.id,
        msg,
    )


async def add_filter(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Set the download filter of the bot.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """

    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /add_filter your filter"),
        )
        return

    filter_str = replace_date_time(args[1])
    res, err = _bot.filter.check_filter(filter_str)
    if res:
        _bot.user_download_filters[str(message.from_user.id)] = args[1]
        _bot.update_config()
        await client.send_message(
            message.chat.id,
            f"{_t('Add download filter')} : {args[1]}\n仅用于你之后提交的下载任务。",
        )
    else:
        await client.send_message(
            message.from_user.id, f"{err}\n{_t('Check error, please add again!')}"
        )
    return


async def add_filter_advertisement_filter(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Set the download filter of the bot.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """

    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /add_ad filter"),
        )
        return

    filter_str = args[1]

    _bot.app.filter_advertisement_list.append(filter_str)
    await client.send_message(message.from_user.id, f"{_t('Add filter')} : {args[1]}")
    _bot.app.update_config(True)


async def remove_filter_advertisement_filter(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Add or remove advertisement filter
    """

    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /remove_ad filter"),
        )
        return

    filter_str = args[1]
    if filter_str in _bot.app.filter_advertisement_list:
        _bot.app.filter_advertisement_list.remove(filter_str)
        await client.send_message(
            message.from_user.id, f"{_t('Remove filter')} : {args[1]}"
        )

        _bot.app.update_config(True)
    else:
        await client.send_message(
            message.from_user.id, f"{_t('Filter')} : {args[1]} {_t('not exist')}"
        )


async def set_add_advertisement(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Add or remove advertisement filter
    """

    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /set_ad mesage_link advertisement"),
        )
        return

    mesage_link = args[1]
    advertisement_str = None if len(args) < 3 else args[2]

    try:
        chat_id, _, _ = await parse_link(_bot.client, mesage_link)
        _bot.app.group_add_advertisement[chat_id] = advertisement_str
        _bot.app.update_config(True)
        await client.send_message(
            message.from_user.id, f"{_t('Set advertisement')} : {advertisement_str}"
        )
    except Exception as e:
        await client.send_message(
            message.from_user.id, f"{_t('Parse link error')}: {e}"
        )
        return


class MessageProcessor:
    """Helper class for processing message captions and entities."""

    def __init__(self, raw_message, filter_str):
        self.raw_message = raw_message
        self.raw_caption = raw_message.caption
        self.filter_str = filter_str
        self.raw_filter_str = pyrogram.parser.utils.add_surrogates(filter_str)
        self.raw_caption_str = pyrogram.parser.utils.add_surrogates(raw_message.caption)
        self.idx = self.raw_caption_str.find(self.raw_filter_str)
        self.start_offset = self.idx
        self.end_offset = self.idx + get_utf16_length(filter_str)
        self.filtered_entities = []

    # pylint: disable = R0916
    def process_entities(self):
        """Process and filter message entities."""
        for entity in self.raw_message.caption_entities:
            cur_start_offset = entity.offset
            cur_end_offset = entity.offset + entity.length

            # Check if entity should be included
            if (
                (
                    cur_start_offset >= self.start_offset
                    and cur_end_offset <= self.end_offset
                )
                or (
                    cur_start_offset < self.start_offset
                    and cur_end_offset > self.start_offset
                )
                or (
                    cur_start_offset < self.end_offset
                    and cur_end_offset > self.end_offset
                )
            ):
                self.filtered_entities.append(entity)

        self.filtered_entities.sort(key=lambda x: x.offset)

    def get_total_span(self):
        """Calculate the total span for text extraction."""
        if self.filtered_entities:
            first_entity = self.filtered_entities[0]
            last_entity = self.filtered_entities[-1]
            return (
                min(self.start_offset, first_entity.offset),
                max(self.end_offset, last_entity.offset + last_entity.length),
            )
        return (self.start_offset, self.end_offset)

    def extract_text(self, total_span):
        """Extract and process text with adjusted entity offsets."""
        text = self.raw_caption[total_span[0] : total_span[1]]
        for entity in self.filtered_entities:
            entity.offset -= total_span[0]
        return pyrogram.parser.Parser.unparse(text, self.filtered_entities, True)


async def proc_replace_advertisement(mesage_link: str, filter_str: str):
    """
    Process and replace advertisement content in a message.

    This function takes a message link and a filter string, retrieves the message,
    and processes its caption by handling entities and filtering advertisement content.
    It preserves the formatting and entities while replacing the specified filter text.

    Args:
        mesage_link (str): The link to the Telegram message
        filter_str (str): The string to filter/replace in the message caption

    Returns:
        str: The processed caption with preserved formatting and entities

    Raises:
        Exception: If there are issues parsing the message link or accessing the message
    """
    chat_id, message_id, _ = await parse_link(_bot.client, mesage_link)
    raw_message = await retry(_bot.client.get_messages, args=(chat_id, message_id))

    processor = MessageProcessor(raw_message, filter_str)
    processor.process_entities()
    total_span = processor.get_total_span()
    return processor.extract_text(total_span)


async def add_replace_advertisement_filter(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Set the download filter of the bot.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """

    args = message.text.split(maxsplit=2)
    if len(args) != 3:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /add_replace_ad your filter"),
        )
        return

    mesage_link = args[1]
    filter_str = args[2]

    try:
        filter_str = await proc_replace_advertisement(mesage_link, filter_str)
        _bot.app.replace_advertisement_list.append(filter_str)
        _bot.app.update_config(True)
        await client.send_message(
            message.from_user.id, f"{_t('Add filter')} : {filter_str}"
        )
    except Exception as e:
        await client.send_message(
            message.from_user.id, f"{_t('Add filter')} : {filter_str}\n{e}"
        )
        return


async def remove_replace_advertisement_filter(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Set the download filter of the bot.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """

    args = message.text.split(maxsplit=2)
    if len(args) != 3:
        await client.send_message(
            message.from_user.id,
            _t(
                "Invalid command format. Please use /remove_replace_ad mesage_link advertisement_filter"
            ),
        )
        return

    mesage_link = args[1]
    filter_str = args[2]

    try:
        filter_str = await proc_replace_advertisement(mesage_link, filter_str)

        if filter_str in _bot.app.replace_advertisement_list:
            _bot.app.replace_advertisement_list.remove(filter_str)
            await client.send_message(
                message.from_user.id, f"{_t('Remove filter')} : {filter_str}"
            )
        else:
            _bot.app.replace_advertisement_list.append(filter_str)
            await client.send_message()
        _bot.app.update_config(True)
    except Exception as e:
        await client.send_message(
            message.from_user.id, f"{_t('Add filter')} : {filter_str}\n{e}"
        )
        return


async def direct_download(
    download_bot: DownloadBot,
    chat_id: Union[str, int],
    message: pyrogram.types.Message,
    download_message: pyrogram.types.Message,
    client: pyrogram.Client = None,
):
    """Direct Download"""

    default_filter = download_bot.user_download_filters.get(str(message.from_user.id))
    if default_filter:
        metadata = MetaData()
        set_meta_data(metadata, download_message)
        download_bot.filter.set_meta_data(metadata)
        if not download_bot.filter.exec(replace_date_time(default_filter)):
            await download_bot.bot.send_message(message.chat.id, "此消息不符合你的默认过滤器，已跳过。")
            return
    replay_message = "已接收下载任务，正在加入持久化队列。使用 /queue 查看或调整任务。"
    last_reply_message = await download_bot.bot.send_message(
        message.chat.id, replay_message, reply_to_message_id=message.id
    )

    node = TaskNode(
        chat_id=chat_id,
        from_user_id=message.from_user.id,
        reply_message_id=last_reply_message.id,
        replay_message=replay_message,
        limit=1,
        bot=download_bot.bot,
        task_id=_bot.gen_task_id(),
    )

    node.reply_chat_id = message.chat.id
    node.language = get_language().name
    node.client = client

    _bot.add_task_node(node)

    await _bot.add_download_task(
        download_message,
        node,
    )

    node.is_running = True


async def download_forward_media(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Downloads the media from a forwarded message.

    Parameters:
        client (pyrogram.Client): The client instance.
        message (pyrogram.types.Message): The message object.

    Returns:
        None
    """

    if message.media and getattr(message, message.media.value):
        await direct_download(_bot, message.from_user.id, message, message, client)
        return

    await client.send_message(
        message.from_user.id,
        f"1. {_t('Direct download, directly forward the message to your robot')}\n\n",
        parse_mode=pyrogram.enums.ParseMode.HTML,
    )


async def download_from_link(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Downloads a single message from a Telegram link.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the Telegram link.

    Returns:
        None
    """

    if not message.text or not message.text.startswith("https://t.me"):
        return

    msg = (
        f"1. {_t('Directly download a single message')}\n"
        "<i>https://t.me/12000000/1</i>\n\n"
    )

    text = message.text.split()
    if len(text) != 1:
        await client.send_message(
            message.from_user.id, msg, parse_mode=pyrogram.enums.ParseMode.HTML
        )
        return

    chat_id, message_id, _ = await parse_link(_bot.client, text[0])

    entity = None
    if chat_id:
        entity = await _bot.client.get_chat(chat_id)
    if entity:
        if message_id:
            download_message = await retry(
                _bot.client.get_messages, args=(chat_id, message_id)
            )
            if download_message:
                await direct_download(_bot, entity.id, message, download_message)
            else:
                await client.send_message(
                    message.from_user.id,
                    f"{_t('From')} {entity.title} {_t('download')} {message_id} {_t('error')}!",
                    reply_to_message_id=message.id,
                )
        return

    await client.send_message(
        message.from_user.id, msg, parse_mode=pyrogram.enums.ParseMode.HTML
    )


# pylint: disable = R0912, R0915,R0914


async def download_from_bot(client: pyrogram.Client, message: pyrogram.types.Message):
    """Download from bot"""

    msg = (
        f"{_t('Parameter error, please enter according to the reference format')}:\n\n"
        f"1. {_t('Download all messages of common group')}\n"
        "<i>/download https://t.me/fkdhlg 1 0</i>\n\n"
        f"{_t('The private group (channel) link is a random group message link')}\n\n"
        f"2. {_t('The download starts from the N message to the end of the M message')}. "
        f"{_t('When M is 0, it means the last message. The filter is optional')}\n"
        f"<i>/download https://t.me/12000000 N M [filter]</i>\n\n"
    )

    args = (message.text or "").split(maxsplit=4)
    if not message.text or len(args) < 4:
        await client.send_message(
            message.from_user.id, msg, parse_mode=pyrogram.enums.ParseMode.HTML
        )
        return

    url = args[1]
    try:
        start_offset_id = int(args[2])
        end_offset_id = int(args[3])
    except Exception:
        await client.send_message(
            message.from_user.id, msg, parse_mode=pyrogram.enums.ParseMode.HTML
        )
        return

    if (
        start_offset_id < 1
        or end_offset_id < 0
        or (end_offset_id and end_offset_id < start_offset_id)
    ):
        await client.send_message(message.chat.id, "消息范围无效：起点必须 ≥ 1，终点为 0 或 ≥ 起点。")
        return
    limit = 0
    if end_offset_id:
        limit = end_offset_id - start_offset_id + 1

    download_filter = (
        args[4]
        if len(args) > 4
        else _bot.user_download_filters.get(str(message.from_user.id))
    )

    if download_filter:
        download_filter = replace_date_time(download_filter)
        res, err = _bot.filter.check_filter(download_filter)
        if not res:
            await client.send_message(
                message.from_user.id, err, reply_to_message_id=message.id
            )
            return
    try:
        entity = None
        chat_id, _, _ = await parse_link(_bot.client, url)
        if chat_id:
            entity = await _bot.client.get_chat(chat_id)
        if entity:
            chat_title = entity.title
            reply_message = f"已接收任务 · 来源：{chat_title}\n"
            chat_download_config = ChatDownloadConfig()
            chat_download_config.last_read_message_id = start_offset_id
            chat_download_config.download_filter = download_filter
            reply_message += (
                f"消息范围：{start_offset_id}—{end_offset_id or '最新'}\n使用 /queue 查看和控制任务。"
            )
            last_reply_message = await client.send_message(
                message.chat.id, reply_message, reply_to_message_id=message.id
            )
            node = TaskNode(
                chat_id=entity.id,
                from_user_id=message.from_user.id,
                reply_message_id=last_reply_message.id,
                replay_message=reply_message,
                limit=limit,
                start_offset_id=start_offset_id,
                end_offset_id=end_offset_id,
                bot=_bot.bot,
                task_id=_bot.gen_task_id(),
            )
            node.reply_chat_id = message.chat.id
            node.language = get_language().name
            _bot.add_task_node(node)
            if getattr(_bot.app, "download_service", None) is not None:
                # Acknowledge completion of submission only after the scan is durable.
                await _bot.download_chat_task(_bot.client, chat_download_config, node)
            else:
                _bot.app.loop.create_task(
                    _bot.download_chat_task(_bot.client, chat_download_config, node)
                )
        else:
            await client.send_message(
                message.chat.id, "无法识别来源，请提供有效的 Telegram 群组或频道链接。"
            )
    except Exception as e:
        await client.send_message(
            message.from_user.id,
            f"{_t('chat input error, please enter the channel or group link')}\n\n"
            f"{_t('Error type')}: {e.__class__}"
            f"{_t('Exception message')}: {e}",
        )
        return


async def get_forward_task_node(
    client: pyrogram.Client,
    message: pyrogram.types.Message,
    task_type: TaskType,
    src_chat_link: str,
    dst_chat_link: str,
    offset_id: int = 0,
    end_offset_id: int = 0,
    download_filter: str = None,
    reply_comment: bool = False,
):
    """Get task node"""
    limit: int = 0

    if end_offset_id:
        if end_offset_id < offset_id:
            await client.send_message(
                message.from_user.id,
                f" end_offset_id({end_offset_id}) < start_offset_id({offset_id}),"
                f" end_offset_id{_t('must be greater than')} offset_id",
            )
            return None

        limit = end_offset_id - offset_id + 1

    src_chat_id, _, _ = await parse_link(_bot.client, src_chat_link)
    dst_chat_id, target_msg_id, topic_id = await parse_link(_bot.client, dst_chat_link)

    if not src_chat_id or not dst_chat_id:
        logger.info(f"{src_chat_id} {dst_chat_id}")
        await client.send_message(
            message.from_user.id,
            _t("Invalid chat link") + f"{src_chat_id} {dst_chat_id}",
            reply_to_message_id=message.id,
        )
        return None

    try:
        src_chat = await _bot.client.get_chat(src_chat_id)
        dst_chat = await _bot.client.get_chat(dst_chat_id)
    except Exception as e:
        await client.send_message(
            message.from_user.id,
            f"{_t('Invalid chat link')} {e}",
            reply_to_message_id=message.id,
        )
        logger.exception(f"get chat error: {e}")
        return None

    me = await client.get_me()
    if dst_chat.id == me.id:
        # TODO: when bot receive message judge if download
        await client.send_message(
            message.from_user.id,
            _t("Cannot be forwarded to this bot, will cause an infinite loop"),
            reply_to_message_id=message.id,
        )
        return None

    if download_filter:
        download_filter = replace_date_time(download_filter)
        res, err = _bot.filter.check_filter(download_filter)
        if not res:
            await client.send_message(
                message.from_user.id, err, reply_to_message_id=message.id
            )

    last_reply_message = await client.send_message(
        message.chat.id,
        "Forwarding message, please wait...",
        reply_to_message_id=message.id,
    )

    node = TaskNode(
        chat_id=src_chat.id,
        from_user_id=message.from_user.id,
        upload_telegram_chat_id=dst_chat_id,
        reply_message_id=last_reply_message.id,
        replay_message=last_reply_message.text,
        has_protected_content=src_chat.has_protected_content,
        download_filter=download_filter,
        limit=limit,
        start_offset_id=offset_id,
        end_offset_id=end_offset_id,
        bot=_bot.bot,
        task_id=_bot.gen_task_id(),
        task_type=task_type,
        topic_id=topic_id,
    )

    node.reply_chat_id = message.chat.id
    node.language = get_language().name
    if target_msg_id and reply_comment:
        node.reply_to_message = await _bot.client.get_discussion_message(
            dst_chat_id, target_msg_id
        )

    _bot.add_task_node(node)

    node.upload_user = _bot.client
    if not dst_chat.type is pyrogram.enums.ChatType.BOT:
        has_permission = await check_user_permission(_bot.client, me.id, dst_chat.id)
        if has_permission:
            node.upload_user = _bot.bot

    if node.upload_user is _bot.client:
        await client.edit_message_text(
            message.from_user.id,
            last_reply_message.id,
            "Note that the robot may not be in the target group,"
            " use the user account to forward",
        )

    node.upload_as_bot = node.upload_user is _bot.bot

    return node


# pylint: disable = R0914
async def forward_message_impl(
    client: pyrogram.Client, message: pyrogram.types.Message, reply_comment: bool
):
    """
    Forward message
    """

    async def report_error(client: pyrogram.Client, message: pyrogram.types.Message):
        """Report error"""

        await client.send_message(
            message.from_user.id,
            f"{_t('Invalid command format')}."
            f"{_t('Please use')} "
            "/forward https://t.me/c/src_chat https://t.me/c/dst_chat "
            f"1 400 `[`{_t('Filter')}`]`\n",
        )

    args = message.text.split(maxsplit=5)
    if len(args) < 5:
        await report_error(client, message)
        return

    src_chat_link = args[1]
    dst_chat_link = args[2]

    try:
        offset_id = int(args[3])
        end_offset_id = int(args[4])
    except Exception:
        await report_error(client, message)
        return

    download_filter = args[5] if len(args) > 5 else None

    node = await get_forward_task_node(
        client,
        message,
        TaskType.Forward,
        src_chat_link,
        dst_chat_link,
        offset_id,
        end_offset_id,
        download_filter,
        reply_comment,
    )

    if not node:
        return

    if getattr(_bot.app, "download_service", None) is not None:
        await forward_msg(node, offset_id)
    elif not node.has_protected_content:
        try:
            async for item in get_chat_history_v2(  # type: ignore
                _bot.client,
                node.chat_id,
                limit=node.limit,
                max_id=node.end_offset_id,
                offset_id=offset_id,
                reverse=True,
            ):
                await forward_normal_content(client, node, item)
                if node.is_stop_transmission:
                    await client.edit_message_text(
                        message.from_user.id,
                        node.reply_message_id,
                        f"{_t('Stop Forward')}",
                    )
                    break
        except Exception as e:
            await client.edit_message_text(
                message.from_user.id,
                node.reply_message_id,
                f"{_t('Error forwarding message')} {e}",
            )
        finally:
            await report_bot_status(client, node, immediate_reply=True)
            node.stop_transmission()
    else:
        await forward_msg(node, offset_id)


async def forward_messages(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Forwards messages from one chat to another.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """
    return await forward_message_impl(client, message, False)


async def forward_normal_content(
    client: pyrogram.Client, node: TaskNode, message: pyrogram.types.Message
):
    """Forward normal content"""
    forward_ret = ForwardStatus.FailedForward
    caption = message.caption
    if caption:
        caption = validate_title(caption)
        _bot.app.set_caption_name(node.chat_id, message.media_group_id, caption)
    else:
        caption = _bot.app.get_caption_name(node.chat_id, message.media_group_id)

    if caption and _bot.app.is_match_advertisement(caption):
        forward_ret = ForwardStatus.SkipForward
        if message.media_group_id:
            # TODO
            node.upload_status[message.id] = UploadStatus.SkipUpload
        return

    if node.download_filter:
        meta_data = MetaData()
        set_meta_data(meta_data, message, caption)
        _bot.filter.set_meta_data(meta_data)
        if not _bot.filter.exec(node.download_filter):
            forward_ret = ForwardStatus.SkipForward
            if message.media_group_id:
                node.upload_status[message.id] = UploadStatus.SkipUpload
                await proc_cache_forward(_bot.client, node, message, False, _bot.app)
            await report_bot_forward_status(client, node, forward_ret)
            return

    await upload_telegram_chat_message(
        _bot.client, node.upload_user, _bot.app, node, message
    )


async def forward_msg(node: TaskNode, message_id: int):
    """Forward normal message"""

    chat_download_config = ChatDownloadConfig()
    chat_download_config.last_read_message_id = message_id
    chat_download_config.download_filter = node.download_filter  # type: ignore

    await _bot.download_chat_task(_bot.client, chat_download_config, node)


async def check_new_messages(
    client: pyrogram.Client, chat_id: int, node: TaskNode, last_message_id: int = 0
):
    """
    Checks for new messages in the chat and forwards them.

    Parameters:
        client (pyrogram.Client): The pyrogram client
        chat_id (int): The chat ID to monitor
        node (TaskNode): The task node containing forwarding configuration
        last_message_id (int): The ID of the last processed message
    """
    try:
        durable = getattr(_bot.app, "download_service", None) is not None
        # Only get the most recent message if last_message_id is 0
        initialized = durable and _bot.listen_forward_configs.get(str(chat_id), {}).get(
            "cursor_initialized", False
        )
        if last_message_id == 0 and not initialized:
            async for message in get_chat_history_v2(  # type: ignore
                client, chat_id, limit=1  # Get only the latest message
            ):
                if durable:
                    _bot.save_listener(node, cursor=message.id)
                return message.id

        # Otherwise check for new messages after last_message_id
        async for message in get_chat_history_v2(  # type: ignore
            client, chat_id, limit=100, offset_id=last_message_id, reverse=True
        ):
            if node.is_stop_transmission or not node.is_running:
                break
            if message.id > last_message_id:
                if durable:
                    # Commit each message before its cursor: replay is safe, loss is not.
                    accepted = await _bot.add_download_task(message, node)
                    if not accepted:
                        raise RuntimeError("Unable to persist listener message")
                    _bot.save_listener(node, cursor=message.id)
                elif not node.has_protected_content:
                    await forward_normal_content(client, node, message)
                    await report_bot_status(client, node, immediate_reply=True)
                else:
                    await _bot.add_download_task(message, node)
                last_message_id = message.id
    except Exception as e:
        logger.exception(f"Error checking new messages in chat {chat_id}: {e}")

    return last_message_id


async def start_message_monitor():
    """
    Starts monitoring all chats that need to be forwarded.
    Runs every 60 seconds to check for new messages.
    """
    last_message_ids = (
        {}
    )  # Legacy mode; durable subscriptions keep their cursor in YAML.

    while _bot.is_running:
        try:
            for chat_id, node in list(_bot.listen_forward_chat.items()):
                if not node.is_running or node.is_stop_transmission:
                    continue

                durable = getattr(_bot.app, "download_service", None) is not None
                last_id = (
                    getattr(node, "last_message_id", 0)
                    if durable
                    else last_message_ids.get(chat_id, 0)
                )
                with use_language(getattr(node, "language", None)):
                    new_last_id = await check_new_messages(
                        _bot.client, chat_id, node, last_id
                    )
                last_message_ids[chat_id] = new_last_id

        except Exception as e:
            logger.exception(f"Error in message monitor: {e}")

        await asyncio.sleep(60)  # 每60秒检查一次


async def set_listen_forward_msg(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Set the chat to listen for forwarded messages.
    """
    args = message.text.split(maxsplit=3)

    if len(args) < 3:
        await client.send_message(
            message.from_user.id,
            f"{_t('Invalid command format')}. {_t('Please use')} /listen_forward "
            f"https://t.me/c/src_chat https://t.me/c/dst_chat [{_t('Filter')}]\n",
        )
        return

    src_chat_link = args[1]
    dst_chat_link = args[2]
    download_filter = args[3] if len(args) > 3 else None

    node = await get_forward_task_node(
        client,
        message,
        TaskType.ListenForward,
        src_chat_link,
        dst_chat_link,
        download_filter=download_filter,
    )

    if not node:
        return

    if getattr(_bot.app, "download_service", None) is not None:
        # Persist the new subscription before exposing it to the monitor.
        previous = _bot.listen_forward_chat.get(node.chat_id)
        if (
            previous is not None
            and previous.from_user_id != message.from_user.id
            and message.from_user.id != _bot.admin_id
        ):
            _bot.remove_task_node(node.task_id)
            await client.send_message(message.chat.id, "该来源已有其他用户的监听任务，请联系管理员。")
            return
        try:
            # Snapshot the baseline now. A later network outage must not cause
            # the first successful monitor pass to skip newly arrived messages.
            cursor = 0
            async for latest in get_chat_history_v2(_bot.client, node.chat_id, limit=1):
                cursor = latest.id
                break
            _bot.save_listener(node, cursor=cursor)
        except Exception:
            _bot.remove_task_node(node.task_id)
            raise
        if previous is not None:
            previous.stop_transmission()

    if node.chat_id in _bot.listen_forward_chat:
        _bot.remove_task_node(_bot.listen_forward_chat[node.chat_id].task_id)

    node.is_running = True
    _bot.listen_forward_chat[node.chat_id] = node

    if not hasattr(_bot, "monitor_task") or _bot.monitor_task is None:
        _bot.monitor_task = _bot.app.loop.create_task(start_message_monitor())


async def stop(client: pyrogram.Client, message: pyrogram.types.Message):
    """Stops listening for forwarded messages."""

    await client.send_message(
        message.chat.id,
        _t("Please select:"),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        _t("Stop Download"), callback_data="stop_download"
                    ),
                    InlineKeyboardButton(
                        _t("Stop Forward"), callback_data="stop_forward"
                    ),
                ],
                [  # Second row
                    InlineKeyboardButton(
                        _t("Stop Listen Forward"), callback_data="stop_listen_forward"
                    )
                ],
            ]
        ),
    )


QUEUE_STATES = {
    "queued": "Queued",
    "running": "Running",
    "retry_wait": "Waiting to retry",
    "paused": "Paused",
    "cancelled": "Cancelled",
    "completed": "Completed",
    "failed": "Failed",
}
QUEUE_ACTIONS = {
    "pause": "Pause",
    "resume": "Resume",
    "cancel": "Cancel",
    "priority": "Prioritize",
    "retry": "Retry",
}


def can_manage_queue_task(row: dict, user_id: int) -> bool:
    """A user owns submitted downloads; only the account owner manages scans."""
    payload = row.get("payload", {})
    owner = payload.get("from_user_id", payload.get("node", {}).get("from_user_id"))
    return user_id == _bot.admin_id or (
        owner is not None and str(owner) == str(user_id)
    )


def queue_task_text(row: dict) -> str:
    """Render a bounded plain-text summary without interpreting remote filenames."""
    payload = row.get("payload", {})
    name = (
        payload.get("file_name")
        or payload.get("chat_title")
        or str(payload.get("chat_id", "下载"))
    )
    name = " ".join(str(name).split())[:70]
    total = row.get("total_bytes") or 0
    downloaded = row.get("downloaded_bytes") or 0
    progress = f" · {min(100, downloaded * 100 / total):.0f}%" if total else ""
    detail = f"#{row['id']} · {_t(QUEUE_STATES.get(row['state'], row['state']))}{progress}\n{name}"
    phase = (
        row.get("stage")
        or row.get("phase")
        or payload.get("phase")
        or (row.get("result") or {}).get("phase")
    )
    if payload.get("kind") == "scan":
        phase = "scanning"
    phase_label = {
        "scanning": "Scanning",
        "downloading": "Downloading file",
        "verifying": "Verifying",
        "verified": "Verified",
        "uploading": "Uploading",
        "uploaded": "Uploaded",
    }.get(phase or "")
    if phase_label:
        detail += f"\n{_t(phase_label)}"
    if row.get("state") == "retry_wait":
        remaining = max(0, ceil((row.get("next_run") or 0) - time.time()))
        detail += f"\n{_t('Retry in seconds')}: {remaining} · {_t('Attempt')} {row.get('attempt', 0)}/{row.get('max_attempts', 3)}"
    if row.get("error"):
        detail += f"\n{_t('Reason')}: " + " ".join(str(row["error"]).split())[:120]
    return detail


def queue_task_buttons(row: dict):
    """Show actions that make sense for the persisted task's current state."""
    state, task_id = row["state"], row["id"]
    actions: List[str] = []
    if state in ("queued", "running", "retry_wait"):
        actions.extend(("pause", "priority", "cancel"))
    elif state == "paused":
        actions.extend(("resume", "priority", "cancel"))
    elif state in ("failed", "cancelled"):
        actions.append("retry")
    buttons = [
        InlineKeyboardButton(
            _t(QUEUE_ACTIONS[action]), callback_data=f"queue:{action}:{task_id}"
        )
        for action in actions
    ]
    buttons.append(
        InlineKeyboardButton(_t("Refresh"), callback_data=f"queue:detail:{task_id}")
    )
    return InlineKeyboardMarkup([buttons])


async def queue_command(client: pyrogram.Client, message: pyrogram.types.Message):
    """List the caller's durable tasks or inspect/control a single task by ID."""
    store = getattr(_bot.app, "task_store", None)
    if store is None:
        await client.send_message(message.chat.id, "任务数据库尚未就绪，请稍后重试。")
        return
    args = (message.text or "").split()
    if len(args) == 1:
        owner_id = (
            None if message.from_user.id == _bot.admin_id else message.from_user.id
        )
        rows = store.list_tasks(limit=10, owner_id=owner_id)
        text = "持久化任务队列\n" + (
            "\n\n".join(queue_task_text(row) for row in rows) if rows else "暂无任务。"
        )
        text += "\n\n/queue 任务ID 查看详情\n/queue pause|resume|cancel|priority|retry 任务ID"
        buttons = [
            [
                InlineKeyboardButton(
                    f"查看 #{row['id']}", callback_data=f"queue:detail:{row['id']}"
                )
            ]
            for row in rows
        ]
        await client.send_message(
            message.chat.id,
            text,
            parse_mode=pyrogram.enums.ParseMode.DISABLED,
            reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
        )
        return
    try:
        if len(args) == 2:
            action, task_id = "detail", int(args[1])
        elif len(args) == 3 and args[1] in QUEUE_ACTIONS:
            action, task_id = args[1], int(args[2])
        else:
            raise ValueError("invalid queue command")
    except ValueError:
        await client.send_message(
            message.chat.id,
            "用法：/queue 或 /queue 任务ID，操作：/queue pause|resume|cancel|priority|retry 任务ID",
        )
        return
    row, error = apply_queue_action(store, task_id, action, message.from_user.id)
    if error:
        await client.send_message(message.chat.id, error)
        return
    await client.send_message(
        message.chat.id,
        queue_task_text(row),
        reply_markup=queue_task_buttons(row),
        parse_mode=pyrogram.enums.ParseMode.DISABLED,
    )


def apply_queue_action(store, task_id: int, action: str, user_id: int):
    """Check ownership before any mutation, including forged callback payloads."""
    row = store.get(task_id)
    if row is None or not can_manage_queue_task(row, user_id):
        return None, "任务不存在或你没有操作权限。"
    if action == "priority":
        # A larger priority moves a waiting task before ordinary queued work.
        store.set_priority(task_id, max(100, int(row.get("priority", 0)) + 100))
    elif action in ("resume", "retry"):
        store.resume(task_id)
    elif action in ("pause", "cancel"):
        getattr(store, action)(task_id)
    elif action != "detail":
        return None, "不支持的任务操作。"
    return store.get(task_id), None


async def queue_callback(client: pyrogram.Client, query: pyrogram.types.CallbackQuery):
    """Apply a queue button then redraw the same message to avoid chat flooding."""
    parts = (query.data or "").split(":")
    store = getattr(_bot.app, "task_store", None)
    if store is None or len(parts) != 3:
        return
    try:
        task_id = int(parts[2])
    except ValueError:
        return
    row, error = apply_queue_action(store, task_id, parts[1], query.from_user.id)
    if error:
        await client.send_message(query.message.chat.id, error)
        return
    try:
        await client.edit_message_text(
            query.message.chat.id,
            query.message.id,
            queue_task_text(row),
            reply_markup=queue_task_buttons(row),
            parse_mode=pyrogram.enums.ParseMode.DISABLED,
        )
    except pyrogram.errors.MessageNotModified:
        # Refreshing unchanged progress is expected and should not fail the callback.
        pass


async def stop_task(
    client: pyrogram.Client,
    query: pyrogram.types.CallbackQuery,
    queryHandler: str,
    task_type: TaskType,
):
    """Stop task"""
    if query.data == queryHandler:
        buttons: List[InlineKeyboardButton] = []
        temp_buttons: List[InlineKeyboardButton] = []
        for key, value in _bot.task_node.copy().items():
            if (
                not value.is_finish()
                and value.task_type is task_type
                and value.from_user_id == query.from_user.id
            ):
                if len(temp_buttons) == 3:
                    buttons.append(temp_buttons)
                    temp_buttons = []
                temp_buttons.append(
                    InlineKeyboardButton(
                        f"#{key} · {value.chat_id}",
                        callback_data=f"{queryHandler} task {key}",
                    )
                )
        if temp_buttons:
            buttons.append(temp_buttons)

        if buttons:
            buttons.insert(
                0,
                [
                    InlineKeyboardButton(
                        _t("all"), callback_data=f"{queryHandler} task all"
                    )
                ],
            )
            await client.edit_message_text(
                query.message.chat.id,
                query.message.id,
                f"{_t('Stop')} {_t(task_type.name)}...",
                reply_markup=InlineKeyboardMarkup(buttons),
            )
        else:
            await client.edit_message_text(
                query.message.chat.id,
                query.message.id,
                f"{_t('No Task')}",
            )
    else:
        parts = query.data.split()
        if len(parts) != 3 or parts[1] != "task":
            return
        task_id = parts[2]
        _bot.stop_task(task_id, task_type=task_type, owner_id=query.from_user.id)
        await client.edit_message_text(
            query.message.chat.id,
            query.message.id,
            f"已取消所选 {_t(task_type.name)} 任务。",
        )


async def on_query_handler(
    client: pyrogram.Client, query: pyrogram.types.CallbackQuery
):
    """
    Asynchronous function that handles query callbacks.

    Parameters:
        client (pyrogram.Client): The Pyrogram client object.
        query (pyrogram.types.CallbackQuery): The callback query object.

    Returns:
        None
    """

    await query.answer()
    if (query.data or "").startswith("queue:"):
        await queue_callback(client, query)
        return
    for it in QueryHandler:
        queryHandler = QueryHandlerStr.get_str(it.value)
        if query.data == queryHandler or (query.data or "").startswith(
            queryHandler + " "
        ):
            await stop_task(client, query, queryHandler, TaskType(it.value))
            return


async def forward_to_comments(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Forwards specified media to a designated comment section.

    Usage: /forward_to_comments <source_chat_link> <destination_chat_link> <msg_start_id> <msg_end_id>

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.
    """
    return await forward_message_impl(client, message, True)
