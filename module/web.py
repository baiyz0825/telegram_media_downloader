"""Authenticated web task console and backwards-compatible download endpoints."""

import hmac
import logging
import os
import secrets
import sys
import threading
from pathlib import Path

from flask import Flask, jsonify, render_template, request, session
from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user

import utils
from module.app import Application
from module.download_stat import (
    DownloadState,
    get_download_result,
    get_download_state,
    get_total_download_speed,
    set_download_state,
)
from utils.crypto import AesBase64
from utils.format import format_byte

logging.getLogger("werkzeug").setLevel(logging.ERROR)
_flask_app = Flask(__name__)
_flask_app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax")
_login_manager = LoginManager()
_login_manager.login_view = "login"
_login_manager.init_app(_flask_app)
web_login_users = {}
_application = None
deAesCrypt = AesBase64("1234123412ABCDEF", "ABCDEF1234123412")


class User(UserMixin):
    """The configured console administrator."""

    id = "root"


@_login_manager.user_loader
def load_user(user_id):
    return User() if user_id == "root" and "root" in web_login_users else None


@_login_manager.unauthorized_handler
def unauthorized():
    if request.path.startswith("/api/"):
        return jsonify(error="登录已过期，请重新登录"), 401
    from flask import redirect, url_for

    return redirect(url_for("login"))


def _csrf_token():
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_urlsafe(32)
    return session["csrf_token"]


@_flask_app.before_request
def check_csrf():
    """Protect every browser mutation, including login and legacy endpoints."""
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return None
    supplied = request.headers.get("X-CSRF-Token", "") or request.form.get(
        "csrf_token", ""
    )
    expected = session.get("csrf_token", "")
    if not expected or not hmac.compare_digest(str(supplied), str(expected)):
        return jsonify(error="页面凭证已失效，请刷新后重试"), 403
    return None


def get_flask_app() -> Flask:
    return _flask_app


def _load_session_secret(directory):
    """Persist separately from queue data so database resets keep login secure."""
    secret_path = Path(directory or ".") / ".web_session_secret"
    secret_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            str(secret_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
    except FileExistsError:
        secret = secret_path.read_text(encoding="utf-8").strip()
        if len(secret) < 32:
            raise ValueError(
                "Web session secret is invalid; replace .web_session_secret"
            )
        return secret
    secret = secrets.token_hex(32)
    with os.fdopen(descriptor, "w", encoding="utf-8") as secret_file:
        secret_file.write(secret)
    return secret


def run_web_server(app: Application):
    get_flask_app().run(
        app.web_host, app.web_port, debug=app.debug_web, use_reloader=False
    )


def init_web(app: Application):
    global web_login_users, _application
    _application = app
    _flask_app.secret_key = _load_session_secret(app.session_file_path)
    web_login_users = (
        {"root": str(app.web_login_secret)} if app.web_login_secret else {}
    )
    _flask_app.config["LOGIN_DISABLED"] = not bool(web_login_users)
    threading.Thread(target=run_web_server, args=(app,), daemon=True).start()


@_flask_app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html", csrf_token=_csrf_token())
    if request.is_json:
        password = (request.get_json(silent=True) or {}).get("password", "")
    else:
        try:
            password = deAesCrypt.decrypt(request.form.get("password", ""))
        except (ValueError, UnicodeError, IndexError):
            return jsonify(code="0", error="密码格式无效"), 400
    configured = web_login_users.get("root")
    if (
        configured
        and isinstance(password, str)
        and hmac.compare_digest(password, configured)
    ):
        session.clear()
        login_user(User())
        _csrf_token()
        return jsonify(code="1")
    return jsonify(code="0", error="密码不正确"), 401


@_flask_app.route("/logout", methods=["POST"])
@login_required
def logout():
    logout_user()
    session.clear()
    return jsonify(ok=True)


@_flask_app.route("/")
@login_required
def index():
    return render_template(
        "index.html",
        csrf_token=_csrf_token(),
        version=utils.__version__,
        login_enabled=bool(web_login_users),
    )


def _store():
    return getattr(_application, "task_store", None)


def _upload_speed():
    nodes = []
    if _application:
        nodes.extend(
            config.node for config in _application.chat_download_config.values()
        )
    bot_module = sys.modules.get("module.bot")
    if bot_module and getattr(bot_module, "_bot", None):
        nodes.extend(list(bot_module._bot.task_node.values()))
    seen = set()
    total = 0
    for node in nodes:
        if id(node) in seen:
            continue
        seen.add(id(node))
        for value in list(node.upload_stat_dict.values()):
            if value.upload_size < value.total_size:
                total += max(0, value.upload_speed)
    return total


@_flask_app.route("/get_download_status")
@login_required
def get_download_speed():
    return jsonify(
        download_speed=format_byte(get_total_download_speed()) + "/s",
        upload_speed=format_byte(_upload_speed()) + "/s",
    )


@_flask_app.route("/set_download_state", methods=["POST"])
@login_required
def web_set_download_state():
    state = request.args.get("state")
    if state not in {"pause", "continue"}:
        return jsonify(error="无效的下载状态"), 400
    _bulk_action("pause" if state == "pause" else "resume")
    return "continue" if state == "pause" else "pause"


@_flask_app.route("/get_app_version")
def get_app_version():
    return utils.__version__


@_flask_app.route("/get_download_list")
@login_required
def get_download_list():
    if request.args.get("already_down") is None:
        return jsonify([])
    completed = request.args.get("already_down") == "true"
    result = []
    for chat_id, messages in list(get_download_result().items()):
        for message_id, value in list(messages.items()):
            total = max(0, value["total_size"])
            finished = total > 0 and value["down_byte"] >= total
            if completed != finished:
                continue
            result.append(
                dict(
                    chat=str(chat_id),
                    id=str(message_id),
                    filename=os.path.basename(value["file_name"]),
                    total_size=format_byte(total),
                    download_progress=min(
                        100, round(value["down_byte"] / total * 100, 1)
                    )
                    if total
                    else 0,
                    download_speed=format_byte(max(0, value["download_speed"])) + "/s",
                    save_path=value["file_name"].replace("\\", "/"),
                )
            )
    return jsonify(result)


@_flask_app.route("/api/tasks")
@login_required
def task_list():
    store = _store()
    if store is None:
        return jsonify(error="任务队列尚未就绪"), 503
    try:
        page = max(1, int(request.args.get("page", 1)))
        limit = min(100, max(1, int(request.args.get("limit", 25))))
    except (TypeError, ValueError):
        return jsonify(error="分页参数无效"), 400
    state = request.args.get("state") or None
    if state not in {
        None,
        "queued",
        "running",
        "retry_wait",
        "paused",
        "cancelled",
        "completed",
        "failed",
    }:
        return jsonify(error="任务状态无效"), 400
    search = request.args.get("search", "").strip()[:200]
    rows = store.list_tasks(
        state=state, limit=limit, offset=(page - 1) * limit, search=search
    )
    total = store.count_tasks(state=state, search=search)
    return jsonify(items=rows, total=total, page=page, limit=limit)


@_flask_app.route("/api/stats")
@login_required
def task_stats():
    store = _store()
    if store is None:
        return jsonify(error="任务队列尚未就绪"), 503
    service = getattr(_application, "download_service", None)
    client = getattr(service, "client", None)
    recent_errors = []
    for state in ("retry_wait", "failed"):
        recent_errors.extend(
            row
            for row in store.list_tasks(state=state, limit=5)
            if row.get("error_kind") in {"network", "rate_limit", "timeout"}
        )
    recent_errors.sort(key=lambda row: row["updated_at"], reverse=True)
    latest = recent_errors[0] if recent_errors else None
    queue = store.stats()
    all_paused = queue.get("paused", 0) > 0 and not any(
        queue.get(state, 0) for state in ("queued", "running", "retry_wait")
    )
    return jsonify(
        queue=queue,
        download_speed=get_total_download_speed(),
        upload_speed=_upload_speed(),
        paused=all_paused,
        network={
            "connected": bool(client.is_connected) if client else None,
            "download_limit": getattr(_application, "max_download_task", None),
            "upload_limit": getattr(_application, "max_upload_task", None),
            "recent_error": latest["error"] if latest else None,
            "error_kind": latest["error_kind"] if latest else None,
            "next_retry": latest["next_run"]
            if latest and latest["state"] == "retry_wait"
            else None,
        },
    )


@_flask_app.route("/api/tasks/<int:task_id>/<action>", methods=["POST"])
@login_required
def task_action(task_id, action):
    store = _store()
    if store is None:
        return jsonify(error="任务队列尚未就绪"), 503
    if action not in {"pause", "resume", "cancel", "retry", "promote"}:
        return jsonify(error="未知操作"), 400
    if not store.get(task_id):
        return jsonify(error="任务不存在或已被清理"), 404
    if action == "promote":
        import time

        changed = store.set_priority(task_id, int(time.time() * 1000))
    else:
        changed = getattr(store, "resume" if action == "retry" else action)(task_id)
    if changed is False:
        return jsonify(error="当前任务状态不支持此操作，请刷新"), 409
    if action in {"resume", "retry"}:
        set_download_state(DownloadState.Downloading)
    return jsonify(ok=True, task=store.get(task_id))


def _bulk_action(action):
    store = _store()
    if store:
        getattr(store, "pause_all" if action == "pause" else "resume_all")()
        # Persisted task states control the queue; newly submitted tasks remain runnable.
        set_download_state(DownloadState.Downloading)
    else:
        set_download_state(
            DownloadState.StopDownload
            if action == "pause"
            else DownloadState.Downloading
        )


@_flask_app.route("/api/tasks/bulk/<action>", methods=["POST"])
@login_required
def bulk_action(action):
    if action not in {"pause", "resume"}:
        return jsonify(error="未知操作"), 400
    if _store() is None:
        return jsonify(error="任务队列尚未就绪"), 503
    _bulk_action(action)
    return jsonify(ok=True)
