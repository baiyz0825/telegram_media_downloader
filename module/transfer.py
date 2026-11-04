"""Recoverable media transfers and explicit error policy.

Telegram does not expose a trustworthy remote MD5 for ordinary media. A file can
therefore be skipped before transfer only when a previously verified Telegram
identity maps to unchanged local content. Other files are compared after transfer.
"""

import asyncio
import errno
import hashlib
import os
import random
from dataclasses import dataclass
from functools import partial

CHUNK_SIZE = 1024 * 1024  # Pyrogram stream_media offsets count 1 MiB chunks.


class TaskInterrupted(Exception):
    """The operator paused/cancelled this claim; retain the partial file."""


class IncompleteTransfer(ConnectionError):
    """The stream ended before all expected bytes arrived."""


@dataclass
class RetryDecision:
    kind: str
    retry: bool
    delay: float = 0


def classify_error(error, attempt=1, base_delay=3, max_delay=300):
    """Separate transport, Telegram, storage and permanent input errors."""
    name = type(error).__name__
    if name in ("FloodWait", "SlowmodeWait"):
        return RetryDecision("rate_limit", True, max(1, float(error.value)))
    if name in ("FileReferenceExpired", "FileReferenceInvalid"):
        return RetryDecision("file_reference", True, 0)
    if name in (
        "ChannelPrivate",
        "ChatAdminRequired",
        "UserBannedInChannel",
        "Forbidden",
    ):
        return RetryDecision("permission", False)
    if name in ("MessageIdInvalid", "PeerIdInvalid", "MediaEmpty", "MessageEmpty"):
        return RetryDecision("not_found", False)
    if isinstance(error, OSError) and error.errno in (
        errno.ENOSPC,
        errno.EDQUOT,
        errno.EACCES,
        errno.EROFS,
        errno.ENOENT,
    ):
        return RetryDecision("storage", False)
    if (
        isinstance(error, (asyncio.TimeoutError, TimeoutError, ConnectionError))
        or (
            isinstance(error, OSError)
            and error.errno
            in (
                errno.ECONNRESET,
                errno.ECONNREFUSED,
                errno.ENETUNREACH,
                errno.EHOSTUNREACH,
                errno.ETIMEDOUT,
                errno.EPIPE,
            )
        )
        or name in ("InternalServerError", "ServiceUnavailable", "RPCError500")
    ):
        delay = min(max_delay, base_delay * 2 ** min(max(attempt - 1, 0), 12))
        return RetryDecision("network", True, delay * random.uniform(0.8, 1.2))
    if isinstance(error, (ValueError, TypeError)):
        return RetryDecision("invalid_input", False)
    return RetryDecision("unexpected", False)


def file_md5(path):
    """Hash in bounded memory; reject a file modified during the read."""
    before = os.stat(path)
    digest = hashlib.md5()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(CHUNK_SIZE), b""):
            digest.update(block)
    after = os.stat(path)
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise OSError("File changed while calculating MD5: %s" % path)
    return digest.hexdigest()


class FileIndex:
    """Read-only index of download/media roots; never delete external files."""

    def __init__(self, store, roots):
        self.store = store
        self.roots = [os.path.realpath(root) for root in roots]

    def allowed(self, path):
        path = os.path.realpath(path)
        for root in self.roots:
            try:
                if os.path.commonpath((root, path)) == root:
                    return True
            except ValueError:  # Different drive letters on Windows.
                continue
        return False

    def record(self, path, unique_id=None):
        stat = os.stat(path)
        digest = file_md5(path)
        self.store.upsert_file(path, stat.st_size, stat.st_mtime_ns, digest, unique_id)
        return digest

    def scan(self):
        """Reuse unchanged index entries; ignore symlinks and unfinished files."""
        count = 0
        for root in self.roots:
            for directory, dirs, files in os.walk(root, followlinks=False):
                dirs[:] = [
                    d for d in dirs if not os.path.islink(os.path.join(directory, d))
                ]
                for name in files:
                    path = os.path.join(directory, name)
                    if os.path.islink(path) or name.endswith((".part", ".tmp")):
                        continue
                    try:
                        stat = os.stat(path)
                        old = self.store.get_file(path)
                        if old and (old["size"], old["mtime_ns"]) == (
                            stat.st_size,
                            stat.st_mtime_ns,
                        ):
                            continue
                        self.record(path)
                        count += 1
                    except (OSError, ValueError):
                        # A concurrently removed/changed file is not a safe candidate.
                        continue
        return count

    def find(self, size, digest=None, unique_id=None):
        if digest is None and unique_id is None:
            return None
        for entry in self.store.find_files(size, md5=digest, file_unique_id=unique_id):
            path = entry["path"]
            try:
                if not self.allowed(path) or os.path.islink(path):
                    continue
                stat = os.stat(path)
                if stat.st_size != size:
                    continue
                # Even an unchanged size/mtime is not proof of byte equality.
                actual = file_md5(path)
                if actual != entry["md5"] or (digest and actual != digest):
                    continue
                return path
            except OSError:
                continue
        return None


async def stream_to_file(
    client, message, path, expected_size, progress, check_control, idle_timeout=60
):
    """Resume at a complete protocol chunk, fsync bytes before checkpointing.

    A torn final chunk is discarded on restart. Completed chunks remain on disk,
    so an interrupted transfer re-downloads at most the last partial MiB.
    The DB progress is informational: the actual file length is authoritative.
    """
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    size = os.path.getsize(path) if os.path.exists(path) else 0
    if expected_size and size > expected_size:
        raise ValueError("Partial file is larger than expected media")
    if expected_size and size == expected_size:
        await check_control()
        await progress(size, expected_size)
        return path
    offset = size // CHUNK_SIZE
    iterator = client.stream_media(message, offset=offset).__aiter__()
    with open(path, "r+b" if os.path.exists(path) else "w+b") as output:
        output.truncate(offset * CHUNK_SIZE)
        output.seek(offset * CHUNK_SIZE)
        written = offset * CHUNK_SIZE
        try:
            while True:
                await check_control()
                try:
                    block = await asyncio.wait_for(iterator.__anext__(), idle_timeout)
                except StopAsyncIteration:
                    break
                if not block:
                    raise IncompleteTransfer("Media stream returned an empty chunk")
                if expected_size and written + len(block) > expected_size:
                    raise ValueError("Media stream exceeds advertised size")
                # Do disk writes and fsync off-loop; otherwise a slow volume
                # stalls every queue consumer and web/bot request.
                await _write_chunk(output, block)
                written += len(block)
                await progress(written, expected_size or written)
        finally:
            close = getattr(iterator, "aclose", None)
            if close:
                await close()
    if expected_size and written != expected_size:
        raise IncompleteTransfer(
            "Expected %s bytes, received %s" % (expected_size, written)
        )
    return path


async def _write_chunk(output, block):
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(None, partial(_write_chunk_sync, output, block))
    try:
        await asyncio.shield(future)
    except asyncio.CancelledError:
        # A worker thread cannot be cancelled. Wait until its file handle use
        # ends before the enclosing context closes the handle.
        await asyncio.shield(future)
        raise


def _write_chunk_sync(output, block):
    output.write(block)
    output.flush()
    os.fsync(output.fileno())


def atomic_publish(source, target, return_owned=False):
    """Publish only verified content; target staging is on the target filesystem."""
    import shutil

    os.makedirs(os.path.dirname(os.path.abspath(target)), exist_ok=True)
    if os.path.exists(target):
        if file_md5(source) == file_md5(target):
            os.remove(source)
            return (target, False) if return_owned else target
        stem, suffix = os.path.splitext(target)
        target = "%s-%s%s" % (stem, file_md5(source)[:12], suffix)
        if os.path.exists(target):
            if file_md5(source) != file_md5(target):
                raise FileExistsError("Conflicting content at %s" % target)
            os.remove(source)
            return (target, False) if return_owned else target
    staging = target + ".part"
    with open(source, "rb") as src, open(staging, "wb") as dest:
        shutil.copyfileobj(src, dest, CHUNK_SIZE)
        dest.flush()
        os.fsync(dest.fileno())
    if file_md5(source) != file_md5(staging):
        raise OSError("MD5 mismatch while publishing file")
    os.replace(staging, target)
    os.remove(source)
    if os.name != "nt":
        descriptor = os.open(os.path.dirname(os.path.abspath(target)), os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return (target, True) if return_owned else target
