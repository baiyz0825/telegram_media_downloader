"""provide upload cloud drive"""
import asyncio
import functools
import importlib
import inspect
import json
import os
import posixpath
import re
import tempfile
from asyncio import subprocess
from typing import Callable
from zipfile import ZipFile

from loguru import logger

from module.transfer import file_md5
from utils import platform


async def _in_thread(function, *args):
    """Run blocking archive/hash/provider work on Python 3.8 and newer."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(function, *args))


# pylint: disable = R0902
class CloudDriveConfig:
    """Rclone Config"""

    def __init__(
        self,
        enable_upload_file: bool = False,
        before_upload_file_zip: bool = False,
        after_upload_file_delete: bool = True,
        rclone_path: str = os.path.join(
            os.path.abspath("."), "rclone", f"rclone{platform.get_exe_ext()}"
        ),
        remote_dir: str = "",
        upload_adapter: str = "rclone",
    ):
        self.enable_upload_file = enable_upload_file
        self.before_upload_file_zip = before_upload_file_zip
        self.after_upload_file_delete = after_upload_file_delete
        self.rclone_path = rclone_path
        self.remote_dir = remote_dir
        self.upload_adapter = upload_adapter
        self.dir_cache: dict = {}  # for remote mkdir
        self.total_upload_success_file_count = 0
        self.last_upload_verified = False
        self.aligo = None

    def pre_run(self):
        """pre run init aligo"""
        if self.enable_upload_file and self.upload_adapter == "aligo":
            CloudDrive.init_upload_adapter(self)


class CloudDrive:
    """rclone support"""

    @staticmethod
    def init_upload_adapter(drive_config: CloudDriveConfig):
        """Initialize the upload adapter."""
        if drive_config.upload_adapter == "aligo":
            Aligo = importlib.import_module("aligo").Aligo
            drive_config.aligo = Aligo()

    @staticmethod
    async def rclone_mkdir(drive_config: CloudDriveConfig, remote_dir: str):
        """mkdir in remote"""
        code, _ = await CloudDrive._run_rclone(drive_config, "mkdir", remote_dir)
        if code:
            raise OSError("rclone mkdir failed with exit code %s" % code)

    @staticmethod
    async def _terminate(proc):
        if proc.returncode is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                return
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                await proc.wait()

    @staticmethod
    async def _run_rclone(
        drive_config, *args, progress_callback=None, progress_args=()
    ):
        """Arguments never pass through a shell; cancellation reaps the child."""
        proc = await asyncio.create_subprocess_exec(
            drive_config.rclone_path,
            *args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        output = bytearray()
        try:
            if proc.stdout:
                async for line in proc.stdout:
                    # Only single-file metadata and a small log tail are needed.
                    output.extend(line)
                    if len(output) > 1024 * 1024:
                        del output[: len(output) - 1024 * 1024]
                    if progress_callback:
                        match = re.search(
                            r"Transferred:\s*(.*?) / (.*?),\s*(.*?)%,\s*(.*?/s)?, ETA (.*?)$",
                            line.decode(errors="replace").strip(),
                        )
                        if match:
                            result = progress_callback(*match.groups(), *progress_args)
                            if inspect.isawaitable(result):
                                await result
            return await proc.wait(), bytes(output)
        except BaseException:
            await asyncio.shield(CloudDrive._terminate(proc))
            raise

    @staticmethod
    def aligo_mkdir(drive_config: CloudDriveConfig, remote_dir: str):
        """mkdir in remote by aligo"""
        if drive_config.aligo and not drive_config.aligo.get_folder_by_path(remote_dir):
            drive_config.aligo.create_folder(name=remote_dir, check_name_mode="refuse")

    @staticmethod
    def zip_file(local_file_path: str) -> str:
        """
        Zip local file
        """

        descriptor, zip_file_name = tempfile.mkstemp(
            prefix=".upload-",
            suffix=".zip",
            dir=os.path.dirname(os.path.abspath(local_file_path)),
        )
        os.close(descriptor)
        with ZipFile(zip_file_name, "w") as zip_writer:
            zip_writer.write(local_file_path, arcname=os.path.basename(local_file_path))

        return zip_file_name

    # pylint: disable = R0914
    @staticmethod
    async def rclone_upload_file(
        drive_config: CloudDriveConfig,
        save_path: str,
        local_file_path: str,
        progress_callback: Callable = None,
        progress_args: tuple = (),
    ) -> bool:
        """Use Rclone upload file"""
        drive_config.last_upload_verified = False
        try:
            relative = os.path.relpath(
                os.path.abspath(local_file_path), os.path.abspath(save_path)
            )
            if relative == ".." or relative.startswith(".." + os.sep):
                relative = os.path.basename(local_file_path)
            remote_file = (
                drive_config.remote_dir.rstrip("/") + "/" + relative.replace("\\", "/")
            )
            file_path = local_file_path
            if drive_config.before_upload_file_zip:
                file_path = await _in_thread(CloudDrive.zip_file, local_file_path)
                remote_file = os.path.splitext(remote_file)[0] + ".zip"
            remote_dir = posixpath.dirname(remote_file)
            if not drive_config.dir_cache.get(remote_dir):
                await CloudDrive.rclone_mkdir(drive_config, remote_dir)
                drive_config.dir_cache[remote_dir] = True
            before = os.stat(file_path)
            digest = await _in_thread(file_md5, file_path)
            # copyto targets exactly one object; no --ignore-existing shortcut.
            code, output = await CloudDrive._run_rclone(
                drive_config,
                "copyto",
                file_path,
                remote_file,
                "--checksum",
                "--progress",
                progress_callback=progress_callback,
                progress_args=progress_args,
            )
            if code:
                logger.error(
                    "rclone copyto failed ({}): {}",
                    code,
                    output.decode(errors="replace")[-1000:],
                )
                return False
            code, output = await CloudDrive._run_rclone(
                drive_config, "lsjson", remote_file, "--stat", "--hash"
            )
            if code:
                return False
            remote = json.loads(output)
            after = os.stat(file_path)
            if (
                remote.get("IsDir")
                or remote.get("Size") != before.st_size
                or (before.st_size, before.st_mtime_ns)
                != (after.st_size, after.st_mtime_ns)
            ):
                logger.error(
                    "Remote size or local stability verification failed: {}",
                    local_file_path,
                )
                return False
            hashes = {
                key.lower(): value
                for key, value in (remote.get("Hashes") or {}).items()
            }
            remote_md5 = hashes.get("md5")
            if remote_md5 and remote_md5.lower() != digest:
                logger.error("Remote MD5 mismatch: {}", local_file_path)
                return False
            drive_config.last_upload_verified = (
                bool(remote_md5) and file_path == local_file_path
            )
            drive_config.total_upload_success_file_count += 1
            if remote_md5:
                if drive_config.after_upload_file_delete:
                    # If a ZIP was uploaded, retain a concurrently changed original.
                    if file_path == local_file_path:
                        os.remove(local_file_path)
                    else:
                        logger.info(
                            "Uploaded ZIP verified; original retained: {}",
                            local_file_path,
                        )
                if file_path != local_file_path:
                    os.remove(file_path)
            else:
                logger.warning(
                    "Remote has no MD5; size verified, retaining local source: {}",
                    file_path,
                )
            return True
        except Exception as e:
            logger.error(f"{e.__class__} {e}")
            return False

    @staticmethod
    def aligo_upload_file(
        drive_config: CloudDriveConfig, save_path: str, local_file_path: str
    ):
        """aliyun upload file"""
        drive_config.last_upload_verified = False
        upload_status: bool = False
        if not drive_config.aligo:
            logger.warning("please config aligo! see README.md")
            return False

        try:
            remote_dir = (
                drive_config.remote_dir
                + "/"
                + os.path.dirname(local_file_path).replace(save_path, "")
                + "/"
            ).replace("\\", "/")

            if not drive_config.dir_cache.get(remote_dir):
                CloudDrive.aligo_mkdir(drive_config, remote_dir)
                aligo_dir = drive_config.aligo.get_folder_by_path(remote_dir)
                if aligo_dir:
                    drive_config.dir_cache[remote_dir] = aligo_dir.file_id

            zip_file_path: str = ""
            file_paths = []
            if drive_config.before_upload_file_zip:
                zip_file_path = CloudDrive.zip_file(local_file_path)
                file_paths.append(zip_file_path)
            else:
                file_paths.append(local_file_path)

            res = drive_config.aligo.upload_files(
                file_paths=file_paths,
                parent_file_id=drive_config.dir_cache[remote_dir],
                check_name_mode="refuse",
            )

            if len(res) > 0:
                drive_config.total_upload_success_file_count += len(res)
                logger.warning(
                    "Aligo returned upload results without remote hash verification; retaining local source"
                )
                upload_status = True

        except Exception as e:
            logger.error(f"{e.__class__} {e}")
            return False

        return upload_status

    @staticmethod
    async def upload_file(
        drive_config: CloudDriveConfig, save_path: str, local_file_path: str
    ) -> bool:
        """Upload file
        Parameters
        ----------
        drive_config: CloudDriveConfig
            see @CloudDriveConfig

        save_path: str
            Local file save path config

        local_file_path: str
            Local file path

        Returns
        -------
        bool
            True or False
        """
        if not drive_config.enable_upload_file:
            return False

        ret: bool = False
        if drive_config.upload_adapter == "rclone":
            ret = await CloudDrive.rclone_upload_file(
                drive_config, save_path, local_file_path
            )
        elif drive_config.upload_adapter == "aligo":
            ret = await _in_thread(
                CloudDrive.aligo_upload_file, drive_config, save_path, local_file_path
            )

        return ret
