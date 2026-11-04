"""Durable, thread-safe task scheduling and verified-file metadata."""

import json
import os
import sqlite3
import threading
import time
import uuid

STATES = (
    "queued",
    "running",
    "retry_wait",
    "paused",
    "cancelled",
    "completed",
    "failed",
)


class TaskStore:
    """One SQLite connection per store; claims are safe across store instances.

    Payloads contain JSON values only. Put the serializable TaskNode settings in
    ``payload['node']`` and its stable group identifier in ``payload['group_id']``.
    Live clients, messages and bot objects must be reconstructed by the consumer.
    """

    def __init__(self, path, recover_running=True):
        self._lock = threading.RLock()
        if str(path) != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(str(path), timeout=10, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        with self._lock, self._db:
            self._db.execute("PRAGMA busy_timeout=10000")
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            version = self._db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2):
                self._db.close()
                raise ValueError("Unsupported task database schema: %s" % version)
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dedup_key TEXT NOT NULL UNIQUE,
                    group_id TEXT,
                    payload TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'queued',
                    priority INTEGER NOT NULL DEFAULT 0,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    next_run REAL NOT NULL DEFAULT 0,
                    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                    total_bytes INTEGER NOT NULL DEFAULT 0,
                    path TEXT,
                    error TEXT,
                    error_kind TEXT,
                    result TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS tasks_schedule
                    ON tasks(state, next_run, priority DESC, id);
                CREATE INDEX IF NOT EXISTS tasks_group ON tasks(group_id, id);
                CREATE TABLE IF NOT EXISTS files (
                    path TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    md5 TEXT NOT NULL,
                    file_unique_id TEXT,
                    verified_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS files_size ON files(size);
                CREATE INDEX IF NOT EXISTS files_unique_id ON files(file_unique_id);
                CREATE TABLE IF NOT EXISTS file_aliases (
                    file_unique_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    md5 TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    PRIMARY KEY(file_unique_id,path)
                );
                CREATE TABLE IF NOT EXISTS scheduler_groups (
                    group_id TEXT PRIMARY KEY,
                    last_claim INTEGER NOT NULL
                );
            """
            )
            columns = {row[1] for row in self._db.execute("PRAGMA table_info(tasks)")}
            for name in ("claim_token", "stage", "md5"):
                if name not in columns:
                    self._db.execute("ALTER TABLE tasks ADD COLUMN " + name + " TEXT")
            self._db.execute("PRAGMA user_version=2")
        if recover_running:
            self.recover_running()

    @staticmethod
    def _row(row):
        if row is None:
            return None
        data = dict(row)
        for key in ("payload", "result"):
            if key in data and data[key] is not None:
                data[key] = json.loads(data[key])
        return data

    def enqueue(self, payload, dedup_key, priority=0, max_attempts=3):
        """Return the existing task for a duplicate key, including terminal tasks."""
        if not dedup_key or max_attempts < 1:
            raise ValueError("dedup_key and positive max_attempts are required")
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        now = time.time()
        group = payload.get("group_id")
        with self._lock, self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO tasks "
                "(dedup_key,group_id,payload,priority,max_attempts,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    dedup_key,
                    str(group) if group is not None else None,
                    encoded,
                    int(priority),
                    int(max_attempts),
                    now,
                    now,
                ),
            )
            return self._row(
                self._db.execute(
                    "SELECT * FROM tasks WHERE dedup_key=?", (dedup_key,)
                ).fetchone()
            )

    def get(self, task_id):
        with self._lock:
            return self._row(
                self._db.execute(
                    "SELECT * FROM tasks WHERE id=?", (task_id,)
                ).fetchone()
            )

    def claim(self, now=None, kinds=None):
        """Claim by priority, rotating equally prioritized groups fairly.

        The persistent group claim sequence ensures a newly arriving group gets
        a turn before a large existing group's next file. ``kinds`` partitions
        consumers without duplicating the durable task row.
        """
        if kinds is not None and not kinds:
            return None
        now = time.time() if now is None else now
        kind_filter = ""
        args = [now]
        if kinds is not None:
            kind_filter = (
                " AND json_extract(t.payload,'$.kind') IN ("
                + ",".join("?" for _ in kinds)
                + ")"
            )
            args.extend(kinds)
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.execute(
                    "SELECT t.id,COALESCE(t.group_id,'') AS scheduler_group FROM tasks t "
                    "LEFT JOIN scheduler_groups g ON g.group_id=COALESCE(t.group_id,'') "
                    "WHERE t.state IN ('queued','retry_wait') "
                    "AND t.next_run<=? AND t.attempt<t.max_attempts"
                    + kind_filter
                    + " ORDER BY t.priority DESC,COALESCE(g.last_claim,0),t.id ASC LIMIT 1",
                    args,
                ).fetchone()
                if row is None:
                    self._db.commit()
                    return None
                self._db.execute(
                    "UPDATE tasks SET state='running',attempt=attempt+1,updated_at=?,claim_token=? "
                    "WHERE id=?",
                    (now, uuid.uuid4().hex, row[0]),
                )
                sequence = self._db.execute(
                    "SELECT COALESCE(MAX(last_claim),0)+1 FROM scheduler_groups"
                ).fetchone()[0]
                self._db.execute(
                    "INSERT INTO scheduler_groups(group_id,last_claim) VALUES(?,?) "
                    "ON CONFLICT(group_id) DO UPDATE SET last_claim=excluded.last_claim",
                    (row["scheduler_group"], sequence),
                )
                result = self._row(
                    self._db.execute(
                        "SELECT * FROM tasks WHERE id=?", (row[0],)
                    ).fetchone()
                )
                self._db.commit()
                return result
            except BaseException:
                self._db.rollback()
                raise

    def recover_running(self):
        """Call once at service startup, before starting consumers.

        A process interruption does not consume a retry attempt. Paused tasks
        retain their state. Do not call while another process consumes this DB.
        """
        with self._lock, self._db:
            return self._db.execute(
                "UPDATE tasks SET state='queued',attempt=MAX(0,attempt-1),"
                "next_run=0,claim_token=NULL,updated_at=? WHERE state='running'",
                (time.time(),),
            ).rowcount

    @staticmethod
    def _filters(state=None, group_id=None, search=None, owner_id=None):
        clauses, args = [], []
        if state is not None:
            if state not in STATES:
                raise ValueError("Unknown task state")
            clauses.append("state=?")
            args.append(state)
        if group_id is not None:
            clauses.append("group_id=?")
            args.append(str(group_id))
        if search:
            clauses.append(
                "(payload LIKE ? ESCAPE '\\' OR path LIKE ? ESCAPE '\\' OR group_id LIKE ? ESCAPE '\\' OR json_extract(payload,'$.file_name') LIKE ? ESCAPE '\\')"
            )
            pattern = (
                "%"
                + str(search)
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
                + "%"
            )
            args.extend([pattern] * 4)
        if owner_id is not None:
            clauses.append(
                "CAST(COALESCE(json_extract(payload,'$.from_user_id'),json_extract(payload,'$.node.from_user_id')) AS TEXT)=?"
            )
            args.append(str(owner_id))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        return where, args

    def list_tasks(
        self, state=None, limit=50, offset=0, group_id=None, search=None, owner_id=None
    ):
        where, args = self._filters(state, group_id, search, owner_id)
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM tasks"
                + where
                + " ORDER BY priority DESC,id DESC LIMIT ? OFFSET ?",
                args + [max(0, min(int(limit), 1000)), max(0, int(offset))],
            ).fetchall()
            return [self._row(row) for row in rows]

    def count_tasks(self, state=None, group_id=None, search=None, owner_id=None):
        where, args = self._filters(state, group_id, search, owner_id)
        with self._lock:
            return self._db.execute(
                "SELECT COUNT(*) FROM tasks" + where, args
            ).fetchone()[0]

    def path_in_use(self, path, excluding=None):
        """Keep shared verified content until all dependent uploads have finished."""
        with self._lock:
            return bool(
                self._db.execute(
                    "SELECT 1 FROM tasks WHERE path=? AND id!=? "
                    "AND state NOT IN ('completed','cancelled') LIMIT 1",
                    (path, excluding or -1),
                ).fetchone()
            )

    def group_summary(self, group_id):
        """Authoritative bot counters survive a process restart or duplicate scan."""
        with self._lock:
            rows = self._db.execute(
                "SELECT state,COUNT(*) AS n,COALESCE(SUM(downloaded_bytes),0) AS bytes,"
                "SUM(CASE WHEN json_extract(result,'$.skipped')=1 THEN 1 ELSE 0 END) AS skipped "
                "FROM tasks WHERE group_id=? AND json_extract(payload,'$.kind')!='scan' GROUP BY state",
                (str(group_id),),
            ).fetchall()
        counts = dict.fromkeys(STATES, 0)
        skipped = size = 0
        for row in rows:
            counts[row["state"]] = row["n"]
            size += row["bytes"]
            if row["state"] == "completed":
                skipped += row["skipped"] or 0
        return {
            "counts": counts,
            "total": sum(counts.values()),
            "skipped": skipped,
            "bytes": size,
            "finished": sum(
                counts[state] for state in ("completed", "failed", "cancelled")
            ),
        }

    def stats(self, group_id=None, owner_id=None):
        where, args = self._filters(group_id=group_id, owner_id=owner_id)
        with self._lock:
            rows = self._db.execute(
                "SELECT state,COUNT(*) AS count FROM tasks" + where + " GROUP BY state",
                args,
            ).fetchall()
        result = dict.fromkeys(STATES, 0)
        result.update({row["state"]: row["count"] for row in rows})
        result["total"] = sum(result.values())
        return result

    def update_progress(
        self, task_id, downloaded_bytes, total_bytes=None, path=None, token=None
    ):
        if downloaded_bytes < 0 or (total_bytes is not None and total_bytes < 0):
            raise ValueError("Progress cannot be negative")
        with self._lock, self._db:
            return bool(
                self._db.execute(
                    "UPDATE tasks SET downloaded_bytes=?,total_bytes=COALESCE(?,total_bytes),"
                    "path=COALESCE(?,path),updated_at=? WHERE id=? "
                    "AND state IN ('running','paused') AND (? IS NULL OR claim_token=?)",
                    (
                        int(downloaded_bytes),
                        total_bytes,
                        path,
                        time.time(),
                        task_id,
                        token,
                        token,
                    ),
                ).rowcount
            )

    def checkpoint(self, task_id, stage, path=None, md5=None, result=None, token=None):
        """Commit a recoverable pipeline stage without marking the task finished."""
        encoded = (
            json.dumps(result, ensure_ascii=False, allow_nan=False)
            if result is not None
            else None
        )
        with self._lock, self._db:
            return bool(
                self._db.execute(
                    "UPDATE tasks SET stage=?,path=COALESCE(?,path),md5=COALESCE(?,md5),"
                    "result=COALESCE(?,result),updated_at=? WHERE id=? AND state='running' "
                    "AND (? IS NULL OR claim_token=?)",
                    (stage, path, md5, encoded, time.time(), task_id, token, token),
                ).rowcount
            )

    def refresh_payload(self, task_id, payload, token=None):
        """Persist the scan cursor together with the recoverable scan settings."""
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        with self._lock, self._db:
            return bool(
                self._db.execute(
                    "UPDATE tasks SET payload=?,updated_at=? WHERE id=? AND state='running' "
                    "AND (? IS NULL OR claim_token=?)",
                    (encoded, time.time(), task_id, token, token),
                ).rowcount
            )

    def release(self, task_id, token):
        """Yield a scan page without spending the error-retry budget."""
        with self._lock, self._db:
            return bool(
                self._db.execute(
                    "UPDATE tasks SET state='queued',attempt=MAX(0,attempt-1),claim_token=NULL,"
                    "updated_at=? WHERE id=? AND state='running' AND claim_token=?",
                    (time.time(), task_id, token),
                ).rowcount
            )

    def handoff(self, task_id, payload, stage="uploading", token=None):
        """Release a downloaded task to the upload consumer without losing its ID."""
        payload = dict(payload, kind="upload")
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        group = payload.get("group_id")
        with self._lock, self._db:
            return bool(
                self._db.execute(
                    "UPDATE tasks SET payload=?,group_id=COALESCE(?,group_id),stage=?,"
                    "state='queued',attempt=0,next_run=0,claim_token=NULL,error=NULL,"
                    "error_kind=NULL,updated_at=? WHERE id=? AND state='running' "
                    "AND (? IS NULL OR claim_token=?)",
                    (
                        encoded,
                        str(group) if group is not None else None,
                        stage,
                        time.time(),
                        task_id,
                        token,
                        token,
                    ),
                ).rowcount
            )

    def complete(self, task_id, result=None, token=None):
        encoded = (
            json.dumps(result, ensure_ascii=False, allow_nan=False)
            if result is not None
            else None
        )
        with self._lock, self._db:
            return bool(
                self._db.execute(
                    "UPDATE tasks SET state='completed',result=COALESCE(?,result),error=NULL,error_kind=NULL,"
                    "updated_at=? WHERE id=? AND state='running' AND (? IS NULL OR claim_token=?)",
                    (encoded, time.time(), task_id, token, token),
                ).rowcount
            )

    def fail(self, task_id, error, error_kind="unknown", retry_delay=None, token=None):
        """Retry only when a delay is supplied and the attempt budget remains."""
        now = time.time()
        with self._lock, self._db:
            return bool(
                self._db.execute(
                    "UPDATE tasks SET state=CASE WHEN ? IS NOT NULL AND attempt<max_attempts "
                    "THEN 'retry_wait' ELSE 'failed' END,error=?,error_kind=?,next_run=?,"
                    "updated_at=? WHERE id=? AND state='running' AND (? IS NULL OR claim_token=?)",
                    (
                        retry_delay,
                        str(error),
                        error_kind,
                        now + max(0, retry_delay or 0),
                        now,
                        task_id,
                        token,
                        token,
                    ),
                ).rowcount
            )

    def _transition(self, task_id, state, allowed):
        placeholders = ",".join("?" for _ in allowed)
        with self._lock, self._db:
            return bool(
                self._db.execute(
                    "UPDATE tasks SET state=?,updated_at=? WHERE id=? AND state IN ("
                    + placeholders
                    + ")",
                    (state, time.time(), task_id) + tuple(allowed),
                ).rowcount
            )

    def pause(self, task_id):
        return self._transition(task_id, "paused", ("queued", "running", "retry_wait"))

    def resume(self, task_id):
        # Reset the budget for explicit user retry; preserve saved progress.
        with self._lock, self._db:
            return bool(
                self._db.execute(
                    "UPDATE tasks SET state='queued',attempt=0,next_run=0,claim_token=NULL,updated_at=? "
                    "WHERE id=? AND state IN ('paused','failed','cancelled')",
                    (time.time(), task_id),
                ).rowcount
            )

    def cancel(self, task_id):
        return self._transition(
            task_id, "cancelled", ("queued", "running", "retry_wait", "paused")
        )

    def pause_all(self):
        with self._lock, self._db:
            return self._db.execute(
                "UPDATE tasks SET state='paused',updated_at=? "
                "WHERE state IN ('queued','running','retry_wait')",
                (time.time(),),
            ).rowcount

    def resume_all(self):
        with self._lock, self._db:
            return self._db.execute(
                "UPDATE tasks SET state='queued',attempt=0,next_run=0,claim_token=NULL,updated_at=? "
                "WHERE state='paused'",
                (time.time(),),
            ).rowcount

    def set_priority(self, task_id, priority):
        with self._lock, self._db:
            return bool(
                self._db.execute(
                    "UPDATE tasks SET priority=?,updated_at=? WHERE id=?",
                    (int(priority), time.time(), task_id),
                ).rowcount
            )

    def upsert_file(self, path, size, mtime_ns, md5, file_unique_id=None):
        """Store an externally verified MD5; consumers must re-stat before reuse."""
        if (
            size < 0
            or len(md5) != 32
            or any(c not in "0123456789abcdefABCDEF" for c in md5)
        ):
            raise ValueError("Invalid file size or MD5")
        path = os.path.abspath(path)
        with self._lock, self._db:
            self._db.execute(
                "INSERT INTO files(path,size,mtime_ns,md5,file_unique_id,verified_at) "
                "VALUES(?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET "
                "size=excluded.size,mtime_ns=excluded.mtime_ns,md5=excluded.md5,"
                "file_unique_id=excluded.file_unique_id,verified_at=excluded.verified_at",
                (
                    path,
                    int(size),
                    int(mtime_ns),
                    md5.lower(),
                    file_unique_id,
                    time.time(),
                ),
            )
            if file_unique_id:
                self._db.execute(
                    "INSERT INTO file_aliases(file_unique_id,path,md5,size) VALUES(?,?,?,?) "
                    "ON CONFLICT(file_unique_id,path) DO UPDATE SET md5=excluded.md5,size=excluded.size",
                    (file_unique_id, path, md5.lower(), int(size)),
                )

    def get_file(self, path):
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM files WHERE path=?", (os.path.abspath(path),)
            ).fetchone()
            return dict(row) if row else None

    def find_files(self, size, md5=None, file_unique_id=None):
        clauses, args = ["size=?"], [size]
        if md5 is not None:
            clauses.append("md5=?")
            args.append(md5.lower())
        if file_unique_id is not None:
            clauses.append(
                "(file_unique_id=? OR EXISTS (SELECT 1 FROM file_aliases a "
                "WHERE a.file_unique_id=? AND a.path=files.path AND a.md5=files.md5 AND a.size=files.size))"
            )
            args.extend((file_unique_id, file_unique_id))
        with self._lock:
            return [
                dict(row)
                for row in self._db.execute(
                    "SELECT * FROM files WHERE " + " AND ".join(clauses), args
                ).fetchall()
            ]

    def close(self):
        with self._lock:
            self._db.close()
