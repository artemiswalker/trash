from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Optional

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    status_message_id INTEGER,
    url TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    total_files INTEGER NOT NULL DEFAULT 0,
    sent_files INTEGER NOT NULL DEFAULT 0,
    skipped_files INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    split_large_files INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS uploaded_files (
    job_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    PRIMARY KEY (job_id, filename)
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
"""


class JobStatus(StrEnum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Job:
    id: int
    chat_id: int
    status_message_id: Optional[int]
    url: str
    status: str
    total_files: int
    sent_files: int
    skipped_files: int
    error: Optional[str]
    created_at: float
    updated_at: float
    split_large_files: int = 1

    @property
    def download_dir(self) -> str:
        return f"job_{self.id}"


class JobStore:
    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def open(self) -> None:
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA)
        # Automatic column migration if jobs table already exists
        try:
            await self._db.execute("ALTER TABLE jobs ADD COLUMN split_large_files INTEGER NOT NULL DEFAULT 1")
        except aiosqlite.OperationalError:
            pass  # Already exists
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "JobStore not opened — call await store.open() first"
        return self._db

    async def create_job(self, chat_id: int, url: str, split_large_files: int = 1) -> Job:
        now = time.time()
        cur = await self.db.execute(
            "INSERT INTO jobs (chat_id, url, status, split_large_files, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (chat_id, url, JobStatus.QUEUED, split_large_files, now, now),
        )
        await self.db.commit()
        job = await self.get_job(cur.lastrowid)
        assert job is not None
        return job

    async def get_job(self, job_id: int) -> Optional[Job]:
        cur = await self.db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = await cur.fetchone()
        return self._row_to_job(row) if row else None

    async def set_status_message(self, job_id: int, message_id: int) -> None:
        await self.db.execute(
            "UPDATE jobs SET status_message_id = ?, updated_at = ? WHERE id = ?",
            (message_id, time.time(), job_id),
        )
        await self.db.commit()

    async def update_progress(
        self,
        job_id: int,
        *,
        status: Optional[str] = None,
        total_files: Optional[int] = None,
        sent_files: Optional[int] = None,
        skipped_files: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        fields, values = [], []
        for col, val in (
            ("status", status),
            ("total_files", total_files),
            ("sent_files", sent_files),
            ("skipped_files", skipped_files),
            ("error", error),
        ):
            if val is not None:
                fields.append(f"{col} = ?")
                values.append(val)
        if not fields:
            return
        fields.append("updated_at = ?")
        values.append(time.time())
        values.append(job_id)
        await self.db.execute(f"UPDATE jobs SET {', '.join(fields)} WHERE id = ?", values)
        await self.db.commit()

    async def mark_uploaded(self, job_id: int, filename: str) -> None:
        await self.db.execute(
            "INSERT OR IGNORE INTO uploaded_files (job_id, filename) VALUES (?, ?)",
            (job_id, filename),
        )
        await self.db.commit()

    async def get_uploaded_filenames(self, job_id: int) -> set[str]:
        cur = await self.db.execute("SELECT filename FROM uploaded_files WHERE job_id = ?", (job_id,))
        rows = await cur.fetchall()
        return {r["filename"] for r in rows}

    async def resumable_jobs(self) -> list[Job]:
        """Jobs that were mid-flight when the process last stopped."""
        cur = await self.db.execute(
            "SELECT * FROM jobs WHERE status IN (?, ?) ORDER BY id",
            (JobStatus.DOWNLOADING, JobStatus.UPLOADING),
        )
        rows = await cur.fetchall()
        return [self._row_to_job(r) for r in rows]

    async def queued_jobs(self) -> list[Job]:
        cur = await self.db.execute("SELECT * FROM jobs WHERE status = ? ORDER BY id", (JobStatus.QUEUED,))
        rows = await cur.fetchall()
        return [self._row_to_job(r) for r in rows]

    @staticmethod
    def _row_to_job(row: aiosqlite.Row) -> Job:
        cols = row.keys()
        split_large_files = row["split_large_files"] if "split_large_files" in cols else 1
        return Job(
            id=row["id"],
            chat_id=row["chat_id"],
            status_message_id=row["status_message_id"],
            url=row["url"],
            status=row["status"],
            total_files=row["total_files"],
            sent_files=row["sent_files"],
            skipped_files=row["skipped_files"],
            error=row["error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            split_large_files=split_large_files,
        )
