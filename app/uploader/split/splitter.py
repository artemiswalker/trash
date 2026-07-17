from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger(__name__)

# Video extensions to decide splitting method
VIDEO_EXT = {
    ".mp4", ".mov", ".webm", ".mkv", ".avi", ".flv", ".wmv",
    ".3gp", ".mpeg", ".mpg", ".m4v", ".ts", ".f4v"
}

async def split_video(video_path: Path, max_size_bytes: int) -> list[Path]:
    try:
        from ...conversion import split_video_async
        parts = await split_video_async(video_path, max_size_bytes)
        if parts:
            return parts
    except Exception as e:
        log.exception("Failed to split video using PyAV segmenter: %s", video_path.name)

    return await split_binary(video_path, max_size_bytes)

async def split_binary(file_path: Path, max_size_bytes: int) -> list[Path]:
    parts = []
    chunk_size = int(max_size_bytes * 0.98)
    buffer_size = 1024 * 1024

    part_num = 1
    try:
        with open(file_path, "rb") as infile:
            while True:
                part_path = file_path.parent / f"{file_path.name}.{part_num:03d}"
                bytes_written = 0

                with open(part_path, "wb") as outfile:
                    while bytes_written < chunk_size:
                        chunk = infile.read(min(buffer_size, chunk_size - bytes_written))
                        if not chunk:
                            break
                        outfile.write(chunk)
                        bytes_written += len(chunk)

                if bytes_written == 0:
                    try:
                        part_path.unlink()
                    except Exception:
                        pass
                    break

                parts.append(part_path)
                part_num += 1
    except Exception:
        log.exception("Failed to binary split file: %s", file_path.name)
        for p in parts:
            try:
                p.unlink()
            except Exception:
                pass
        return []

    return parts

async def handle_large_file(path: Path, split_large_files: bool) -> list[Path]:
    max_size = int(1.95 * 1024 * 1024 * 1024)
    size = path.stat().st_size
    if size <= max_size:
        return [path]

    log.info("File '%s' size is %s bytes, exceeds 1.95GB threshold", path.name, size)
    if not split_large_files:
        log.info("split_large_files is False; deleting and skipping '%s'", path.name)
        try:
            path.unlink()
        except Exception:
            pass
        return []

    log.info("split_large_files is True; splitting '%s'", path.name)
    ext = path.suffix.lower()
    if ext in VIDEO_EXT:
        parts = await split_video(path, max_size)
    else:
        parts = await split_binary(path, max_size)

    if parts:
        log.info("Successfully split '%s' into %s parts", path.name, len(parts))
        try:
            path.unlink()
        except Exception:
            pass
        return parts
    else:
        log.error("Failed to split '%s'; deleting to avoid loop", path.name)
        try:
            path.unlink()
        except Exception:
            pass
        return []
