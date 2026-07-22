from __future__ import annotations

import asyncio
import logging
import os
import shutil
import zipfile

from pathlib import Path
from typing import Optional

from ..config import settings

log = logging.getLogger(__name__)


async def _run_cmd_in_cwd(cmd: list[str], cwd: Path) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(cwd),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    output = (stdout.decode(errors="ignore") + stderr.decode(errors="ignore")).strip()
    return proc.returncode or 0, output


async def archive_folder_async(
    folder_path: Path,
    archive_format: str = "zip",
    max_part_size_mb: int = 1900,
    mirror_pixeldrain: bool = False
) -> tuple[list[Path], list[tuple[str, str]]]:
    """Compresses a folder into single or split .zip / .7z archives.

    If mirror_pixeldrain is True, uploads the original unsplit archive to Pixeldrain.
    Returns (telegram_file_paths, pixeldrain_links).
    """
    pd_links: list[tuple[str, str]] = []
    if not folder_path.exists() or not folder_path.is_dir():
        log.warning("Path %s is not a directory or does not exist. Skipping archive.", folder_path)
        return ([folder_path] if folder_path.exists() else []), pd_links

    parent_dir = folder_path.parent
    folder_name = folder_path.name
    fmt = archive_format.lower().lstrip("-")
    if fmt not in ("zip", "7z"):
        fmt = "zip"

    output_archive = parent_dir / f"{folder_name}.{fmt}"
    has_7z = shutil.which("7z") is not None
    has_zip = shutil.which("zip") is not None

    log.info("Archiving folder '%s' into unsplit %s format...", folder_name, fmt)

    success = False
    if has_7z:
        type_flag = "-tzip" if fmt == "zip" else "-t7z"
        cmd = ["7z", "a", type_flag, "-y", str(output_archive), "."]
        code, out = await _run_cmd_in_cwd(cmd, folder_path)
        if code == 0 and output_archive.exists():
            success = True
        else:
            log.warning("7z archive command failed (code %s): %s", code, out)

    elif fmt == "zip" and has_zip:
        cmd = ["zip", "-r", str(output_archive), "."]
        code, out = await _run_cmd_in_cwd(cmd, folder_path)
        if code == 0 and output_archive.exists():
            success = True
        else:
            log.warning("zip command failed: %s", out)

    elif fmt == "zip":
        try:
            log.info("7z and zip CLI tools not found. Using standard Python zipfile library for %s", output_archive.name)
            def _create_zip():
                with zipfile.ZipFile(output_archive, "w", zipfile.ZIP_DEFLATED) as zf:
                    for root, _, files in os.walk(folder_path):
                        for file in files:
                            full_path = Path(root) / file
                            arcname = full_path.relative_to(folder_path)
                            zf.write(full_path, arcname)

            await asyncio.to_thread(_create_zip)
            if output_archive.exists() and output_archive.stat().st_size > 0:
                success = True
        except Exception as ze:
            log.exception("Python zipfile fallback failed: %s", ze)

    if not success or not output_archive.exists():
        log.error("Failed to archive folder %s. Keeping uncompressed files.", folder_path)
        return list(folder_path.rglob("*")), pd_links

    archive_size = output_archive.stat().st_size
    pixeldrain_max_bytes = 10 * 1024 * 1024 * 1024  # 10 GB limit for free accounts
    upload_unsplit_to_pd = mirror_pixeldrain and archive_size <= pixeldrain_max_bytes
    upload_parts_to_pd = mirror_pixeldrain and archive_size > pixeldrain_max_bytes

    # 1. Upload original unsplit archive to Pixeldrain if requested and size <= 10GB
    if upload_unsplit_to_pd:
        try:
            from ..uploader import upload_to_pixeldrain
            log.info("Mirroring original unsplit archive '%s' (%.2f GB) to Pixeldrain...", output_archive.name, archive_size / (1024**3))
            res, pd_logs = await upload_to_pixeldrain(output_archive, api_key=settings.pixeldrain_api_key)
            if isinstance(res, dict) and res.get("id"):
                pd_url = f"https://pixeldrain.com/u/{res['id']}"
                log.info("Successfully mirrored '%s' to Pixeldrain: %s", output_archive.name, pd_url)
                pd_links.append((output_archive.name, pd_url))
            else:
                log.warning("Pixeldrain upload response missing id for %s: %s", output_archive.name, res)
        except Exception as pe:
            log.exception("Failed to upload %s to Pixeldrain: %s", output_archive.name, pe)

    # 2. Check if volume splitting is required for Telegram (> 1.95GB)
    file_size = archive_size
    limit_bytes = max_part_size_mb * 1024 * 1024

    telegram_archives: list[Path] = []
    if file_size > limit_bytes:
        log.info("Archive '%s' (%.2f MB) exceeds %d MB limit. Splitting for Telegram upload...", output_archive.name, file_size / (1024*1024), max_part_size_mb)
        if has_7z:
            type_flag = "-tzip" if fmt == "zip" else "-t7z"
            cmd = ["7z", "a", type_flag, f"-v{max_part_size_mb}m", "-y", str(parent_dir / f"{folder_name}_parts.{fmt}"), str(output_archive)]
            code, out = await _run_cmd_in_cwd(cmd, parent_dir)
            if code == 0:
                output_archive.unlink(missing_ok=True)
                prefix = f"{folder_name}_parts.{fmt}"
                for p in sorted(parent_dir.iterdir()):
                    if p.is_file() and (p.name == prefix or p.name.startswith(f"{prefix}.")):
                        telegram_archives.append(p)
            else:
                log.warning("Failed to split archive with 7z: %s", out)

        if not telegram_archives and shutil.which("split"):
            split_prefix = f"{output_archive.name}."
            cmd = ["split", "-b", f"{max_part_size_mb}m", "-d", str(output_archive), str(parent_dir / split_prefix)]
            code, out = await _run_cmd_in_cwd(cmd, parent_dir)
            if code == 0:
                output_archive.unlink(missing_ok=True)
                for p in sorted(parent_dir.iterdir()):
                    if p.is_file() and p.name.startswith(split_prefix):
                        telegram_archives.append(p)

    if not telegram_archives:
        telegram_archives = [output_archive]

    # 3. Fallback Pixeldrain upload for split volume parts if unsplit archive exceeded 10 GB
    if upload_parts_to_pd and telegram_archives:
        log.info(
            "Unsplit archive '%s' (%.2f GB) exceeded 10 GB limit. Mirroring %d split volume parts to Pixeldrain...",
            output_archive.name,
            archive_size / (1024**3),
            len(telegram_archives)
        )
        from ..uploader import upload_to_pixeldrain
        for part_path in telegram_archives:
            try:
                res, pd_logs = await upload_to_pixeldrain(part_path, api_key=settings.pixeldrain_api_key)
                if isinstance(res, dict) and res.get("id"):
                    pd_url = f"https://pixeldrain.com/u/{res['id']}"
                    log.info("Successfully mirrored split volume '%s' to Pixeldrain: %s", part_path.name, pd_url)
                    pd_links.append((part_path.name, pd_url))
                else:
                    log.warning("Pixeldrain upload response missing id for split volume %s: %s", part_path.name, res)
            except Exception as pe:
                log.exception("Failed to upload split volume %s to Pixeldrain: %s", part_path.name, pe)

    shutil.rmtree(folder_path, ignore_errors=True)
    log.info("Successfully archived '%s' into %d Telegram file(s). Original folder deleted.", folder_name, len(telegram_archives))
    return telegram_archives, pd_links


async def archive_all_folders_in_dir(
    target_dir: Path,
    archive_format: str = "zip",
    mirror_pixeldrain: bool = False
) -> tuple[list[Path], list[tuple[str, str]]]:
    """Iterates through target_dir and archives each folder individually.
    
    If top-level files exist alongside or without subfolders, they are also archived.
    Returns (telegram_file_paths, pixeldrain_links).
    """
    final_paths: list[Path] = []
    all_pd_links: list[tuple[str, str]] = []

    if not target_dir.exists() or not target_dir.is_dir():
        return final_paths, all_pd_links

    subdirs = [p for p in target_dir.iterdir() if p.is_dir()]
    top_files = [p for p in target_dir.iterdir() if p.is_file()]

    for item in subdirs:
        archives, pd_links = await archive_folder_async(
            item, archive_format=archive_format, mirror_pixeldrain=mirror_pixeldrain
        )
        final_paths.extend(archives)
        all_pd_links.extend(pd_links)

    if top_files and archive_format:
        top_files_dir = target_dir / "Files"
        top_files_dir.mkdir(exist_ok=True)
        for f in top_files:
            try:
                f.rename(top_files_dir / f.name)
            except Exception as e:
                log.warning("Could not move %s into %s: %s", f.name, top_files_dir.name, e)

        archives, pd_links = await archive_folder_async(
            top_files_dir, archive_format=archive_format, mirror_pixeldrain=mirror_pixeldrain
        )
        final_paths.extend(archives)
        all_pd_links.extend(pd_links)

    return final_paths, all_pd_links