from __future__ import annotations

import asyncio
import logging
import shutil
import socket
import json
import base64
import urllib.request
from pathlib import Path
from typing import Callable, Optional

from .downloader import DownloadResult

log = logging.getLogger(__name__)

TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.coppersurfer.tk:6969/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://tracker.internetwarriors.net:1337/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker.cyberia.is:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://retracker.lanta-net.ru:2710/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://valkyrie.info:6969/announce",
    "udp://ipv4.tracker.harry.lu:80/announce",
    "http://tracker.gbitt.info:80/announce",
    "http://tracker.ipv6tracker.ru:80/announce"
]


def get_free_port() -> int:
    """Find a free local TCP port dynamically."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def sync_rpc_call(port: int, method: str, params: list) -> dict:
    """Synchronous JSON-RPC client call using urllib, bypassing system proxies."""
    url = f"http://localhost:{port}/jsonrpc"
    payload = {
        "jsonrpc": "2.0",
        "id": "tgdl",
        "method": method,
        "params": params
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"}
    )
    # Bypass system proxies for localhost calls
    proxy_handler = urllib.request.ProxyHandler({})
    opener = urllib.request.build_opener(proxy_handler)
    with opener.open(req, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


async def async_rpc_call(port: int, method: str, params: list) -> dict:
    """Run JSON-RPC client call asynchronously using asyncio.to_thread."""
    return await asyncio.to_thread(sync_rpc_call, port, method, params)


async def download_torrent_async(
    torrent_or_magnet: str,
    dest_dir: Path,
    on_progress: Optional[Callable[[float, float, float], None]] = None,
    register_proc: Optional[Callable[[Optional[asyncio.subprocess.Process]], None]] = None,
) -> DownloadResult:
    """Download a torrent or magnet link asynchronously using a dynamic aria2c RPC daemon."""
    if shutil.which("aria2c") is None:
        return DownloadResult(ok=False, error_tail="aria2c CLI client is not installed.")

    target = torrent_or_magnet
    is_torrent_file = target.startswith("torrent:") or target.endswith(".torrent")
    if target.startswith("torrent:"):
        target = target[len("torrent:"):]

    tracker_arg = f"--bt-tracker={','.join(TRACKERS)}"
    port = get_free_port()

    cmd = [
        "aria2c",
        "--enable-rpc",
        "--rpc-listen-all=true",
        f"--rpc-listen-port={port}",
        "--seed-time=0",
        "--seed-ratio=0.0",
        "--bt-tracker-connect-timeout=10",
        "--bt-tracker-timeout=10",
        "--enable-dht=true",
        "--bt-enable-lpd=true",
        "--enable-peer-exchange=true",
        "--bt-max-peers=80",
        "--max-overall-upload-limit=50K",
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        tracker_arg
    ]


    log.info("Launching aria2c RPC daemon on port %s", port)
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
    except Exception as e:
        log.exception("Failed to start aria2c RPC daemon")
        return DownloadResult(ok=False, error_tail=f"Failed to start aria2c daemon: {e}")

    if register_proc:
        register_proc(proc)

    daemon_ready = False
    for _ in range(25):
        try:
            await async_rpc_call(port, "aria2.getVersion", [])
            daemon_ready = True
            break
        except Exception:
            await asyncio.sleep(0.2)

    if not daemon_ready:
        try:
            proc.terminate()
            await proc.wait()
        except Exception:
            pass
        return DownloadResult(ok=False, error_tail="aria2c RPC daemon failed to start listening.")

    gid = None
    try:
        if is_torrent_file:
            torrent_path = Path(target)
            if not torrent_path.exists():
                raise FileNotFoundError(f"Torrent file not found: {torrent_path}")
            with open(torrent_path, "rb") as f:
                b64_content = base64.b64encode(f.read()).decode("utf-8")
            response = await async_rpc_call(port, "aria2.addTorrent", [b64_content, [], {"dir": str(dest_dir)}])
        else:
            response = await async_rpc_call(port, "aria2.addUri", [[target], {"dir": str(dest_dir)}])
        gid = response.get("result")
    except Exception as e:
        log.exception("Failed to add download to aria2c RPC daemon")
        try:
            proc.terminate()
            await proc.wait()
        except Exception:
            pass
        return DownloadResult(ok=False, error_tail=f"Failed to add download to daemon: {e}")

    if not gid:
        try:
            proc.terminate()
            await proc.wait()
        except Exception:
            pass
        return DownloadResult(ok=False, error_tail="aria2c daemon did not return a GID.")

    ok = False
    error_tail = ""
    try:
        while True:
            # Check if subprocess has exited (non-blocking poll)
            try:
                await asyncio.wait_for(proc.wait(), timeout=0.0)
            except asyncio.TimeoutError:
                pass

            if proc.returncode is not None:
                log.error("aria2c daemon stopped unexpectedly with code %s", proc.returncode)
                break

            try:
                response = await async_rpc_call(port, "aria2.tellStatus", [gid])
                result = response.get("result", {})
            except Exception as e:
                log.error("Failed to query status from aria2c daemon: %s", e)
                await asyncio.sleep(2.0)
                continue


            followed_by = result.get("followedBy")
            if followed_by and len(followed_by) > 0:
                log.info("Download transitioned to new GID: %s", followed_by[0])
                gid = followed_by[0]
                await asyncio.sleep(1.0)
                continue

            status = result.get("status")
            if status == "complete":
                ok = True
                break
            elif status == "error":
                ok = False
                error_code = result.get("errorCode", "unknown")
                error_msg = result.get("errorMessage", "unknown error")
                error_tail = f"Aria2 error code {error_code}: {error_msg}"
                break

            completed_len = float(result.get("completedLength", 0))
            total_len = float(result.get("totalLength", 0))
            speed = float(result.get("downloadSpeed", 0))

            pct = (completed_len * 100.0 / total_len) if total_len > 0 else 0.0

            torrent_name = None
            bt = result.get("bittorrent", {})
            info = bt.get("info", {})
            if info.get("name"):
                torrent_name = info["name"]
            else:
                files = result.get("files", [])
                if files and files[0].get("path"):
                    p = Path(files[0]["path"])
                    if p.name:
                        torrent_name = p.name

            if on_progress:
                try:
                    on_progress(pct, completed_len, speed, torrent_name)
                except TypeError:
                    on_progress(pct, completed_len, speed)

            await asyncio.sleep(2.0)

    except Exception as e:
        log.exception("Exception in progress monitoring loop")
        error_tail = str(e)
    finally:
        try:
            proc.terminate()
            await proc.wait()
        except Exception:
            pass

    files = []
    if ok and dest_dir.exists():
        files = [
            p for p in dest_dir.rglob("*")
            if p.is_file() and not p.name.endswith(".part") and not p.name.endswith(".aria2")
        ]

    return DownloadResult(ok=ok, files=files, error_tail=error_tail, attempts=1)
