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

ARIA2_PORT: Optional[int] = None
ARIA2_PROC: Optional[asyncio.subprocess.Process] = None


class Aria2DownloadTask:
    """Mock process object to support cancellation via RPC forceRemove."""
    def __init__(self, port: int, gid: str):
        self.port = port
        self.gid = gid

    def kill(self):
        log.info("Cancelling Aria2 download GID %s", self.gid)
        try:
            # RPC call is run in a separate thread to prevent blocking
            sync_rpc_call(self.port, "aria2.forceRemove", [self.gid])
        except Exception as e:
            log.warning("Failed to forceRemove GID %s: %s", self.gid, e)

    def terminate(self):
        self.kill()


def get_free_port() -> int:
    """Find a free local TCP port dynamically."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def sync_rpc_call(port: int, method: str, params: list) -> dict:
    """Synchronous JSON-RPC client call using urllib, bypassing system proxies."""
    url = f"http://127.0.0.1:{port}/jsonrpc"
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


async def start_aria2_daemon() -> None:
    """Launch the global aria2c RPC daemon."""
    global ARIA2_PORT, ARIA2_PROC
    if ARIA2_PROC is not None:
        return  # Already running

    if shutil.which("aria2c") is None:
        log.warning("aria2c is not installed. Torrent downloads will fail.")
        return

    port = get_free_port()
    tracker_arg = f"--bt-tracker={','.join(TRACKERS)}"
    
    # Create logs directory under data if it exists or use default ./logs
    log_dir = Path("./logs").resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "aria2c_daemon.log"

    cmd = [
        "aria2c",
        "--enable-rpc",
        "--rpc-listen-all=true",
        f"--rpc-listen-port={port}",
        f"--log={log_file}",
        "--log-level=notice",
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

    log.info("Launching global aria2c RPC daemon on port %s...", port)
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        ARIA2_PROC = proc
        ARIA2_PORT = port
    except Exception as e:
        log.exception("Failed to start global aria2c daemon")
        return

    # Wait up to 5 seconds for the daemon to start listening
    daemon_ready = False
    for _ in range(25):
        try:
            await async_rpc_call(port, "aria2.getVersion", [])
            daemon_ready = True
            break
        except Exception:
            await asyncio.sleep(0.2)

    if not daemon_ready:
        log.error("Global aria2c daemon failed to start listening.")
        await stop_aria2_daemon()
    else:
        log.info("Global aria2c daemon successfully initialized on port %s", port)


async def stop_aria2_daemon() -> None:
    """Terminate the global aria2c RPC daemon."""
    global ARIA2_PORT, ARIA2_PROC
    if ARIA2_PROC:
        log.info("Stopping global aria2c RPC daemon...")
        try:
            ARIA2_PROC.terminate()
            await ARIA2_PROC.wait()
        except Exception:
            pass
    ARIA2_PROC = None
    ARIA2_PORT = None


async def download_torrent_async(
    torrent_or_magnet: str,
    dest_dir: Path,
    on_progress: Optional[Callable[[float, float, float, Optional[str]], None]] = None,
    register_proc: Optional[Callable[[Optional[Aria2DownloadTask]], None]] = None,
) -> DownloadResult:
    """Download a torrent or magnet link asynchronously using the global aria2c RPC daemon."""
    global ARIA2_PORT, ARIA2_PROC
    if ARIA2_PROC is None or ARIA2_PORT is None:
        await start_aria2_daemon()
        if ARIA2_PROC is None or ARIA2_PORT is None:
            return DownloadResult(ok=False, error_tail="aria2c RPC daemon is not running.")

    port = ARIA2_PORT
    proc = ARIA2_PROC

    target = torrent_or_magnet
    is_torrent_file = target.startswith("torrent:") or target.endswith(".torrent")
    if target.startswith("torrent:"):
        target = target[len("torrent:"):]

    # Add the download via RPC
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
            
        if "error" in response:
            raise Exception(response["error"].get("message", "unknown error"))
            
        gid = response.get("result")
    except Exception as e:
        log.exception("Failed to add download to aria2c RPC daemon")
        return DownloadResult(ok=False, error_tail=f"Failed to add download to daemon: {e}")

    if not gid:
        return DownloadResult(ok=False, error_tail="aria2c daemon did not return a GID.")

    # Register mock task for cancellation
    task_wrapper = Aria2DownloadTask(port, gid)
    if register_proc:
        register_proc(task_wrapper)

    ok = False
    error_tail = ""
    try:
        while True:
            # Check if global daemon subprocess is still running
            if proc.returncode is not None:
                log.error("Global aria2c daemon stopped unexpectedly with code %s", proc.returncode)
                break

            try:
                response = await async_rpc_call(port, "aria2.tellStatus", [gid])
                if "error" in response:
                    raise Exception(response["error"].get("message", "unknown error"))
                result = response.get("result", {})
            except Exception as e:
                log.error("Failed to query status from aria2c daemon (GID %s): %s", gid, e)
                await asyncio.sleep(2.0)
                continue

            # Handle followedBy transition for magnet links downloading metadata (check before status == "complete")
            followed_by = result.get("followedBy")
            if followed_by and len(followed_by) > 0:
                log.info("Download transitioned to new GID: %s -> %s", gid, followed_by[0])
                gid = followed_by[0]
                task_wrapper.gid = gid  # Update mock task GID for cancellation
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

            # Calculate progress and extract torrent name
            completed_len = float(result.get("completedLength", 0))
            total_len = float(result.get("totalLength", 0))
            speed = float(result.get("downloadSpeed", 0))

            pct = (completed_len * 100.0 / total_len) if total_len > 0 else 0.0

            # Extract resolved torrent name
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
        # Remove task from aria2c to clean up memory
        try:
            await async_rpc_call(port, "aria2.removeDownloadResult", [gid])
        except Exception:
            pass

    files = []
    if ok and dest_dir.exists():
        files = [
            p for p in dest_dir.rglob("*")
            if p.is_file() and not p.name.endswith(".part") and not p.name.endswith(".aria2")
        ]

    return DownloadResult(ok=ok, files=files, error_tail=error_tail, attempts=1)
