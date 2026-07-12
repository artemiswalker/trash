# tgdl-bot

A Telegram bot that downloads media albums and videos via `gallery-dl` and uploads them back to your chat. Built on top of Kurigram (pyrogram fork) to bypass the 50MB HTTP Bot API limit and support up to 2GB uploads.

---

## Key Features

### Performance & Concurrency
* **Parallel Pipeline**: Downloads and uploads run concurrently. Uploader task processes completed files while the downloader is still fetching remaining album content.
* **Storage Protection**: Uploaded files are immediately deleted from disk.
* **Startup Pruning**: Obsolete folders from interrupted downloads are automatically cleaned on startup.

### Live Tracking & Status
* **Dynamic Status Updates**: A live text status dashboard tracking downloader progress (items, sizes, and speed) and active upload progress.
* **Visual Progress Bar**: Monospace block bar `[■■■■■□□□□□]` tracking upload progress.
* **Paced Uploads**: Automatic FloodWait handling, batch cooldowns, and randomized delay jitter to prevent Telegram rate limits.

### Advanced Media Handling
* **Grouped Albums**: Generates 9 random screenshots across the video timeline and uploads them together with the video in a single 10-item Telegram album.
* **Metadata Probing**: Uses `ffprobe` to determine video dimensions and duration, ensuring streamable playback and thumbnail rendering.
* **Automatic Captions**: Places the file name in the caption of each upload.

### Chat Integrations
* **Group Chat Optimization**: Silently ignores non-URL chatter in group, supergroup, and channel chats, keeping prompts strictly for private DMs.
* **Resumability**: Leverages `gallery-dl` download archive and an SQLite state store to avoid duplicate downloads and duplicate uploads.
* **Graceful Shutdown**: SIGINT/SIGTERM finishes the current file, marks unfinished work as queued, and exits cleanly.

---

## Setup & Installation

### Prerequisites
* Python 3.12+
* `ffmpeg` and `ffprobe` (for screenshot extraction and metadata probing)

### Local Run

1. Copy the environment template:
   ```bash
   cp .env.example .env
   ```
2. Configure credentials in `.env` (`TG_API_ID` & `TG_API_HASH` from my.telegram.org; `TG_BOT_TOKEN` from @BotFather).
3. Install dependencies:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
4. Run the bot:
   ```bash
   python -m app.bot
   ```

---

## Deployment

### Docker
Run via Docker Compose:
```bash
docker compose up -d --build
docker compose logs -f
```

### systemd (VPS / Linux Host)
```bash
# 1. Create system user
sudo useradd -r -m -d /opt/tgdl-bot tgdlbot

# 2. Copy source files
sudo cp -r . /opt/tgdl-bot
cd /opt/tgdl-bot

# 3. Create virtual environment and install packages
sudo -u tgdlbot python3 -m venv .venv
sudo -u tgdlbot .venv/bin/pip install -r requirements.txt

# 4. Set up the service
sudo cp tgdl-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tgdl-bot

# 5. Monitor service
sudo journalctl -u tgdl-bot -f
```

---

## Bot Usage

### Submitting Downloads
Simply send a media album or video URL to the bot in a chat.

* **Interactive File Splitting**: If a file in the download queue is larger than 1.95GB, the bot will prompt you via inline buttons to choose whether to split it or skip it:
  * **Split**: Large videos are segmented into playable sub-2GB clips using `ffmpeg` copy mode; documents are split into multi-part chunks.
  * **Skip**: The file is deleted from the host and skipped.

### Custom gallery-dl Arguments, Shorthands & Multiple URLs
You can send one or more URLs, or upload a `.txt` links file to download multiple targets simultaneously. Trailing custom options are automatically mapped to the first URL's extractor.
* **Shorthand Option (Recommended)**: Simply pass `option=value` after the URL:
  ```text
  https://example.com/album/aabbbccdd pages=1-16
  ```
* **Multiple URLs**: Send multiple space-separated links directly in the message:
  ```text
  https://example.com/album1 https://example.com/album2
  ```
* **Links File (.txt)**: Send a `.txt` file containing URLs (one per line) and **reply to it** with `/gdl [options]`:
  ```text
  /gdl pages=1-16
  ```
* **Full Syntax Option**: You can also use standard command-line options if preferred:
  ```text
  https://example.com/album/aabbbccdd --extractor-argument example:pages=1-16
  ```
> [!NOTE]
> Options that modify directory layout, output paths, or system config (`-d`, `--directory`, `-o base-directory=...`, `--config`, `-h`, `--help`, `--version`) are automatically stripped out to protect the bot's filesystem.

---

## Bot Commands

* `/start` — Send welcome message and descriptions.
* `/status` — View active job details or queue status.
* `/cancel` — Instantly abort the active job, prune working directories, and mark it cancelled.

---

## Configuration Options

Key configuration parameters in `.env` / `app/config.py`:

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `GDL_SLEEP_MIN` / `GDL_SLEEP_MAX` | `1.0` / `4.0` | Random delay range for gallery-dl extraction to avoid IP blocks. |
| `GDL_MAX_RUN_RETRIES` | `3` | Maximum attempts allowed for downloader run. |
| `GDL_BACKOFF_BASE_S` | `30` | Exponential backoff delay when rate-limited. |
| `TG_UPLOAD_DELAY_MIN` / `TG_UPLOAD_DELAY_MAX` | `1.0` / `3.0` | Jitter spacing between Telegram file uploads. |
| `TG_BATCH_SIZE` | `10` | Number of files sent in one batch before pausing. |
| `TG_BATCH_COOLDOWN_S` | `15` | Delay time in seconds applied between upload batches. |
| `TG_MAX_CONCURRENT_UPLOADS` | `1` | Maximum parallel upload tasks allowed inside MTProto client. |

---

## Important Notes

> [!IMPORTANT]
> **Namespace Conflicts**: Kurigram and the standard `pyrogram` library share the same import namespace. Do not install both packages in the same Python environment.

> [!WARNING]
> **MTProto Bot Limits**: The 2GB upload limit is a strict Telegram MTProto protocol limit for bot accounts. It cannot be bypassed further unless you have a premium account.

---

## Credits

* Assisted by Gemini and Claude.
