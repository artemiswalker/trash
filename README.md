# tgdl-bot

A minimal Telegram bot that downloads media albums, videos, torrents, and Google Drive folders via `gallery-dl`, `aria2c`, and `Google Drive API`, uploading them directly to Telegram. Built on Kurigram (pyrogram fork) to support up to 2GB uploads.

---

## Features
- **Concurrent Pipeline**: Downloads and uploads run in parallel to minimize latency.
- **Google Drive to Telegram (`/gd2tg`)**: Download Google Drive files/folders directly to Telegram with individual folder archiving (`.zip` / `.7z`).
- **Automatic Archiving & Splitting**: Archives downloaded folders into `.zip` or `.7z` format and automatically splits volumes if size exceeds 1.95GB to satisfy Telegram limits.
- **Space Protection**: Completed uploads are deleted immediately to conserve disk space.
- **Auto-Splitting**: Prompts to split video files larger than 1.95GB into sub-2GB segments or skip them.
- **Live Status & Speed**: Real-time progress updates with monospace progress bars and throttled edit protection.
- **Multi-URL Downloads**: Process a list of space-separated links or a `.txt` links file reply sequentially.
- **Timeline Screenshots**: Generates and uploads timeline screenshots grouped in a separate album after the main video.
- **Torrent/Magnet Support**: Download torrents or magnet links headless using `aria2c` with custom speed parsing.
- **Lossless Media Transcoding**: Interactive video container conversions to MP4 (using FFmpeg stream copy) and image transcoding of WebP, BMP, HEIC, etc., to PNG for inline photo display.
- **Interactive Decompression**: Pauses and prompts the user to select extraction choices when zip/rar/7z archives are downloaded.

---

## Usage

### Commands
- `/start` or `/help` — Display welcome message and command instructions.
- `/gd2tg <gdrive_link> [-zip|-7z] [-pd]` — Download a Google Drive link, archive folders individually, and upload to Telegram. Use `-pd` to mirror the original unsplit archives to Pixeldrain.
- `/gdl` — Process replied `.txt` links files.
- `/tor` — Download magnet/torrent links or reply to a `.torrent` file.
- `/unzip` — Reply to a compressed archive file to extract and upload its contents.
- `/pdup` — Reply to a media file to upload it directly to Pixeldrain.
- `/status` — View active job status or queue state.
- `/cancel` — Instantly abort the active job or cancel queued jobs.

### Input Formats
- **Google Drive**: `/gd2tg https://drive.google.com/drive/folders/... -zip -pd` (or `-7z`)
- **Single URL**: `https://example.com/album1`
- **With Shorthands**: `https://example.com/album1 pages=1-16`
- **Multiple URLs**: `https://example.com/album1 https://example.com/album2`
- **Links File (.txt)**: Reply to a `.txt` file containing URLs (one per line) with `/gdl`.
- **Torrents**: `/tor magnet:?xt=urn:...` or reply to a `.torrent` file with `/tor`.
- **Archive Extract**: Reply to any `.zip`, `.rar`, `.7z`, etc., file with `/unzip`.

---

## Google Drive Setup Guide (First Time Setup)

To use `/gd2tg`, you need to provide Google Drive credentials under `auth/<user_id>/` (per-user credential isolation). You can use either **Service Accounts** or an **OAuth User Token**.

### Method 1: Service Accounts (`auth/<user_id>/accounts/*.json`) [Recommended - 0 Browser Steps & 0 VPS Access Needed]
Service Accounts bypass Google Drive's 750 GB/day download quota per account and require **zero browser login steps**:
1. In your Google Cloud Console project, go to **IAM & Admin > Service Accounts**.
2. Click **Create Service Account**, fill in a name, and click **Create and Continue**.
3. Under **Keys**, click **Add Key > Create new key** and choose **JSON**.
4. **Interactive In-Chat Setup**: Upload your Service Account `.json` file (any filename) to the Telegram bot chat and **reply to it with `/gd2tg`**. The bot automatically saves it to `auth/<user_id>/accounts/`!

### Method 2: Upload `token.pickle` in Chat (`auth/<user_id>/token.pickle`) [0 VPS Access Needed]
If you generate or possess a `token.pickle` file:
1. Upload `token.pickle` directly to the Telegram bot chat.
2. **Reply to the `token.pickle` file with `/gd2tg`**.
3. The bot automatically saves it to `auth/<user_id>/token.pickle`!

### Method 3: Global Credentials (Set up by Bot Owner)
If the bot owner places global credentials in `auth/token.pickle` or `auth/accounts/*.json` on the server:
- All users can use `/gd2tg <gdrive_link>` directly without uploading any files!


---

## Setup & Running

### Prerequisites
- Python 3.12+
- `ffmpeg` & `ffprobe` (for video transcoding, screenshots, and metadata probing)
- `aria2c` (for torrent and magnet link downloads)
- `unzip`, `unrar`, `7z` (for archive extraction)

### Getting Started
1. Copy `.env.example` to `.env` and fill in `TG_API_ID`, `TG_API_HASH`, and `TG_BOT_TOKEN`.
2. Install dependencies and run:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   python -m app.bot
   ```

### Docker
```bash
docker compose up -d --build
```
