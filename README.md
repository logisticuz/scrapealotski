# Media Scraper

## Purpose
Collect images and video from selected Discord channels (for example #live-footage)
and store them in a clean, searchable archive.

## MVP behavior
- Run on demand with channel and date filters.
- Save each file plus metadata (uploader, channel, timestamp).
- Optional upload to Dropbox or GCS.

## Output files (example)
- img_001.jpg
- vid_001.mp4
- metadata_<timestamp>.json

## Quick start (local archive)
1) Copy `.env.example` to `.env`.
2) Set `DISCORD_BOT_TOKEN` and `SCRAPE_CHANNEL_ID`.
3) Install deps: `pip install -r requirements.txt`.
4) Run `python discord_scraper.py`.

Optional uploads:
- Install cloud deps with `pip install -r requirements-optional.txt`.

## Interactive menu
When you run in a terminal, you get a simple menu to pick "latest" vs "backfill" and override limits for that run, including the output folder. Defaults come from `.env`, so you can just hit enter to accept them.

## Bot setup (Discord)
1) Discord Developer Portal -> Applications -> New Application.
2) Bot tab -> Add Bot -> copy token to `DISCORD_BOT_TOKEN`.
3) Enable `Message Content Intent` under Privileged Gateway Intents.
4) OAuth2 -> URL Generator -> scope `bot`.
5) Permissions: `View Channels` + `Read Message History`.
6) Open the generated URL to invite the bot.

## Scraper settings
- `SCRAPE_LIMIT`: Max messages per run (0 = no limit).
- `SCRAPE_SINCE_DAYS`: Only scrape recent messages (0 = no date filter).
- `SCRAPE_USE_LAST_RUN`: If true, only scrape new messages since last run.
- `SCRAPE_STATE_PATH`: Where the last-run timestamp is stored.
- `SCRAPE_OUTPUT_DIR`: Base output folder (images, videos, data, logs).
- `SCRAPE_MEDIA_DIR`: Optional folder for images/videos only.
- `UPLOAD_BASE_PATH`: Cloud base path (only used if uploads enabled).
- `UPLOAD_IMAGEBANK_PATH`: Cloud images path (only used if uploads enabled).
- `SCRAPE_BACKFILL`: Enable backfill mode to scrape older history in batches.
- `SCRAPE_BATCH_SIZE`: Messages per run when backfilling.
- `SCRAPE_BACKFILL_AUTORUN`: Run multiple backfill batches in one run.
- `SCRAPE_BACKFILL_SLEEP_SECONDS`: Pause between backfill batches.
- `SCRAPE_BACKFILL_MAX_BATCHES`: Cap batches per run (0 = unlimited).
- `SCRAPE_DRY_RUN`: Log what would be downloaded without saving files.
- `SCRAPE_METADATA_ONLY`: Save metadata JSON but skip file downloads.
- `LOG_TO_FILE`: Append logs to `LOG_PATH`.
- `LOG_PATH`: Log file location when `LOG_TO_FILE=true`.
- `LOG_ERRORS_TO_FILE`: Append error logs to `LOG_ERROR_PATH`.
- `LOG_ERROR_PATH`: Error log file location.
- `LOG_JSON`: Write JSONL metrics to `LOG_JSON_PATH`.
- `LOG_JSON_PATH`: JSONL metrics file location.
- `DOWNLOAD_RETRIES`: How many times to retry a failed download.
- `DOWNLOAD_BACKOFF_SECONDS`: Base backoff for retries (seconds).
- `DOWNLOAD_TIMEOUT_SECONDS`: Per-download timeout (seconds).
- `MAX_ATTACHMENT_MB`: Skip attachments larger than this (0 = no limit).
- `DEDUPE_INDEX_PATH`: JSONL index file used to avoid duplicate downloads.
- `IMAGE_EXTENSIONS` / `VIDEO_EXTENSIONS`: Comma-separated file extensions.

Backfill tip: set `SCRAPE_BACKFILL=true` and rerun the script multiple times to walk backwards in history. Delete `SCRAPE_STATE_PATH` to start over.

Metadata output: each run writes a timestamped JSON file under `scraped_data/` to avoid overwriting previous batches.
