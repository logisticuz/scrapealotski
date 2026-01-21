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
- metadata.json

## Quick start (local archive)
1) Copy `.env.example` to `.env`.
2) Set `DISCORD_BOT_TOKEN` and `SCRAPE_CHANNEL_ID`.
3) Run `python discord_scraper.py`.

## Scraper settings
- `SCRAPE_LIMIT`: Max messages per run (0 = no limit).
- `SCRAPE_SINCE_DAYS`: Only scrape recent messages (0 = no date filter).
- `SCRAPE_USE_LAST_RUN`: If true, only scrape new messages since last run.
- `SCRAPE_STATE_PATH`: Where the last-run timestamp is stored.
- `SCRAPE_DRY_RUN`: Log what would be downloaded without saving files.
- `SCRAPE_METADATA_ONLY`: Save metadata JSON but skip file downloads.
- `LOG_TO_FILE`: Append logs to `LOG_PATH`.
- `LOG_PATH`: Log file location when `LOG_TO_FILE=true`.
- `IMAGE_EXTENSIONS` / `VIDEO_EXTENSIONS`: Comma-separated file extensions.
