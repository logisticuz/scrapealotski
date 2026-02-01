# Runbook

## Daily run (latest)
1) Activate venv: `.venv\Scripts\Activate.ps1`
2) Run: `python discord_scraper.py`
3) Choose "Latest" and confirm.

## Backfill run (historical)
1) Activate venv: `.venv\Scripts\Activate.ps1`
2) Run: `python discord_scraper.py`
3) Choose "Backfill" and set batch/sleep/max.

## Resume after interruption
- Just run again with the same mode; the scraper resumes using the channel state file.

## Where files go
- Media: `scraped_images/<channel_id>/` and `scraped_videos/<channel_id>/`
- Documents: `scraped_documents/<channel_id>/`
- Metadata: `scraped_data/<channel_id>_<timestamp>.json`
- Report: `scraped_data/report_<timestamp>.json`

## Common checks
- Missing files? Verify `SCRAPE_MEDIA_DIR` and output folder in the menu.
- Rate limit warnings are normal; the client retries automatically.
- If you need to restart a backfill from scratch, delete the channel state file.
