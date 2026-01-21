import os

try:
    from dotenv import load_dotenv
except ImportError:  # optional dependency
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv(override=True)

# Configuration file for the bot


def _env_bool(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_csv_list(name, default):
    raw = os.getenv(name)
    if raw is None:
        return default
    values = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if not item.startswith("."):
            item = f".{item}"
        values.append(item.lower())
    return values or default


# Cloud storage settings
ENABLE_UPLOADS = _env_bool("ENABLE_UPLOADS", False)
USE_DROPBOX = _env_bool("USE_DROPBOX", False)  # Set to True to use Dropbox uploads
DROPBOX_ACCESS_TOKEN = os.getenv("DROPBOX_ACCESS_TOKEN", "")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "")
GCS_CREDENTIALS_PATH = os.getenv("GCS_CREDENTIALS_PATH", "")

# Discord bot tokens
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")

# Scraper settings
SCRAPE_CHANNEL_ID = _env_int("SCRAPE_CHANNEL_ID", 0)
SCRAPE_LIMIT = _env_int("SCRAPE_LIMIT", 0)
SCRAPE_SINCE_DAYS = _env_int("SCRAPE_SINCE_DAYS", 0)
SCRAPE_USE_LAST_RUN = _env_bool("SCRAPE_USE_LAST_RUN", True)
SCRAPE_STATE_PATH = os.getenv("SCRAPE_STATE_PATH", "scrape_state.json")
SCRAPE_BACKFILL = _env_bool("SCRAPE_BACKFILL", False)
SCRAPE_BATCH_SIZE = _env_int("SCRAPE_BATCH_SIZE", 200)
SCRAPE_BACKFILL_AUTORUN = _env_bool("SCRAPE_BACKFILL_AUTORUN", False)
SCRAPE_BACKFILL_SLEEP_SECONDS = _env_int("SCRAPE_BACKFILL_SLEEP_SECONDS", 10)
SCRAPE_BACKFILL_MAX_BATCHES = _env_int("SCRAPE_BACKFILL_MAX_BATCHES", 0)
SCRAPE_DRY_RUN = _env_bool("SCRAPE_DRY_RUN", False)
SCRAPE_METADATA_ONLY = _env_bool("SCRAPE_METADATA_ONLY", False)
LOG_TO_FILE = _env_bool("LOG_TO_FILE", False)
LOG_PATH = os.getenv("LOG_PATH", "scrape.log")
DOWNLOAD_RETRIES = _env_int("DOWNLOAD_RETRIES", 3)
DOWNLOAD_BACKOFF_SECONDS = _env_int("DOWNLOAD_BACKOFF_SECONDS", 2)
DOWNLOAD_TIMEOUT_SECONDS = _env_int("DOWNLOAD_TIMEOUT_SECONDS", 30)
IMAGE_EXTENSIONS = _env_csv_list(
    "IMAGE_EXTENSIONS",
    [".png", ".jpg", ".jpeg", ".gif"],
)
VIDEO_EXTENSIONS = _env_csv_list(
    "VIDEO_EXTENSIONS",
    [".mp4", ".mov", ".webm", ".mkv", ".avi"],
)
