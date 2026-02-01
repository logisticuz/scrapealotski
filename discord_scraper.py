import discord
import os
import aiohttp
import asyncio
import json
import time
import sys
from datetime import datetime, timedelta, timezone
from storage_handler import upload_file
from config import (
    DISCORD_BOT_TOKEN,
    ENABLE_UPLOADS,
    DOWNLOAD_BACKOFF_SECONDS,
    DOWNLOAD_RETRIES,
    DOWNLOAD_TIMEOUT_SECONDS,
    MAX_ATTACHMENT_MB,
    DEDUPE_INDEX_PATH,
    IMAGE_EXTENSIONS,
    LOG_PATH,
    LOG_TO_FILE,
    LOG_ERRORS_TO_FILE,
    LOG_ERROR_PATH,
    LOG_JSON,
    LOG_JSON_PATH,
    DOCUMENT_EXTENSIONS,
    SCRAPE_BACKFILL,
    SCRAPE_BACKFILL_AUTORUN,
    SCRAPE_BACKFILL_MAX_BATCHES,
    SCRAPE_BACKFILL_SLEEP_SECONDS,
    SCRAPE_BATCH_SIZE,
    SCRAPE_CHANNEL_ID,
    SCRAPE_CHANNEL_IDS,
    SCRAPE_DRY_RUN,
    SCRAPE_LIMIT,
    SCRAPE_METADATA_ONLY,
    SCRAPE_COUNT_ONLY,
    SCRAPE_OUTPUT_DIR,
    SCRAPE_MEDIA_DIR,
    SCRAPE_SINCE_DAYS,
    SCRAPE_STATE_PATH,
    SCRAPE_USE_LAST_RUN,
    UPLOAD_BASE_PATH,
    UPLOAD_IMAGEBANK_PATH,
    VIDEO_EXTENSIONS,
)


SCRAPED_IMAGES_DIR = ""
SCRAPED_VIDEOS_DIR = ""
SCRAPED_DOCS_DIR = ""
SCRAPED_DATA_DIR = ""
STATE_PATH = ""
LOG_PATH_RESOLVED = ""
LOG_ERROR_PATH_RESOLVED = ""
LOG_JSON_PATH_RESOLVED = ""
DEDUPE_INDEX_PATH_RESOLVED = ""

# Discord bot intents (defines what events the bot can listen to)
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True  # Required to read messages
client = discord.Client(intents=intents)

RUN_SCRAPE_BACKFILL = SCRAPE_BACKFILL
RUN_SCRAPE_BATCH_SIZE = SCRAPE_BATCH_SIZE
RUN_SCRAPE_BACKFILL_AUTORUN = SCRAPE_BACKFILL_AUTORUN
RUN_SCRAPE_BACKFILL_SLEEP_SECONDS = SCRAPE_BACKFILL_SLEEP_SECONDS
RUN_SCRAPE_BACKFILL_MAX_BATCHES = SCRAPE_BACKFILL_MAX_BATCHES
RUN_SCRAPE_LIMIT = SCRAPE_LIMIT
RUN_SCRAPE_SINCE_DAYS = SCRAPE_SINCE_DAYS
RUN_SCRAPE_USE_LAST_RUN = SCRAPE_USE_LAST_RUN
RUN_SCRAPE_DRY_RUN = SCRAPE_DRY_RUN
RUN_SCRAPE_METADATA_ONLY = SCRAPE_METADATA_ONLY
RUN_SCRAPE_COUNT_ONLY = SCRAPE_COUNT_ONLY
RUN_SCRAPE_OUTPUT_DIR = SCRAPE_OUTPUT_DIR
RUN_SCRAPE_MEDIA_DIR = SCRAPE_MEDIA_DIR
RUN_SCRAPE_CHANNEL_IDS = SCRAPE_CHANNEL_IDS


def _resolve_output_path(path, base_dir=None):
    base = base_dir if base_dir is not None else RUN_SCRAPE_OUTPUT_DIR
    if not base:
        return path
    if os.path.isabs(path):
        return path
    return os.path.join(base, path)


def _resolve_state_path(channel_id):
    template = SCRAPE_STATE_PATH
    if "{channel_id}" in template:
        resolved = template.format(channel_id=channel_id)
    elif template == "scrape_state.json":
        resolved = f"scrape_state_{channel_id}.json"
    else:
        resolved = template
    return _resolve_output_path(resolved)


def _init_output_paths(channel_id):
    global SCRAPED_IMAGES_DIR
    global SCRAPED_VIDEOS_DIR
    global SCRAPED_DOCS_DIR
    global SCRAPED_DATA_DIR
    global STATE_PATH
    global LOG_PATH_RESOLVED
    global LOG_ERROR_PATH_RESOLVED
    global LOG_JSON_PATH_RESOLVED
    global DEDUPE_INDEX_PATH_RESOLVED

    media_base = RUN_SCRAPE_MEDIA_DIR or RUN_SCRAPE_OUTPUT_DIR
    SCRAPED_IMAGES_DIR = _resolve_output_path(
        os.path.join("scraped_images", str(channel_id)),
        media_base,
    )
    SCRAPED_VIDEOS_DIR = _resolve_output_path(
        os.path.join("scraped_videos", str(channel_id)),
        media_base,
    )
    SCRAPED_DOCS_DIR = _resolve_output_path(
        os.path.join("scraped_documents", str(channel_id)),
        media_base,
    )
    SCRAPED_DATA_DIR = _resolve_output_path("scraped_data")
    STATE_PATH = _resolve_state_path(channel_id)
    LOG_PATH_RESOLVED = _resolve_output_path(LOG_PATH)
    LOG_ERROR_PATH_RESOLVED = _resolve_output_path(LOG_ERROR_PATH)
    LOG_JSON_PATH_RESOLVED = _resolve_output_path(LOG_JSON_PATH)
    DEDUPE_INDEX_PATH_RESOLVED = _resolve_output_path(DEDUPE_INDEX_PATH)

    os.makedirs(SCRAPED_IMAGES_DIR, exist_ok=True)
    os.makedirs(SCRAPED_VIDEOS_DIR, exist_ok=True)
    os.makedirs(SCRAPED_DOCS_DIR, exist_ok=True)
    os.makedirs(SCRAPED_DATA_DIR, exist_ok=True)


def _write_log_line(path, line):
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


def _log(message):
    timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} {message}"
    print(line)
    if not LOG_TO_FILE:
        return
    _write_log_line(LOG_PATH_RESOLVED, line)


def _log_error(message):
    _log(message)
    if not LOG_ERRORS_TO_FILE:
        return
    timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    _write_log_line(LOG_ERROR_PATH_RESOLVED, f"{timestamp} {message}")


def _log_json(event, payload):
    if not LOG_JSON:
        return
    record = {
        "timestamp": datetime.now().astimezone().isoformat(),
        "event": event,
        "payload": payload,
    }
    with open(LOG_JSON_PATH_RESOLVED, "a", encoding="utf-8") as handle:
        handle.write(f"{json.dumps(record, ensure_ascii=False)}\n")


def _load_dedupe_index(path):
    index = set()
    if not os.path.exists(path):
        return index
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                attachment_id = record.get("attachment_id")
                if attachment_id is None:
                    continue
                try:
                    index.add(int(attachment_id))
                except (TypeError, ValueError):
                    continue
    except OSError as exc:
        _log_error(f"⚠️ Failed to read dedupe index: {exc}")
    return index


def _append_dedupe_entry(path, attachment_id, filename):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    record = {
        "attachment_id": int(attachment_id),
        "filename": filename,
    }
    try:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(f"{json.dumps(record, ensure_ascii=False)}\n")
    except OSError as exc:
        _log_error(f"⚠️ Failed to write dedupe index: {exc}")


def _merge_stats(total, stats):
    for key, value in stats.items():
        if not isinstance(value, (int, float)):
            continue
        total[key] = total.get(key, 0) + value


def _prompt_int(label, default):
    value = input(f"{label} [{default}]: ").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        print("Invalid number, keeping default.")
        return default


def _prompt_int_csv(label, default):
    display_default = ",".join(str(value) for value in default) if default else ""
    value = input(f"{label} [{display_default}]: ").strip()
    if not value:
        return default
    values = []
    for item in value.split(","):
        item = item.strip()
        if item.isdigit():
            values.append(int(item))
    return values or default


def _prompt_bool(label, default):
    default_label = "Y/n" if default else "y/N"
    value = input(f"{label} ({default_label}): ").strip().lower()
    if not value:
        return default
    return value in {"y", "yes", "true", "1"}


def _prompt_path(label, default):
    display_default = default or "(project folder)"
    value = input(f"{label} [{display_default}]: ").strip()
    if not value:
        return default
    return value


def _configure_runtime_settings():
    global RUN_SCRAPE_BACKFILL
    global RUN_SCRAPE_BATCH_SIZE
    global RUN_SCRAPE_BACKFILL_AUTORUN
    global RUN_SCRAPE_BACKFILL_SLEEP_SECONDS
    global RUN_SCRAPE_BACKFILL_MAX_BATCHES
    global RUN_SCRAPE_LIMIT
    global RUN_SCRAPE_SINCE_DAYS
    global RUN_SCRAPE_USE_LAST_RUN
    global RUN_SCRAPE_DRY_RUN
    global RUN_SCRAPE_METADATA_ONLY
    global RUN_SCRAPE_COUNT_ONLY
    global RUN_SCRAPE_OUTPUT_DIR
    global RUN_SCRAPE_MEDIA_DIR
    global RUN_SCRAPE_CHANNEL_IDS

    if not sys.stdin.isatty():
        return
    print("Select mode:")
    print("  1) Latest (new messages)")
    print("  2) Backfill (older history)")
    choice = input("Mode [1]: ").strip() or "1"
    if choice == "2":
        RUN_SCRAPE_BACKFILL = True
        RUN_SCRAPE_BATCH_SIZE = _prompt_int(
            "Backfill batch size", RUN_SCRAPE_BATCH_SIZE
        )
        RUN_SCRAPE_BACKFILL_AUTORUN = _prompt_bool(
            "Auto-run multiple batches", RUN_SCRAPE_BACKFILL_AUTORUN
        )
        if RUN_SCRAPE_BACKFILL_AUTORUN:
            RUN_SCRAPE_BACKFILL_SLEEP_SECONDS = _prompt_int(
                "Sleep seconds between batches", RUN_SCRAPE_BACKFILL_SLEEP_SECONDS
            )
            RUN_SCRAPE_BACKFILL_MAX_BATCHES = _prompt_int(
                "Max batches per run (0 = unlimited)", RUN_SCRAPE_BACKFILL_MAX_BATCHES
            )
        RUN_SCRAPE_LIMIT = 0
        RUN_SCRAPE_SINCE_DAYS = 0
        RUN_SCRAPE_USE_LAST_RUN = False
    else:
        RUN_SCRAPE_BACKFILL = False
        RUN_SCRAPE_LIMIT = _prompt_int("Limit (0 = no limit)", RUN_SCRAPE_LIMIT)
        RUN_SCRAPE_SINCE_DAYS = _prompt_int(
            "Since days (0 = no filter)", RUN_SCRAPE_SINCE_DAYS
        )
        RUN_SCRAPE_USE_LAST_RUN = _prompt_bool(
            "Use last-run cursor", RUN_SCRAPE_USE_LAST_RUN
        )
    RUN_SCRAPE_CHANNEL_IDS = _prompt_int_csv(
        "Channel IDs (comma-separated)", RUN_SCRAPE_CHANNEL_IDS
    )
    if RUN_SCRAPE_DRY_RUN:
        RUN_SCRAPE_DRY_RUN = _prompt_bool("Dry run (no downloads)", RUN_SCRAPE_DRY_RUN)
    if RUN_SCRAPE_METADATA_ONLY:
        RUN_SCRAPE_METADATA_ONLY = _prompt_bool(
            "Metadata only (skip downloads)", RUN_SCRAPE_METADATA_ONLY
        )
    if RUN_SCRAPE_COUNT_ONLY:
        RUN_SCRAPE_COUNT_ONLY = _prompt_bool(
            "Count only (no downloads, no metadata)", RUN_SCRAPE_COUNT_ONLY
        )
    if RUN_SCRAPE_OUTPUT_DIR:
        RUN_SCRAPE_OUTPUT_DIR = _prompt_path(
            "Output folder", RUN_SCRAPE_OUTPUT_DIR
        )
    RUN_SCRAPE_MEDIA_DIR = _prompt_path(
        "Media folder (images/videos)", RUN_SCRAPE_MEDIA_DIR
    )


async def _validate_channel_access(channel_id):
    channel = client.get_channel(channel_id)
    if channel is None:
        raise RuntimeError(f"Channel not found for ID {channel_id}.")
    try:
        async for _ in channel.history(limit=1):
            break
    except discord.Forbidden as exc:
        raise RuntimeError(
            "Missing permissions to read channel history. "
            "Ensure View Channels + Read Message History."
        ) from exc
    except discord.HTTPException as exc:
        raise RuntimeError(f"Failed to read channel history: {exc}") from exc
    if not client.intents.message_content:
        _log_error("⚠️ Message Content Intent is disabled; message content may be missing.")


async def _upload_file_async(local_path, cloud_path):
    if not ENABLE_UPLOADS:
        return
    await asyncio.to_thread(upload_file, local_path, cloud_path)


def _uses_state():
    return RUN_SCRAPE_USE_LAST_RUN or RUN_SCRAPE_BACKFILL


def _load_state(path):
    if not _uses_state():
        return {}
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def _save_state(path, state):
    if not _uses_state():
        return
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=4)


def _get_last_run(state):
    if not RUN_SCRAPE_USE_LAST_RUN:
        return None
    timestamp = state.get("last_run")
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _set_last_run(state, timestamp):
    if not RUN_SCRAPE_USE_LAST_RUN:
        return
    state["last_run"] = timestamp.isoformat()


def _get_backfill_state(state):
    if not RUN_SCRAPE_BACKFILL:
        return None, False
    before_id = state.get("backfill_before_id")
    complete = bool(state.get("backfill_complete", False))
    if before_id is not None:
        try:
            before_id = int(before_id)
        except (TypeError, ValueError):
            before_id = None
    return before_id, complete


def _set_backfill_state(state, before_id, complete):
    if not RUN_SCRAPE_BACKFILL:
        return
    state["backfill_complete"] = complete
    if before_id is None:
        state.pop("backfill_before_id", None)
    else:
        state["backfill_before_id"] = int(before_id)


def _ensure_state_path(path):
    if not _uses_state():
        return
    directory = os.path.dirname(path)
    if directory and not os.path.isdir(directory):
        raise RuntimeError(f"SCRAPE_STATE_PATH directory does not exist: {directory}")
    try:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write("")
    except OSError as exc:
        raise RuntimeError(f"SCRAPE_STATE_PATH is not writable: {path}") from exc

# Function to download and save attachments (images, files, etc.)
async def download_attachment(session, url, filename):
    if RUN_SCRAPE_COUNT_ONLY:
        return True
    if RUN_SCRAPE_METADATA_ONLY:
        _log(f"🗒️ Metadata only, skipping download: {filename}")
        return True
    if RUN_SCRAPE_DRY_RUN:
        _log(f"🔎 Dry run, would download: {filename}")
        return True
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    with open(filename, "wb") as f:
                        f.write(await resp.read())
                    _log(f"📥 Downloaded: {filename}")
                    cloud_name = os.path.basename(filename)
                    await _upload_file_async(
                        filename,
                        f"{UPLOAD_IMAGEBANK_PATH}/{cloud_name}",
                    )  # Upload to cloud storage
                    return True
                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    wait_time = float(retry_after) if retry_after else DOWNLOAD_BACKOFF_SECONDS
                    _log(f"⏳ Rate limited ({resp.status}), retrying in {wait_time}s: {filename}")
                    await asyncio.sleep(wait_time)
                    continue
                _log_error(f"⚠️ Download failed ({resp.status}): {filename}")
                if resp.status >= 500 and attempt < DOWNLOAD_RETRIES:
                    await asyncio.sleep(DOWNLOAD_BACKOFF_SECONDS * attempt)
                    continue
                return False
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            _log_error(f"⚠️ Download error ({exc.__class__.__name__}): {filename}")
            if attempt < DOWNLOAD_RETRIES:
                await asyncio.sleep(DOWNLOAD_BACKOFF_SECONDS * attempt)
                continue
            return False
    return False

# Function to scrape messages and images from a given channel
async def scrape_messages(
    channel_id,
    limit=None,
    after=None,
    before=None,
    dedupe_index=None,
    max_bytes=0,
):
    channel = client.get_channel(channel_id)
    if channel is None:
        raise RuntimeError(f"Channel not found for ID {channel_id}.")
    messages = []
    message_count = 0
    message_error_count = 0
    attachment_count = 0
    attachment_success = 0
    attachment_fail = 0
    attachment_skipped_dedupe = 0
    attachment_skipped_size = 0
    oldest_id = None
    oldest_timestamp = None

    timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async for message in channel.history(limit=limit, after=after, before=before):
            try:
                msg_data = {
                    "author": message.author.name,
                    "content": message.content,
                    "timestamp": str(message.created_at),
                    "attachments": [],
                    "errors": [],
                }

                # Download and store images separately for marketing/image bank
                for attachment in message.attachments:
                    attachment_name = attachment.filename.lower()
                    is_image = any(attachment_name.endswith(ext) for ext in IMAGE_EXTENSIONS)
                    is_video = any(attachment_name.endswith(ext) for ext in VIDEO_EXTENSIONS)
                    is_doc = any(attachment_name.endswith(ext) for ext in DOCUMENT_EXTENSIONS)
                    if is_image:
                        filename = os.path.join(
                            SCRAPED_IMAGES_DIR,
                            f"{attachment.id}_{attachment.filename}",
                        )
                    elif is_video:
                        filename = os.path.join(
                            SCRAPED_VIDEOS_DIR,
                            f"{attachment.id}_{attachment.filename}",
                        )
                    elif is_doc:
                        filename = os.path.join(
                            SCRAPED_DOCS_DIR,
                            f"{attachment.id}_{attachment.filename}",
                        )
                    else:
                        continue
                    if dedupe_index is not None and attachment.id in dedupe_index:
                        attachment_skipped_dedupe += 1
                        continue
                    if max_bytes > 0 and attachment.size > max_bytes:
                        size_mb = attachment.size / (1024 * 1024)
                        _log(
                            f"⏭️ Skipping large file ({size_mb:.1f} MB): {filename}"
                        )
                        attachment_skipped_size += 1
                        continue
                    attachment_count += 1
                    success = await download_attachment(session, attachment.url, filename)
                    if success:
                        msg_data["attachments"].append(filename)
                        attachment_success += 1
                        if (
                            dedupe_index is not None
                            and not RUN_SCRAPE_METADATA_ONLY
                            and not RUN_SCRAPE_DRY_RUN
                            and not RUN_SCRAPE_COUNT_ONLY
                        ):
                            dedupe_index.add(attachment.id)
                            _append_dedupe_entry(
                                DEDUPE_INDEX_PATH_RESOLVED,
                                attachment.id,
                                filename,
                            )
                    else:
                        msg_data["errors"].append(
                            {
                                "filename": attachment.filename,
                                "url": attachment.url,
                            }
                        )
                        attachment_fail += 1

                messages.append(msg_data)
                message_count += 1
                oldest_id = message.id
                oldest_timestamp = message.created_at
            except Exception as exc:
                _log_error(f"⚠️ Message skipped due to error: {exc}")
                message_error_count += 1
                continue
    
    if not RUN_SCRAPE_COUNT_ONLY:
        # Save messages to a JSON file
        run_stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(
            SCRAPED_DATA_DIR,
            f"{channel_id}_{run_stamp}.json",
        )
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(messages, f, ensure_ascii=False, indent=4)

        _log(f"✅ Saved {len(messages)} messages from channel {channel_id}!")
        cloud_relative = f"scraped_data/{os.path.basename(output_file)}"
        await _upload_file_async(
            output_file,
            f"{UPLOAD_BASE_PATH}/{cloud_relative}",
        )  # Upload JSON file to cloud storage
    stats = {
        "messages": message_count,
        "message_errors": message_error_count,
        "attachments": attachment_count,
        "attachments_ok": attachment_success,
        "attachments_failed": attachment_fail,
        "attachments_skipped_dedupe": attachment_skipped_dedupe,
        "attachments_skipped_size": attachment_skipped_size,
        "oldest_timestamp": oldest_timestamp.isoformat() if oldest_timestamp else None,
    }
    return stats, oldest_id

# Event handler: Runs when the bot connects to Discord
@client.event
async def on_ready():
    channels = RUN_SCRAPE_CHANNEL_IDS or ([SCRAPE_CHANNEL_ID] if SCRAPE_CHANNEL_ID else [])
    if not channels:
        raise RuntimeError("SCRAPE_CHANNEL_ID is not set.")
    _log(f"Bot is online as {client.user}")
    max_bytes = MAX_ATTACHMENT_MB * 1024 * 1024 if MAX_ATTACHMENT_MB > 0 else 0
    for index, channel_id in enumerate(channels, start=1):
        _log(f"▶️ Processing channel {index}/{len(channels)}: {channel_id}")
        _init_output_paths(channel_id)
        _ensure_state_path(STATE_PATH)
        await _validate_channel_access(channel_id)
        state = _load_state(STATE_PATH)
        dedupe_index = _load_dedupe_index(DEDUPE_INDEX_PATH_RESOLVED)
        total_stats = {
            "messages": 0,
            "message_errors": 0,
            "attachments": 0,
            "attachments_ok": 0,
            "attachments_failed": 0,
            "attachments_skipped_dedupe": 0,
            "attachments_skipped_size": 0,
        }
        if dedupe_index:
            _log(f"ℹ️ Loaded dedupe index with {len(dedupe_index)} entries.")
        if RUN_SCRAPE_BACKFILL:
            batches = 0
            while True:
                before_id, backfill_complete = _get_backfill_state(state)
                if backfill_complete:
                    _log("✅ Backfill complete, no older messages to scrape.")
                    break
                limit = RUN_SCRAPE_BATCH_SIZE if RUN_SCRAPE_BATCH_SIZE > 0 else 200
                _log(f"▶️ Backfill batch {batches + 1} starting (limit={limit}).")
                started_at = time.monotonic()
                stats, oldest_id = await scrape_messages(
                    channel_id,
                    limit=limit,
                    after=None,
                    before=discord.Object(id=before_id) if before_id else None,
                    dedupe_index=dedupe_index,
                    max_bytes=max_bytes,
                )
                elapsed = time.monotonic() - started_at
                _log(
                    "✅ Batch done: "
                    f"messages={stats['messages']} attachments={stats['attachments']} "
                    f"ok={stats['attachments_ok']} failed={stats['attachments_failed']} "
                    f"skipped_dedupe={stats['attachments_skipped_dedupe']} "
                    f"skipped_size={stats['attachments_skipped_size']} "
                    f"message_errors={stats['message_errors']} duration={elapsed:.1f}s "
                    f"oldest={stats['oldest_timestamp']}"
                )
                _log_json(
                    "batch_complete",
                    {
                        "mode": "backfill",
                        "channel_id": channel_id,
                        "batch": batches + 1,
                        "messages": stats["messages"],
                        "message_errors": stats["message_errors"],
                        "attachments": stats["attachments"],
                        "attachments_ok": stats["attachments_ok"],
                        "attachments_failed": stats["attachments_failed"],
                        "attachments_skipped_dedupe": stats["attachments_skipped_dedupe"],
                        "attachments_skipped_size": stats["attachments_skipped_size"],
                        "oldest_timestamp": stats["oldest_timestamp"],
                        "duration_seconds": round(elapsed, 2),
                    },
                )
                _merge_stats(total_stats, stats)
                if stats["messages"] == 0:
                    _log("✅ Backfill complete, no older messages to scrape.")
                    _set_backfill_state(state, None, True)
                    _save_state(STATE_PATH, state)
                    break
                _set_backfill_state(state, oldest_id, False)
                _save_state(STATE_PATH, state)
                _log(f"ℹ️ Backfill cursor saved: {oldest_id}")
                batches += 1
                if not RUN_SCRAPE_BACKFILL_AUTORUN:
                    break
                if RUN_SCRAPE_BACKFILL_MAX_BATCHES > 0 and batches >= RUN_SCRAPE_BACKFILL_MAX_BATCHES:
                    _log("ℹ️ Backfill max batches reached, stopping.")
                    break
                _log(f"⏸️ Sleeping {RUN_SCRAPE_BACKFILL_SLEEP_SECONDS}s before next batch.")
                await asyncio.sleep(RUN_SCRAPE_BACKFILL_SLEEP_SECONDS)
        else:
            limit = RUN_SCRAPE_LIMIT if RUN_SCRAPE_LIMIT > 0 else None
            after_candidates = []
            if RUN_SCRAPE_SINCE_DAYS > 0:
                after_candidates.append(
                    datetime.now(timezone.utc) - timedelta(days=RUN_SCRAPE_SINCE_DAYS)
                )
            if RUN_SCRAPE_USE_LAST_RUN:
                last_run = _get_last_run(state)
                if last_run is not None:
                    after_candidates.append(last_run)
            after = max(after_candidates) if after_candidates else None
            _log("▶️ Scrape run starting.")
            started_at = time.monotonic()
            stats, _ = await scrape_messages(
                channel_id,
                limit=limit,
                after=after,
                before=None,
                dedupe_index=dedupe_index,
                max_bytes=max_bytes,
            )
            elapsed = time.monotonic() - started_at
            _log(
                "✅ Run done: "
                f"messages={stats['messages']} attachments={stats['attachments']} "
                f"ok={stats['attachments_ok']} failed={stats['attachments_failed']} "
                f"skipped_dedupe={stats['attachments_skipped_dedupe']} "
                f"skipped_size={stats['attachments_skipped_size']} "
                f"message_errors={stats['message_errors']} duration={elapsed:.1f}s "
                f"oldest={stats['oldest_timestamp']}"
            )
            _log_json(
                "run_complete",
                {
                    "mode": "latest",
                    "channel_id": channel_id,
                    "messages": stats["messages"],
                    "message_errors": stats["message_errors"],
                    "attachments": stats["attachments"],
                    "attachments_ok": stats["attachments_ok"],
                    "attachments_failed": stats["attachments_failed"],
                    "attachments_skipped_dedupe": stats["attachments_skipped_dedupe"],
                    "attachments_skipped_size": stats["attachments_skipped_size"],
                    "oldest_timestamp": stats["oldest_timestamp"],
                    "duration_seconds": round(elapsed, 2),
                },
            )
            _merge_stats(total_stats, stats)
            _set_last_run(state, datetime.now(timezone.utc))
            _save_state(STATE_PATH, state)
        _log(
            "🏁 Summary: "
            f"messages={total_stats['messages']} attachments={total_stats['attachments']} "
            f"ok={total_stats['attachments_ok']} failed={total_stats['attachments_failed']} "
            f"skipped_dedupe={total_stats['attachments_skipped_dedupe']} "
            f"skipped_size={total_stats['attachments_skipped_size']} "
            f"message_errors={total_stats['message_errors']}"
        )
        _log_json(
            "run_summary",
            {
                "mode": "backfill" if RUN_SCRAPE_BACKFILL else "latest",
                "channel_id": channel_id,
                "messages": total_stats["messages"],
                "message_errors": total_stats["message_errors"],
                "attachments": total_stats["attachments"],
                "attachments_ok": total_stats["attachments_ok"],
                "attachments_failed": total_stats["attachments_failed"],
                "attachments_skipped_dedupe": total_stats["attachments_skipped_dedupe"],
                "attachments_skipped_size": total_stats["attachments_skipped_size"],
            },
        )
    await client.close()

# Start the Discord bot
_configure_runtime_settings()
client.run(DISCORD_BOT_TOKEN)
