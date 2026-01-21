import discord
import os
import aiohttp
import asyncio
import json
from datetime import datetime, timedelta, timezone
from storage_handler import upload_file
from config import (
    DISCORD_BOT_TOKEN,
    ENABLE_UPLOADS,
    DOWNLOAD_BACKOFF_SECONDS,
    DOWNLOAD_RETRIES,
    DOWNLOAD_TIMEOUT_SECONDS,
    IMAGE_EXTENSIONS,
    LOG_PATH,
    LOG_TO_FILE,
    SCRAPE_BACKFILL,
    SCRAPE_BACKFILL_AUTORUN,
    SCRAPE_BACKFILL_MAX_BATCHES,
    SCRAPE_BACKFILL_SLEEP_SECONDS,
    SCRAPE_BATCH_SIZE,
    SCRAPE_CHANNEL_ID,
    SCRAPE_DRY_RUN,
    SCRAPE_LIMIT,
    SCRAPE_METADATA_ONLY,
    SCRAPE_SINCE_DAYS,
    SCRAPE_STATE_PATH,
    SCRAPE_USE_LAST_RUN,
    VIDEO_EXTENSIONS,
)

# Ensure required directories exist for storing scraped data
os.makedirs("scraped_images", exist_ok=True)
os.makedirs("scraped_videos", exist_ok=True)
os.makedirs("scraped_data", exist_ok=True)

# Discord bot intents (defines what events the bot can listen to)
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True  # Required to read messages
client = discord.Client(intents=intents)


def _log(message):
    timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} {message}"
    print(line)
    if not LOG_TO_FILE:
        return
    with open(LOG_PATH, "a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


async def _upload_file_async(local_path, cloud_path):
    if not ENABLE_UPLOADS:
        return
    await asyncio.to_thread(upload_file, local_path, cloud_path)


def _uses_state():
    return SCRAPE_USE_LAST_RUN or SCRAPE_BACKFILL


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
    if not SCRAPE_USE_LAST_RUN:
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
    if not SCRAPE_USE_LAST_RUN:
        return
    state["last_run"] = timestamp.isoformat()


def _get_backfill_state(state):
    if not SCRAPE_BACKFILL:
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
    if not SCRAPE_BACKFILL:
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
    if SCRAPE_METADATA_ONLY:
        _log(f"🗒️ Metadata only, skipping download: {filename}")
        return True
    if SCRAPE_DRY_RUN:
        _log(f"🔎 Dry run, would download: {filename}")
        return True
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    with open(filename, "wb") as f:
                        f.write(await resp.read())
                    _log(f"📥 Downloaded: {filename}")
                    await _upload_file_async(
                        filename,
                        f"/DiscordBot/ImageBank/{filename}",
                    )  # Upload to cloud storage
                    return True
                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    wait_time = float(retry_after) if retry_after else DOWNLOAD_BACKOFF_SECONDS
                    _log(f"⏳ Rate limited ({resp.status}), retrying in {wait_time}s: {filename}")
                    await asyncio.sleep(wait_time)
                    continue
                _log(f"⚠️ Download failed ({resp.status}): {filename}")
                if resp.status >= 500 and attempt < DOWNLOAD_RETRIES:
                    await asyncio.sleep(DOWNLOAD_BACKOFF_SECONDS * attempt)
                    continue
                return False
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            _log(f"⚠️ Download error ({exc.__class__.__name__}): {filename}")
            if attempt < DOWNLOAD_RETRIES:
                await asyncio.sleep(DOWNLOAD_BACKOFF_SECONDS * attempt)
                continue
            return False
    return False

# Function to scrape messages and images from a given channel
async def scrape_messages(channel_id, limit=None, after=None, before=None):
    channel = client.get_channel(channel_id)
    if channel is None:
        raise RuntimeError(f"Channel not found for ID {channel_id}.")
    messages = []
    message_count = 0
    oldest_id = None

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
                    if is_image:
                        filename = f"scraped_images/{attachment.id}_{attachment.filename}"
                    elif is_video:
                        filename = f"scraped_videos/{attachment.id}_{attachment.filename}"
                    else:
                        continue
                    success = await download_attachment(session, attachment.url, filename)
                    if success:
                        msg_data["attachments"].append(filename)
                    else:
                        msg_data["errors"].append(
                            {
                                "filename": attachment.filename,
                                "url": attachment.url,
                            }
                        )

                messages.append(msg_data)
                message_count += 1
                oldest_id = message.id
            except Exception as exc:
                _log(f"⚠️ Message skipped due to error: {exc}")
                continue
    
    # Save messages to a JSON file
    run_stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    output_file = f"scraped_data/{channel_id}_{run_stamp}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False, indent=4)

    _log(f"✅ Saved {len(messages)} messages from channel {channel_id}!")
    await _upload_file_async(
        output_file,
        f"/DiscordBot/{output_file}",
    )  # Upload JSON file to cloud storage
    return message_count, oldest_id

# Event handler: Runs when the bot connects to Discord
@client.event
async def on_ready():
    _log(f"Bot is online as {client.user}")
    if SCRAPE_CHANNEL_ID == 0:
        raise RuntimeError("SCRAPE_CHANNEL_ID is not set.")
    _ensure_state_path(SCRAPE_STATE_PATH)
    state = _load_state(SCRAPE_STATE_PATH)
    if SCRAPE_BACKFILL:
        batches = 0
        while True:
            before_id, backfill_complete = _get_backfill_state(state)
            if backfill_complete:
                _log("✅ Backfill complete, no older messages to scrape.")
                break
            limit = SCRAPE_BATCH_SIZE if SCRAPE_BATCH_SIZE > 0 else 200
            message_count, oldest_id = await scrape_messages(
                SCRAPE_CHANNEL_ID,
                limit=limit,
                after=None,
                before=discord.Object(id=before_id) if before_id else None,
            )
            if message_count == 0:
                _log("✅ Backfill complete, no older messages to scrape.")
                _set_backfill_state(state, None, True)
                _save_state(SCRAPE_STATE_PATH, state)
                break
            _set_backfill_state(state, oldest_id, False)
            _save_state(SCRAPE_STATE_PATH, state)
            _log(f"ℹ️ Backfill cursor saved: {oldest_id}")
            batches += 1
            if not SCRAPE_BACKFILL_AUTORUN:
                break
            if SCRAPE_BACKFILL_MAX_BATCHES > 0 and batches >= SCRAPE_BACKFILL_MAX_BATCHES:
                _log("ℹ️ Backfill max batches reached, stopping.")
                break
            _log(f"⏸️ Sleeping {SCRAPE_BACKFILL_SLEEP_SECONDS}s before next batch.")
            await asyncio.sleep(SCRAPE_BACKFILL_SLEEP_SECONDS)
    else:
        limit = SCRAPE_LIMIT if SCRAPE_LIMIT > 0 else None
        after_candidates = []
        if SCRAPE_SINCE_DAYS > 0:
            after_candidates.append(
                datetime.now(timezone.utc) - timedelta(days=SCRAPE_SINCE_DAYS)
            )
        last_run = _get_last_run(state)
        if last_run is not None:
            after_candidates.append(last_run)
        after = max(after_candidates) if after_candidates else None
        await scrape_messages(
            SCRAPE_CHANNEL_ID,
            limit=limit,
            after=after,
            before=None,
        )
        _set_last_run(state, datetime.now(timezone.utc))
        _save_state(SCRAPE_STATE_PATH, state)
    await client.close()

# Start the Discord bot
client.run(DISCORD_BOT_TOKEN)
