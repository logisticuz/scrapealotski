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


def _load_last_run(path):
    if not SCRAPE_USE_LAST_RUN:
        return None
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    timestamp = data.get("last_run")
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _save_last_run(path, timestamp):
    if not SCRAPE_USE_LAST_RUN:
        return
    data = {"last_run": timestamp.isoformat()}
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=4)


def _ensure_state_path(path):
    if not SCRAPE_USE_LAST_RUN:
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
                    if ENABLE_UPLOADS:
                        upload_file(
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
async def scrape_messages(channel_id, limit=None, after=None):
    channel = client.get_channel(channel_id)
    if channel is None:
        raise RuntimeError(f"Channel not found for ID {channel_id}.")
    messages = []

    timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async for message in channel.history(limit=limit, after=after):
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
            except Exception as exc:
                _log(f"⚠️ Message skipped due to error: {exc}")
                continue
    
    # Save messages to a JSON file
    output_file = f"scraped_data/{channel_id}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False, indent=4)

    _log(f"✅ Saved {len(messages)} messages from channel {channel_id}!")
    if ENABLE_UPLOADS:
        upload_file(
            output_file,
            f"/DiscordBot/{output_file}",
        )  # Upload JSON file to cloud storage

# Event handler: Runs when the bot connects to Discord
@client.event
async def on_ready():
    _log(f"Bot is online as {client.user}")
    if SCRAPE_CHANNEL_ID == 0:
        raise RuntimeError("SCRAPE_CHANNEL_ID is not set.")
    _ensure_state_path(SCRAPE_STATE_PATH)
    limit = SCRAPE_LIMIT if SCRAPE_LIMIT > 0 else None
    after_candidates = []
    if SCRAPE_SINCE_DAYS > 0:
        after_candidates.append(
            datetime.now(timezone.utc) - timedelta(days=SCRAPE_SINCE_DAYS)
        )
    last_run = _load_last_run(SCRAPE_STATE_PATH)
    if last_run is not None:
        after_candidates.append(last_run)
    after = max(after_candidates) if after_candidates else None
    await scrape_messages(SCRAPE_CHANNEL_ID, limit=limit, after=after)
    _save_last_run(SCRAPE_STATE_PATH, datetime.now(timezone.utc))
    await client.close()

# Start the Discord bot
client.run(DISCORD_BOT_TOKEN)
