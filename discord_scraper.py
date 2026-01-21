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

# Function to download and save attachments (images, files, etc.)
async def download_attachment(url, filename):
    if SCRAPE_METADATA_ONLY:
        _log(f"🗒️ Metadata only, skipping download: {filename}")
        return
    if SCRAPE_DRY_RUN:
        _log(f"🔎 Dry run, would download: {filename}")
        return
    async with aiohttp.ClientSession() as session:
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

# Function to scrape messages and images from a given channel
async def scrape_messages(channel_id, limit=None, after=None):
    channel = client.get_channel(channel_id)
    messages = []
    
    async for message in channel.history(limit=limit, after=after):
        msg_data = {
            "author": message.author.name,
            "content": message.content,
            "timestamp": str(message.created_at),
            "attachments": []
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
            await download_attachment(attachment.url, filename)
            msg_data["attachments"].append(filename)
        
        messages.append(msg_data)
    
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
