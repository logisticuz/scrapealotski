# Agent Guide for DiscordScraper

Purpose
- This repository hosts a small Python Discord media scraper and cloud upload helper.
- Main entrypoint: discord_scraper.py.
- Config via environment variables in config.py and .env.example.

Repo Map
- discord_scraper.py: Discord client, message scraping, attachment download, JSON export.
- storage_handler.py: Cloud upload abstraction (Dropbox or GCS).
- config.py: Environment-driven configuration helpers and constants.
- .env.example: Example env vars to copy into a real .env.
- README.md: High-level purpose and MVP behavior.

Build / Run
- This is a Python script repo; no build step is defined.
- No package manager files are present (no requirements.txt / pyproject.toml).

Install Dependencies
- Create a virtual environment (recommended): `python -m venv .venv`
- Activate venv (PowerShell): `.venv\Scripts\Activate.ps1`
- Install likely deps (verify versions as needed):
  - `pip install discord.py aiohttp dropbox google-cloud-storage python-dotenv`
- If you only use Dropbox, GCS client may be optional; if you only use GCS, dropbox may be optional.

Run Commands
- Run the scraper: `python discord_scraper.py`
- Ensure `DISCORD_BOT_TOKEN` is set in env or .env.
- Ensure cloud storage env vars are set if uploads are desired.

Lint / Format
- No lint/format tools are configured in this repo.
- If adding one, document it here and keep tooling minimal.

Tests
- No tests are present.
- No test runner is configured.
- If you add tests, add a clear command for a single test file, e.g. `pytest tests/test_x.py`.

Single Test Guidance (if added)
- Prefer pytest; keep test naming `test_*.py`.
- For a single test file: `pytest tests/test_module.py`.
- For a single test case: `pytest tests/test_module.py::test_case_name`.

Configuration Notes
- Environment variables are loaded via python-dotenv when available.
- `USE_DROPBOX` toggles Dropbox vs GCS (default False).
- `DROPBOX_ACCESS_TOKEN` is required if Dropbox is enabled.
- `GCS_BUCKET_NAME` and `GCS_CREDENTIALS_PATH` are required for GCS uploads.
- `DISCORD_BOT_TOKEN` must be set to run the bot.

Code Style (Observed + Expected)

Python Version
- No explicit version is pinned; assume Python 3.9+.

Imports
- Follow standard grouping: stdlib, third-party, local.
- Example ordering used in repo:
  - `import os`
  - `import aiohttp`
  - `from storage_handler import upload_file`
- Keep one import per line unless importing multiple names from a single module.

Formatting
- Use 4 spaces indentation.
- Use double quotes for strings (consistent with existing files).
- Keep lines reasonably short; break long calls with parentheses.
- Prefer f-strings for string interpolation.

Naming Conventions
- Modules: lowercase with underscores (snake_case).
- Functions: snake_case (e.g. `download_attachment`).
- Constants: UPPER_SNAKE_CASE (e.g. `DISCORD_BOT_TOKEN`).
- Private helpers in config: prefix with underscore (e.g. `_env_bool`).

Types and Annotations
- No typing is currently used.
- If adding types, keep them minimal and consistent across files.

Error Handling
- Raise explicit errors for misconfiguration when it blocks execution (see storage_handler.py).
- For optional subsystems, log and continue (see "Skipping upload").
- Prefer explicit checks for required env vars before running network operations.

Async / IO
- Use `async def` for Discord or aiohttp workflows.
- Keep IO-bound tasks async; avoid blocking calls in async functions.
- Use `async with aiohttp.ClientSession()` pattern for HTTP.

Logging / Output
- Current code uses `print` statements for status.
- If you add logging, keep it lightweight and consistent.

Data Handling
- Save scraped metadata as JSON with UTF-8 and `ensure_ascii=False`.
- Store attachments under `scraped_images/` and metadata under `scraped_data/`.

Discord Usage Notes
- Ensure intents include `message_content` when reading messages.
- `channel_id` is currently hardcoded in on_ready; consider env-driven config if extending.

Cloud Storage
- Dropbox path format: `/DiscordBot/ImageBank/<filename>`.
- GCS uses `GCS_BUCKET_NAME` and upload path as given.
- Keep uploads optional; do not crash when storage is not configured (unless required).

Security / Secrets
- Never commit real tokens or service account JSON files.
- Use `.env` locally (not tracked) or your shell environment.

Repo Hygiene
- Avoid introducing new dependencies unless needed.
- Keep files ASCII unless the file already uses unicode (current code is ASCII; avoid emoji).
- Do not reformat unrelated sections.

If You Add Tooling
- Add a minimal `requirements.txt` or `pyproject.toml`.
- Document commands here under Build/Lint/Test.
- Prefer pytest and black/ruff only if needed.

Known Gaps (as of now)
- No dependency lockfile or pinned versions.
- No tests or CI.
- No structured logging.

Suggested Next Steps (Optional)
- Add a requirements file for repeatable installs.
- Add a config option for channel IDs instead of hardcoding.
- Add tests for config parsing and storage selection.
