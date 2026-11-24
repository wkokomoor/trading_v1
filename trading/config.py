"""Settings from environment variables (and `.env` locally).

In the cloud these come from the job's environment and Secret Manager; locally
from `.env`. Lowercase Schwab keys (app_key, ...) from older `.env` files are
still accepted.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class Settings:
    database_url: str | None
    schwab_app_key: str | None
    schwab_app_secret: str | None
    schwab_callback_url: str
    schwab_tokens_file: str | None  # cloud: read-only tokens.db mounted from Secret Manager
    smtp_host: str
    smtp_user: str | None  # sender, e.g. a Gmail address
    smtp_password: str | None  # Gmail: an app password, never the account password
    notify_to: str | None  # comma-separated recipients for the daily summary


def _env(name: str, legacy: str | None = None, default: str | None = None) -> str | None:
    return os.environ.get(name) or (os.environ.get(legacy) if legacy else None) or default


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    from dotenv import load_dotenv

    load_dotenv()
    return Settings(
        database_url=_env("DATABASE_URL"),
        schwab_app_key=_env("SCHWAB_APP_KEY", "app_key"),
        schwab_app_secret=_env("SCHWAB_APP_SECRET", "app_secret"),
        schwab_callback_url=_env("SCHWAB_CALLBACK_URL", "callback_url", "https://127.0.0.1"),
        schwab_tokens_file=_env("SCHWAB_TOKENS_FILE"),
        smtp_host=_env("SMTP_HOST", default="smtp.gmail.com"),
        smtp_user=_env("SMTP_USER"),
        smtp_password=_env("SMTP_PASSWORD"),
        notify_to=_env("NOTIFY_TO"),
    )


def code_version() -> tuple[str | None, bool]:
    """(git sha, has uncommitted changes). GIT_SHA env var wins (set at image build time)."""
    if sha := os.environ.get("GIT_SHA"):
        return sha, False
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain", "--untracked-files=no"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
        )
        return sha, dirty
    except (OSError, subprocess.CalledProcessError):
        return None, False
