"""Project-wide constants to eliminate magic numbers."""
from __future__ import annotations

# Time units (seconds)
SECOND = 1
MINUTE = 60
HOUR = 3600
DAY = 86400
WEEK = 604800

# Common timeout defaults
DEFAULT_JOB_TIMEOUT = HOUR
DEFAULT_JOB_RETRY_BACKOFF = MINUTE

# Common interval defaults
WATCH_FOLDER_INTERVAL = MINUTE
TELEGRAM_INTERVAL = 5 * MINUTE
RSS_INTERVAL = HOUR
OFFICIAL_INTERVAL = DAY
WEEKLY_INTERVAL = WEEK
MONTHLY_INTERVAL = 30 * DAY
