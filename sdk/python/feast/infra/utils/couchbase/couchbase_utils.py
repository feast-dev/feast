from datetime import datetime, timezone


def normalize_timestamp(
    dt: datetime, target_format: str = "%Y-%m-%dT%H:%M:%S%z"
) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)  # Assume UTC for naive datetimes
    # Convert to UTC
    utc_dt = dt.astimezone(timezone.utc)
    # Format with strftime
    formatted = utc_dt.strftime(target_format)
    return formatted
