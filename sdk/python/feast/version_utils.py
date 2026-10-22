import re
import uuid
from typing import Tuple

LATEST_VERSION = "latest"
_VERSION_PATTERN = re.compile(r"^v(?:ersion)?(\d+)$", re.IGNORECASE)


def parse_version(version: str) -> Tuple[bool, int]:
    """Parse a version string into (is_latest, version_number).

    Accepts "latest", "vN", or "versionN" (case-insensitive).
    Returns (True, 0) for "latest", (False, N) for pinned versions.

    Raises:
        ValueError: If the version string is invalid.
    """
    if not version or version.lower() == LATEST_VERSION:
        return True, 0

    match = _VERSION_PATTERN.match(version)
    if not match:
        raise ValueError(
            f"Invalid version string '{version}'. "
            f"Expected 'latest', 'vN', or 'versionN' (e.g. 'v2', 'version3')."
        )
    return False, int(match.group(1))


def normalize_version_string(version: str) -> str:
    """Normalize a version string for comparison.

    Empty string and "latest" both normalize to "latest".
    "v2" and "version2" both normalize to "v2".
    """
    if not version or version.lower() == LATEST_VERSION:
        return LATEST_VERSION
    is_latest, num = parse_version(version)
    if is_latest:
        return LATEST_VERSION
    return version_tag(num)


def version_tag(n: int) -> str:
    """Convert an integer version number to the canonical short form 'vN'."""
    return f"v{n}"


def generate_version_id() -> str:
    """Generate a UUID for a version record."""
    return str(uuid.uuid4())
