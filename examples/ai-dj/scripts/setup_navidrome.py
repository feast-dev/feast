"""Write Navidrome's config and a local test login. Run once before starting Navidrome.

The login is a throwaway account for this local demo, saved in config/.env
(git-ignored). ListenBrainz scrobbles go to our bridge instead of listenbrainz.org.
"""

import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dj.config import BRIDGE_PORT  # noqa: E402

CONFIG = ROOT / "config"


def main() -> None:
    CONFIG.mkdir(exist_ok=True)
    env = CONFIG / ".env"
    if not env.exists():
        env.write_text(f"ND_USER=dj\nND_PASSWORD={secrets.token_urlsafe(12)}\n")
    (ROOT / "data" / "navidrome").mkdir(parents=True, exist_ok=True)
    (CONFIG / "navidrome.toml").write_text(
        f"""MusicFolder = "{ROOT / "data" / "music"}"
DataFolder = "{ROOT / "data" / "navidrome"}"
Address = "127.0.0.1"
Port = 4533
LogLevel = "info"
EnableInsightsCollector = false

[ListenBrainz]
Enabled = true
BaseURL = "http://127.0.0.1:{BRIDGE_PORT}/1/"
"""
    )
    print("Wrote config/navidrome.toml and config/.env")


if __name__ == "__main__":
    main()
