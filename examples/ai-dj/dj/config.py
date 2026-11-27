import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
FEATURE_REPO = ROOT / "feature_repo"

FEAST_URL = os.getenv("FEAST_URL", "http://127.0.0.1:6566")
# The standalone `feast mcp` server (feast_mcp.yaml), which proxies to FEAST_URL.
MCP_URL = os.getenv("MCP_URL", "http://127.0.0.1:6590/mcp")
CLAMP3_URL = os.getenv("CLAMP3_URL", "http://127.0.0.1:6580")
BRIDGE_PORT = int(os.getenv("BRIDGE_PORT", "6570"))
NAVIDROME_URL = os.getenv("NAVIDROME_URL", "http://127.0.0.1:4533")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "granite4.2:3b")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
UI_PORT = int(os.getenv("UI_PORT", "6600"))

# The demo listener. The bridge uses the ListenBrainz token as the Feast user_id.
USER_ID = os.getenv("DJ_USER_ID", "demo")


def navidrome_credentials() -> tuple[str, str]:
    """Local test login, created by scripts/setup_navidrome.py in config/.env."""
    env = {}
    path = ROOT / "config" / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env.get("ND_USER", "dj"), env.get("ND_PASSWORD", "")
