"""Feast feature server with CLaMP 3 as the query embedder.

The agent reaches it through the standalone `feast mcp` server (feast_mcp.yaml).

`feast serve` would build its own FeatureStore without our embedder, so we
build the store here and hand it to Feast's get_app().
"""

import uvicorn
from feast import FeatureStore
from feast.feature_server import get_app

from dj.clamp3_client import Clamp3TextEmbedder
from dj.config import FEATURE_REPO


def main() -> None:
    store = FeatureStore(
        repo_path=str(FEATURE_REPO), embedding_provider=Clamp3TextEmbedder()
    )
    app = get_app(store)
    uvicorn.run(app, host="127.0.0.1", port=6566)


if __name__ == "__main__":
    main()
