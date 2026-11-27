"""Client for the CLaMP 3 sidecar (clamp3/server.py), plus a Feast query embedder.

CLaMP 3 is a music-text model: a song and a sentence that describes it land
close together in the same 768-number space. It runs in its own env, so the
demo calls it over HTTP.
"""

from typing import Any

import httpx
import numpy as np

from dj.config import CLAMP3_URL


def embed_text(texts: list[str]) -> np.ndarray:
    r = httpx.post(f"{CLAMP3_URL}/embed", json={"texts": texts}, timeout=120)
    r.raise_for_status()
    return np.array(r.json()["embeddings"], dtype=np.float32)


class Clamp3TextEmbedder:
    """Feast EmbeddingProvider: turns a search query into a CLaMP 3 text vector.

    Passed to FeatureStore(embedding_provider=...), so the OpenAI-compatible
    /v1/vector_stores/{id}/search endpoint searches songs by mood text.
    """

    def embed(self, texts: list[str]) -> list[list[float]]:
        return embed_text(texts).tolist()

    async def aembed(self, texts: list[str]) -> list[list[float]]:
        async with httpx.AsyncClient(timeout=120) as client:
            r = await client.post(f"{CLAMP3_URL}/embed", json={"texts": texts})
        r.raise_for_status()
        out: Any = r.json()["embeddings"]
        return out
