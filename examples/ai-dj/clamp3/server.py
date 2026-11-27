"""Tiny CLaMP 3 text-embedding service.

Feast's feature server calls it (through dj.clamp3_client.Clamp3TextEmbedder)
to turn a search query like "dreamy synthesizers" into a CLaMP 3 vector.
It runs in its own env because CLaMP 3 pins transformers 4.40.
"""

import uvicorn
from fastapi import FastAPI
from model import get_clamp3
from pydantic import BaseModel

app = FastAPI(title="CLaMP 3 embeddings")


class EmbedRequest(BaseModel):
    texts: list[str]


@app.post("/embed")
def embed(req: EmbedRequest) -> dict:
    model = get_clamp3()
    return {"embeddings": [model.embed_text(t).tolist() for t in req.texts]}


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


if __name__ == "__main__":
    get_clamp3()  # load the model before the first request
    uvicorn.run(app, host="127.0.0.1", port=6580)
