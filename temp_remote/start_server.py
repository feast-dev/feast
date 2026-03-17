"""
Feast Feature Server  —  standalone launcher
=============================================
Sets up data, materializes into PostgreSQL (pgvector), and starts
the feature server with uvicorn (single-process, no fork).

Usage:
    python start_server.py              # full setup + serve
    python start_server.py --serve-only # skip setup, just start serving

Prerequisites:
    docker run -d --name feast-pg -p 5432:5432 \
        -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=feast_demo \
        pgvector/pgvector:pg16
"""

import hashlib
import importlib
import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from feast import FeatureStore

EMBEDDING_DIM = 384
HOST = "0.0.0.0"
PORT = 6566

DOCUMENTS = {
    "Introduction to Machine Learning": [
        "Machine learning is a subset of artificial intelligence that enables systems to learn from data. "
        "Instead of being explicitly programmed, these systems improve their performance through experience.",
        "Supervised learning uses labeled training data to learn a mapping from inputs to outputs. "
        "Common algorithms include linear regression, decision trees, and neural networks.",
        "Unsupervised learning finds hidden patterns in data without labeled responses. "
        "Clustering and dimensionality reduction are key techniques in this category.",
        "Reinforcement learning trains agents to make sequences of decisions by rewarding desired behaviors. "
        "It has been successfully applied to game playing, robotics, and autonomous driving.",
    ],
    "Natural Language Processing": [
        "Natural language processing (NLP) combines linguistics and computer science to help machines "
        "understand, interpret, and generate human language.",
        "Tokenization breaks text into smaller units like words or subwords. Modern models use byte-pair "
        "encoding or WordPiece tokenization for better handling of rare words.",
        "Transformer architectures revolutionized NLP by introducing self-attention mechanisms that capture "
        "long-range dependencies in text more effectively than recurrent networks.",
        "Large language models like GPT and BERT are pre-trained on massive text corpora and can be "
        "fine-tuned for downstream tasks such as classification, translation, and question answering.",
    ],
    "Vector Databases": [
        "Vector databases are specialized systems designed to store and query high-dimensional vector "
        "embeddings efficiently, enabling similarity search at scale.",
        "Approximate nearest neighbor (ANN) algorithms like HNSW and IVF trade a small amount of accuracy "
        "for dramatic speed improvements over brute-force search.",
        "Common distance metrics include cosine similarity, Euclidean distance, and inner product. "
        "The choice depends on how embeddings were trained and what notion of similarity is needed.",
        "Remote online stores allow Feast clients to query a centralized feature server without direct "
        "database access, useful for multi-tenant or security-sensitive deployments.",
    ],
}


def simple_embedding(text: str) -> list[float]:
    h = hashlib.sha512(text.encode()).digest()
    rng = np.random.RandomState(int.from_bytes(h[:4], "big"))
    vec = rng.randn(EMBEDDING_DIM).astype(np.float32)
    vec /= np.linalg.norm(vec)
    return vec.tolist()


def build_parquet():
    rows = []
    for doc_title, chunks in DOCUMENTS.items():
        for i, text in enumerate(chunks):
            chunk_id = f"{doc_title.lower().replace(' ', '_')}_chunk_{i}"
            rows.append(
                {
                    "chunk_id": chunk_id,
                    "chunk_text": text,
                    "doc_title": doc_title,
                    "chunk_index": i,
                    "embedding": simple_embedding(text),
                    "event_timestamp": datetime.now() - timedelta(hours=1),
                    "created_timestamp": datetime.now() - timedelta(hours=1),
                }
            )

    os.makedirs(os.path.join("server", "data"), exist_ok=True)
    path = os.path.join("server", "data", "doc_chunks.parquet")
    pd.DataFrame(rows).to_parquet(path, index=False)
    print(f"[1/3] Wrote {len(rows)} chunks → {path}")


def setup_and_materialize():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "server"))
    doc_chunks_repo = importlib.import_module("doc_chunks_repo")

    store = FeatureStore(repo_path="server")

    pg_store = store._get_provider().online_store
    table_name = f"{store.project}_{doc_chunks_repo.doc_chunks_fv.name}"
    with pg_store._get_conn(store.config) as conn:
        cur = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = %s",
            (table_name,),
        )
        if cur.fetchone():
            conn.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
            conn.commit()
            print(f"  Dropped old table '{table_name}'")
    pg_store._conn = None

    store.apply([doc_chunks_repo.chunk_entity, doc_chunks_repo.doc_chunks_fv])
    store.materialize(
        start_date=datetime.now() - timedelta(days=1),
        end_date=datetime.now(),
    )
    print("[2/3] Applied definitions & materialized into PostgreSQL")


def serve():
    from feast.feature_server import get_app

    import uvicorn

    store = FeatureStore(repo_path="server")
    app = get_app(store)
    print(f"[3/3] Starting uvicorn on {HOST}:{PORT} ...")
    uvicorn.run(app, host=HOST, port=PORT)


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    serve_only = "--serve-only" in sys.argv

    if serve_only:
        print("Skipping setup (--serve-only), starting server directly.")
    else:
        build_parquet()
        setup_and_materialize()

    serve()
