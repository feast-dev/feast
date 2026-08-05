def bootstrap():
    # Bootstrap() is called from init_repo() during `feast init`
    import pathlib
    from datetime import datetime

    import numpy as np
    import pandas as pd

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)

    # Minimal city data with embeddings (384-d to match feature_store embedding_dim)
    embedding_dim = 384
    now = datetime.now().replace(microsecond=0, tzinfo=None)
    cities = [
        (
            1,
            "New York",
            "New York",
            "New York City is the most populous city in the United States.",
        ),
        (
            2,
            "Los Angeles",
            "California",
            "Los Angeles is the second most populous city in the United States.",
        ),
        (
            3,
            "Chicago",
            "Illinois",
            "Chicago is the third most populous city in the United States.",
        ),
    ]
    rows = []
    for city_id, city_name, state, wiki_summary in cities:
        vec = np.random.randn(embedding_dim).astype(np.float32)
        vec = (vec / np.linalg.norm(vec)).tolist()
        rows.append(
            {
                "city_id": city_id,
                "event_timestamp": pd.Timestamp(now),
                "vector": vec,
                "sentence_chunks": wiki_summary[:200],
                "state": f"{city_name}, {state}",
                "wiki_summary": wiki_summary,
            }
        )
    df = pd.DataFrame(rows)
    parquet_path = data_path / "city_wikipedia_summaries_with_embeddings.parquet"
    df.to_parquet(path=str(parquet_path), index=False)


if __name__ == "__main__":
    bootstrap()
