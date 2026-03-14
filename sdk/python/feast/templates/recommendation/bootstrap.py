from feast.file_utils import replace_str_in_file


def bootstrap():
    # Called automatically by init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    import numpy as np
    import pandas as pd

    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        raise SystemExit(
            "sentence-transformers is required for this template: "
            "pip install sentence-transformers"
        )

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    project_name = pathlib.Path(__file__).parent.absolute().name
    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)

    products = [
        (
            "P001",
            "Wireless Noise-Cancelling Headphones",
            "Premium over-ear headphones with active noise cancellation and 30-hour battery life.",
            "Electronics",
            299.99,
            4.7,
        ),
        (
            "P002",
            "Bluetooth Portable Speaker",
            "Waterproof portable speaker with deep bass and 12-hour playtime.",
            "Electronics",
            79.99,
            4.5,
        ),
        (
            "P003",
            "Mechanical Gaming Keyboard",
            "RGB mechanical keyboard with Cherry MX switches and programmable keys.",
            "Electronics",
            149.99,
            4.6,
        ),
        (
            "P004",
            "Ergonomic Wireless Mouse",
            "Vertical ergonomic mouse designed to reduce wrist strain.",
            "Electronics",
            49.99,
            4.3,
        ),
        (
            "P005",
            "Python Machine Learning Cookbook",
            "Practical recipes for building ML models with scikit-learn and TensorFlow.",
            "Books",
            39.99,
            4.4,
        ),
        (
            "P006",
            "Data Engineering Fundamentals",
            "Comprehensive guide to building modern data pipelines and architectures.",
            "Books",
            44.99,
            4.6,
        ),
        (
            "P007",
            "Introduction to Deep Learning",
            "Beginner-friendly deep learning textbook with hands-on PyTorch examples.",
            "Books",
            54.99,
            4.7,
        ),
        (
            "P008",
            "Trail Running Shoes",
            "Lightweight trail running shoes with superior grip and cushioning.",
            "Sports",
            129.99,
            4.6,
        ),
        (
            "P009",
            "Premium Yoga Mat",
            "Non-slip extra-thick yoga mat with carrying strap.",
            "Sports",
            34.99,
            4.4,
        ),
        (
            "P010",
            "Resistance Bands Set",
            "Set of 5 resistance bands with varying tension levels for home workouts.",
            "Sports",
            24.99,
            4.3,
        ),
        (
            "P011",
            "Robot Vacuum Cleaner",
            "Self-navigating robot vacuum with app control and auto-charging.",
            "Home",
            349.99,
            4.5,
        ),
        (
            "P012",
            "Air Purifier with HEPA Filter",
            "Room air purifier with true HEPA filter and air quality sensor.",
            "Home",
            199.99,
            4.6,
        ),
    ]

    model = SentenceTransformer("all-MiniLM-L6-v2")
    descriptions = [f"{name}. {desc}" for _, name, desc, _, _, _ in products]
    embeddings = model.encode(descriptions, normalize_embeddings=True)

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)
    timestamps = [start_date + timedelta(hours=i) for i in range(len(products))]

    columns = [
        "product_id",
        "product_name",
        "description",
        "category",
        "price",
        "rating",
    ]
    df = pd.DataFrame(products, columns=columns)
    df["price"] = df["price"].astype(np.float32)
    df["rating"] = df["rating"].astype(np.float32)
    df["embedding"] = [emb.tolist() for emb in embeddings]
    df["event_timestamp"] = timestamps
    df["created"] = end_date

    products_path = data_path / "products.parquet"
    df.to_parquet(path=str(products_path), allow_truncated_timestamps=True)

    example_py_file = repo_path / "feature_definitions.py"
    replace_str_in_file(example_py_file, "%PROJECT_NAME%", str(project_name))
    replace_str_in_file(
        example_py_file, "%PARQUET_PATH%", str(products_path.relative_to(repo_path))
    )


if __name__ == "__main__":
    bootstrap()
