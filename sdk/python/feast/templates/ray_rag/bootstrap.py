import os
import pathlib

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from feast.file_utils import replace_str_in_file


def bootstrap():
    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    project_name = pathlib.Path(__file__).parent.absolute().name
    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)

    print("   🎬 Downloading real IMDB movie data for RAG demonstration...")

    def download_imdb_dataset():
        """Download and process the IMDB movies dataset."""

        try:
            try:
                import kaggle

                print("   🔑 Kaggle API found, checking authentication...")
                kaggle.api.dataset_download_files(
                    "yashgupta24/48000-movies-dataset", path="./data", unzip=True
                )
                print("   📥 Dataset downloaded successfully!")
            except OSError as auth_error:
                if "kaggle.json" in str(auth_error):
                    print("Kaggle credentials not found")
                    print("   💡 To use Kaggle API:")
                    print(
                        "      1. Get API credentials from https://www.kaggle.com/account"
                    )
                    print("      2. Place kaggle.json in ~/.kaggle/")
                    print("      3. chmod 600 ~/.kaggle/kaggle.json")
                    raise ImportError("Kaggle credentials not configured")
                else:
                    raise

            data_dir = "./data"
            if os.path.exists(data_dir):
                csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
                if csv_files:
                    dataset_path = os.path.join(data_dir, csv_files[0])
                    print(f"   📄 Found dataset file: {csv_files[0]}")
                    df = pd.read_csv(dataset_path)
                    print(f"   📊 Dataset shape: {df.shape}")
                    print(f"   📋 Columns: {list(df.columns)}")
                    print(
                        f"   ✅ Successfully downloaded Kaggle dataset with {len(df)} movies"
                    )
                    import shutil

                    target_csv_path = data_path / "final_data.csv"
                    if os.path.exists(dataset_path) and not os.path.exists(
                        target_csv_path
                    ):
                        shutil.copy2(dataset_path, str(target_csv_path))
                        print(f"   📁 Copied CSV to: {target_csv_path}")
                    return
        except ImportError:
            print("Kaggle API not available. Install with: pip install kaggle")

    try:
        print("   📥 Attempting to download IMDB dataset...")
        download_imdb_dataset()
    except Exception as e:
        print(f"Dataset download failed: {e}")

    try:
        csv_path = data_path / "final_data.csv"
        parquet_path = data_path / "raw_movies.parquet"

        if csv_path.exists():
            df = pd.read_csv(csv_path)

            # Convert timestamp columns to datetime with UTC timezone
            # Drop rows without DatePublished as it's required for timestamp filtering
            if "DatePublished" in df.columns:
                df = df.dropna(subset=["DatePublished"])
                df["DatePublished"] = pd.to_datetime(
                    df["DatePublished"], errors="coerce", utc=True
                )

            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_path)
        else:
            print(f"CSV file not found at {csv_path}")
    except Exception as e:
        print(f"Parquet conversion failed: {e}")

    example_py_file = repo_path / "example_repo.py"
    replace_str_in_file(example_py_file, "%PROJECT_NAME%", str(project_name))

    print("🚀 Ray RAG template initialized successfully!")

    print("\n🎯 To get started:")
    print(f"  1. cd {project_name}/feature_repo")
    print("  2. feast apply")
    print("  3. feast materialize --disable-event-timestamp")
    print("  4. python test_workflow.py")


if __name__ == "__main__":
    bootstrap()
