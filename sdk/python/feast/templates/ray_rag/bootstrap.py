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

    print("   🎬 Setting up sample IMDB movie data for RAG demonstration...")

    parquet_file = data_path / "raw_movies.parquet"

    if parquet_file.exists():
        try:
            df = pd.read_parquet(parquet_file)
            print(f"   ✅ Sample dataset ready with {len(df)} movies")
            print("   💡 For full dataset (48K+ movies), see README.md")
        except Exception as e:
            print(f"   ⚠️  Could not read sample dataset: {e}")
    else:
        print("   ⚠️  Sample dataset not found, creating minimal example...")
        sample_data = pd.DataFrame(
            {
                "id": ["tt0111161", "tt0068646", "tt0468569", "tt0071562", "tt0050083"],
                "Name": [
                    "The Shawshank Redemption",
                    "The Godfather",
                    "The Dark Knight",
                    "The Godfather Part II",
                    "12 Angry Men",
                ],
                "Description": [
                    "Two imprisoned men bond over a number of years, finding solace and eventual redemption through acts of common decency.",
                    "The aging patriarch of an organized crime dynasty transfers control of his clandestine empire to his reluctant son.",
                    "When the menace known as the Joker wreaks havoc and chaos on the people of Gotham, Batman must accept one of the greatest psychological and physical tests.",
                    "The early life and career of Vito Corleone in 1920s New York City is portrayed, while his son, Michael, expands and tightens his grip on the family crime syndicate.",
                    "A jury holdout attempts to prevent a miscarriage of justice by forcing his colleagues to reconsider the evidence.",
                ],
                "Director": [
                    "Frank Darabont",
                    "Francis Ford Coppola",
                    "Christopher Nolan",
                    "Francis Ford Coppola",
                    "Sidney Lumet",
                ],
                "Genres": [
                    "Drama",
                    "Crime, Drama",
                    "Action, Crime, Drama",
                    "Crime, Drama",
                    "Crime, Drama",
                ],
                "RatingValue": [9.3, 9.2, 9.0, 9.0, 9.0],
                "DatePublished": pd.to_datetime(
                    [
                        "1994-09-23",
                        "1972-03-24",
                        "2008-07-18",
                        "1974-12-20",
                        "1957-04-10",
                    ],
                    utc=True,
                ),
            }
        )
        table = pa.Table.from_pandas(sample_data)
        pq.write_table(table, parquet_file)
        print(f"   ✅ Created sample dataset with {len(sample_data)} movies")

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
