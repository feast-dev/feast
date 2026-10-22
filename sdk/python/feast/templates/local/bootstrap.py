from feast.file_utils import replace_str_in_file


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    import pyarrow as pa
    import pyarrow.parquet as pq

    from feast.driver_test_data import create_driver_hourly_stats_df

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    project_name = pathlib.Path(__file__).parent.absolute().name
    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

    driver_stats_path = data_path / "driver_stats.parquet"
    driver_df.to_parquet(path=str(driver_stats_path), allow_truncated_timestamps=True)

    # Create an empty parquet file for the label view batch source
    # Use explicit pyarrow schema to ensure proper column types even with zero rows
    label_schema = pa.schema(
        [
            ("driver_id", pa.int64()),
            ("is_reliable", pa.int64()),
            ("quality_score", pa.float32()),
            ("reviewer_notes", pa.string()),
            ("labeler", pa.string()),
            ("event_timestamp", pa.timestamp("ns")),
        ]
    )
    label_table = pa.table(
        {field.name: pa.array([], type=field.type) for field in label_schema},
        schema=label_schema,
    )
    label_data_path = data_path / "driver_quality_labels.parquet"
    pq.write_table(label_table, str(label_data_path))

    example_py_file = repo_path / "feature_definitions.py"
    replace_str_in_file(example_py_file, "%PROJECT_NAME%", str(project_name))
    replace_str_in_file(
        example_py_file, "%PARQUET_PATH%", str(driver_stats_path.relative_to(repo_path))
    )
    replace_str_in_file(
        example_py_file, "%LOGGING_PATH%", str(data_path.relative_to(repo_path))
    )
    replace_str_in_file(
        example_py_file,
        "%LABEL_DATA_PATH%",
        str(label_data_path.relative_to(repo_path)),
    )


if __name__ == "__main__":
    bootstrap()
