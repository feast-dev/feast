<<<<<<< HEAD
import os
import tempfile
from datetime import datetime, timedelta
=======
from datetime import timedelta
>>>>>>> 2b22e7ea9 (address review)

from feast.aggregation import Aggregation
from feast.data_format import AvroFormat
from feast.data_source import KafkaSource
<<<<<<< HEAD
from feast.driver_test_data import (
    create_driver_hourly_stats_df,
    create_global_daily_stats_df,
)
=======
>>>>>>> 2b22e7ea9 (address review)
from feast.entity import Entity
from feast.field import Field
from feast.stream_feature_view import stream_feature_view
from feast.types import Float32
<<<<<<< HEAD
from tests.utils.cli_repo_creator import CliRunner, get_example_repo
from tests.utils.data_source_test_creator import prep_file_source
=======
from tests.utils.cli_utils import CliRunner, get_example_repo
from tests.utils.data_source_utils import prep_file_source
>>>>>>> 2b22e7ea9 (address review)


def test_apply_stream_feature_view(simple_dataset_1) -> None:
    """
    Test apply of StreamFeatureView.
    """
    runner = CliRunner()
<<<<<<< HEAD
    with tempfile.TemporaryDirectory() as data_dir:
        # Generate test data.
        end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_date = end_date - timedelta(days=15)

        driver_entities = [1001, 1002, 1003, 1004, 1005]
        driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)
        driver_stats_path = os.path.join(data_dir, "driver_stats.parquet")
        driver_df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)

        global_df = create_global_daily_stats_df(start_date, end_date)
        global_stats_path = os.path.join(data_dir, "global_stats.parquet")
        global_df.to_parquet(path=global_stats_path, allow_truncated_timestamps=True)

        with runner.local_repo(
            get_example_repo("example_feature_repo_2.py")
            .replace("%PARQUET_PATH%", driver_stats_path)
            .replace("%PARQUET_PATH_GLOBAL%", global_stats_path),
            "file",
        ) as fs, prep_file_source(
            df=simple_dataset_1, timestamp_field="ts_1"
        ) as file_source:
            entity = Entity(name="driver_entity", join_keys=["test_key"])

            stream_source = KafkaSource(
                name="kafka",
                timestamp_field="event_timestamp",
                kafka_bootstrap_servers="",
                message_format=AvroFormat(""),
                topic="topic",
                batch_source=file_source,
                watermark_delay_threshold=timedelta(days=1),
            )

            @stream_feature_view(
                entities=[entity],
                ttl=timedelta(days=30),
                owner="test@example.com",
                online=True,
                schema=[Field(name="dummy_field", dtype=Float32)],
                description="desc",
                aggregations=[
                    Aggregation(
                        column="dummy_field",
                        function="max",
                        time_window=timedelta(days=1),
                    ),
                    Aggregation(
                        column="dummy_field2",
                        function="count",
                        time_window=timedelta(days=24),
                    ),
                ],
                timestamp_field="event_timestamp",
                mode="spark",
                source=stream_source,
                tags={},
            )
            def simple_sfv(df):
                return df

            fs.apply([entity, simple_sfv])

            stream_feature_views = fs.list_stream_feature_views()
            assert len(stream_feature_views) == 1
            assert stream_feature_views[0] == simple_sfv

            features = fs.get_online_features(
                features=["simple_sfv:dummy_field"],
                entity_rows=[{"test_key": 1001}],
            ).to_dict(include_event_timestamps=True)

            assert "test_key" in features
            assert features["test_key"] == [1001]
            assert "dummy_field" in features
            assert features["dummy_field"] == [None]
=======
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "bigquery"
    ) as fs, prep_file_source(
        df=simple_dataset_1, timestamp_field="ts_1"
    ) as file_source:
        entity = Entity(name="driver_entity", join_keys=["test_key"])

        stream_source = KafkaSource(
            name="kafka",
            timestamp_field="event_timestamp",
            kafka_bootstrap_servers="",
            message_format=AvroFormat(""),
            topic="topic",
            batch_source=file_source,
            watermark_delay_threshold=timedelta(days=1),
        )

        @stream_feature_view(
            entities=[entity],
            ttl=timedelta(days=30),
            owner="test@example.com",
            online=True,
            schema=[Field(name="dummy_field", dtype=Float32)],
            description="desc",
            aggregations=[
                Aggregation(
                    column="dummy_field",
                    function="max",
                    time_window=timedelta(days=1),
                ),
                Aggregation(
                    column="dummy_field2",
                    function="count",
                    time_window=timedelta(days=24),
                ),
            ],
            timestamp_field="event_timestamp",
            mode="spark",
            source=stream_source,
            tags={},
        )
        def simple_sfv(df):
            return df

        fs.apply([entity, simple_sfv])

        stream_feature_views = fs.list_stream_feature_views()
        assert len(stream_feature_views) == 1
        assert stream_feature_views[0] == simple_sfv

        features = fs.get_online_features(
            features=["simple_sfv:dummy_field"],
            entity_rows=[{"test_key": 1001}],
        ).to_dict(include_event_timestamps=True)

        assert "test_key" in features
        assert features["test_key"] == [1001]
        assert "dummy_field" in features
        assert features["dummy_field"] == [None]
>>>>>>> 2b22e7ea9 (address review)


def test_stream_feature_view_udf(simple_dataset_1) -> None:
    """
    Test apply of StreamFeatureView udfs are serialized correctly and usable.
    """
    runner = CliRunner()
<<<<<<< HEAD
    with tempfile.TemporaryDirectory() as data_dir:
        # Generate test data.
        end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_date = end_date - timedelta(days=15)

        driver_entities = [1001, 1002, 1003, 1004, 1005]
        driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)
        driver_stats_path = os.path.join(data_dir, "driver_stats.parquet")
        driver_df.to_parquet(path=driver_stats_path, allow_truncated_timestamps=True)

        global_df = create_global_daily_stats_df(start_date, end_date)
        global_stats_path = os.path.join(data_dir, "global_stats.parquet")
        global_df.to_parquet(path=global_stats_path, allow_truncated_timestamps=True)

        with runner.local_repo(
            get_example_repo("example_feature_repo_2.py")
            .replace("%PARQUET_PATH%", driver_stats_path)
            .replace("%PARQUET_PATH_GLOBAL%", global_stats_path),
            "file",
        ) as fs, prep_file_source(
            df=simple_dataset_1, timestamp_field="ts_1"
        ) as file_source:
            entity = Entity(name="driver_entity", join_keys=["test_key"])

            stream_source = KafkaSource(
                name="kafka",
                timestamp_field="event_timestamp",
                kafka_bootstrap_servers="",
                message_format=AvroFormat(""),
                topic="topic",
                batch_source=file_source,
                watermark_delay_threshold=timedelta(days=1),
            )

            @stream_feature_view(
                entities=[entity],
                ttl=timedelta(days=30),
                owner="test@example.com",
                online=True,
                schema=[Field(name="dummy_field", dtype=Float32)],
                description="desc",
                aggregations=[
                    Aggregation(
                        column="dummy_field",
                        function="max",
                        time_window=timedelta(days=1),
                    ),
                    Aggregation(
                        column="dummy_field2",
                        function="count",
                        time_window=timedelta(days=24),
                    ),
                ],
                timestamp_field="event_timestamp",
                mode="spark",
                source=stream_source,
                tags={},
            )
            def pandas_view(pandas_df):
                import pandas as pd

                assert type(pandas_df) == pd.DataFrame
                df = pandas_df.transform(lambda x: x + 10, axis=1)
                df.insert(2, "C", [20.2, 230.0, 34.0], True)
                return df

            import pandas as pd

            fs.apply([entity, pandas_view])

            stream_feature_views = fs.list_stream_feature_views()
            assert len(stream_feature_views) == 1
            assert stream_feature_views[0] == pandas_view

            sfv = stream_feature_views[0]

            df = pd.DataFrame({"A": [1, 2, 3], "B": [10, 20, 30]})
            new_df = sfv.udf(df)
            expected_df = pd.DataFrame(
                {"A": [11, 12, 13], "B": [20, 30, 40], "C": [20.2, 230.0, 34.0]}
            )
            assert new_df.equals(expected_df)
=======
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "bigquery"
    ) as fs, prep_file_source(
        df=simple_dataset_1, timestamp_field="ts_1"
    ) as file_source:
        entity = Entity(name="driver_entity", join_keys=["test_key"])

        stream_source = KafkaSource(
            name="kafka",
            timestamp_field="event_timestamp",
            kafka_bootstrap_servers="",
            message_format=AvroFormat(""),
            topic="topic",
            batch_source=file_source,
            watermark_delay_threshold=timedelta(days=1),
        )

        @stream_feature_view(
            entities=[entity],
            ttl=timedelta(days=30),
            owner="test@example.com",
            online=True,
            schema=[Field(name="dummy_field", dtype=Float32)],
            description="desc",
            aggregations=[
                Aggregation(
                    column="dummy_field",
                    function="max",
                    time_window=timedelta(days=1),
                ),
                Aggregation(
                    column="dummy_field2",
                    function="count",
                    time_window=timedelta(days=24),
                ),
            ],
            timestamp_field="event_timestamp",
            mode="spark",
            source=stream_source,
            tags={},
        )
        def pandas_view(pandas_df):
            import pandas as pd

            assert type(pandas_df) == pd.DataFrame
            df = pandas_df.transform(lambda x: x + 10, axis=1)
            df.insert(2, "C", [20.2, 230.0, 34.0], True)
            return df

        import pandas as pd

        fs.apply([entity, pandas_view])

        stream_feature_views = fs.list_stream_feature_views()
        assert len(stream_feature_views) == 1
        assert stream_feature_views[0] == pandas_view

        sfv = stream_feature_views[0]

        df = pd.DataFrame({"A": [1, 2, 3], "B": [10, 20, 30]})
        new_df = sfv.udf(df)
        expected_df = pd.DataFrame(
            {"A": [11, 12, 13], "B": [20, 30, 40], "C": [20.2, 230.0, 34.0]}
        )
        assert new_df.equals(expected_df)
>>>>>>> 2b22e7ea9 (address review)
