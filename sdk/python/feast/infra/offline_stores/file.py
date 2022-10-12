import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple, Union

import dask.dataframe as dd
import pandas as pd
import pyarrow
import pyarrow.dataset
import pyarrow.parquet
import pytz
from pydantic.typing import Literal

from feast.data_source import DataSource
from feast.errors import (
    FeastJoinKeysDuringMaterialization,
    SavedDatasetLocationAlreadyExists,
)
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.file_source import (
    FileLoggingDestination,
    FileSource,
    SavedDatasetFileStorage,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    get_pyarrow_schema_from_batch_source,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import log_exceptions_and_usage
from feast.utils import (
    _get_requested_feature_views_to_features_dict,
    _run_dask_field_mapping,
)


class FileOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for local (file-based) store"""

    type: Literal["file"] = "file"
    """ Offline store type selector"""


class FileRetrievalJob(RetrievalJob):
    def __init__(
        self,
        evaluation_function: Callable,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        """Initialize a lazy historical retrieval job"""

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    @log_exceptions_and_usage
    def _to_df_internal(self) -> pd.DataFrame:
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().compute()
        df = df.reset_index(drop=True)
        return df

    @log_exceptions_and_usage
    def _to_arrow_internal(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().compute()
        return pyarrow.Table.from_pandas(df)

    def persist(self, storage: SavedDatasetStorage, allow_overwrite: bool = False):
        assert isinstance(storage, SavedDatasetFileStorage)

        # Check if the specified location already exists.
        if not allow_overwrite and os.path.exists(storage.file_options.uri):
            raise SavedDatasetLocationAlreadyExists(location=storage.file_options.uri)

        filesystem, path = FileSource.create_filesystem_and_path(
            storage.file_options.uri,
            storage.file_options.s3_endpoint_override,
        )

        if path.endswith(".parquet"):
            pyarrow.parquet.write_table(
                self.to_arrow(), where=path, filesystem=filesystem
            )
        else:
            # otherwise assume destination is directory
            pyarrow.parquet.write_to_dataset(
                self.to_arrow(), root_path=path, filesystem=filesystem
            )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def supports_remote_storage_export(self) -> bool:
        return False


class FileOfflineStore(OfflineStore):
    @staticmethod
    @log_exceptions_and_usage(offline_store="file")
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, FileOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, FileSource)

        if not isinstance(entity_df, pd.DataFrame) and not isinstance(
            entity_df, dd.DataFrame
        ):
            raise ValueError(
                f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
            )
        entity_df_event_timestamp_col = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL  # local modifiable copy of global variable
        if entity_df_event_timestamp_col not in entity_df.columns:
            datetime_columns = entity_df.select_dtypes(
                include=["datetime", "datetimetz"]
            ).columns
            if len(datetime_columns) == 1:
                print(
                    f"Using {datetime_columns[0]} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
                )
                entity_df_event_timestamp_col = datetime_columns[0]
            else:
                raise ValueError(
                    f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
                )
        (
            feature_views_to_features,
            on_demand_feature_views_to_features,
        ) = _get_requested_feature_views_to_features_dict(
            feature_refs,
            feature_views,
            registry.list_on_demand_feature_views(config.project),
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df, entity_df_event_timestamp_col
        )

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_historical_retrieval():

            # Create a copy of entity_df to prevent modifying the original
            entity_df_with_features = entity_df.copy()

            entity_df_event_timestamp_col_type = entity_df_with_features.dtypes[
                entity_df_event_timestamp_col
            ]
            if (
                not hasattr(entity_df_event_timestamp_col_type, "tz")
                or entity_df_event_timestamp_col_type.tz != pytz.UTC
            ):
                # Make sure all event timestamp fields are tz-aware. We default tz-naive fields to UTC
                entity_df_with_features[
                    entity_df_event_timestamp_col
                ] = entity_df_with_features[entity_df_event_timestamp_col].apply(
                    lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc)
                )

                # Convert event timestamp column to datetime and normalize time zone to UTC
                # This is necessary to avoid issues with pd.merge_asof
                if isinstance(entity_df_with_features, dd.DataFrame):
                    entity_df_with_features[
                        entity_df_event_timestamp_col
                    ] = dd.to_datetime(
                        entity_df_with_features[entity_df_event_timestamp_col], utc=True
                    )
                else:
                    entity_df_with_features[
                        entity_df_event_timestamp_col
                    ] = pd.to_datetime(
                        entity_df_with_features[entity_df_event_timestamp_col], utc=True
                    )

            # Sort event timestamp values
            entity_df_with_features = entity_df_with_features.sort_values(
                entity_df_event_timestamp_col
            )

            all_join_keys = []

            # Load feature view data from sources and join them incrementally
            for feature_view, features in feature_views_to_features.items():
                timestamp_field = feature_view.batch_source.timestamp_field
                created_timestamp_column = (
                    feature_view.batch_source.created_timestamp_column
                )

                # Build a list of entity columns to join on (from the right table)
                join_keys = []

                for entity_column in feature_view.entity_columns:
                    join_key = feature_view.projection.join_key_map.get(
                        entity_column.name, entity_column.name
                    )
                    join_keys.append(join_key)

                right_entity_key_columns = [
                    timestamp_field,
                    created_timestamp_column,
                ] + join_keys
                right_entity_key_columns = [c for c in right_entity_key_columns if c]

                all_join_keys = list(set(all_join_keys + join_keys))

                df_to_join = _read_datasource(feature_view.batch_source)

                df_to_join, timestamp_field = _field_mapping(
                    df_to_join,
                    feature_view,
                    features,
                    right_entity_key_columns,
                    entity_df_event_timestamp_col,
                    timestamp_field,
                    full_feature_names,
                )

                df_to_join = _merge(entity_df_with_features, df_to_join, join_keys)

                df_to_join = _normalize_timestamp(
                    df_to_join, timestamp_field, created_timestamp_column
                )

                df_to_join = _filter_ttl(
                    df_to_join,
                    feature_view,
                    entity_df_event_timestamp_col,
                    timestamp_field,
                )

                df_to_join = _drop_duplicates(
                    df_to_join,
                    all_join_keys,
                    timestamp_field,
                    created_timestamp_column,
                    entity_df_event_timestamp_col,
                )

                entity_df_with_features = _drop_columns(
                    df_to_join, timestamp_field, created_timestamp_column
                )

                # Ensure that we delete dataframes to free up memory
                del df_to_join

            return entity_df_with_features.persist()

        job = FileRetrievalJob(
            evaluation_function=evaluate_historical_retrieval,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(set(entity_df.columns) - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )
        return job

    @staticmethod
    @log_exceptions_and_usage(offline_store="file")
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, FileOfflineStoreConfig)
        assert isinstance(data_source, FileSource)

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_offline_job():
            source_df = _read_datasource(data_source)

            source_df = _normalize_timestamp(
                source_df, timestamp_field, created_timestamp_column
            )

            source_columns = set(source_df.columns)
            if not set(join_key_columns).issubset(source_columns):
                raise FeastJoinKeysDuringMaterialization(
                    data_source.path, set(join_key_columns), source_columns
                )

            ts_columns = (
                [timestamp_field, created_timestamp_column]
                if created_timestamp_column
                else [timestamp_field]
            )
            # try-catch block is added to deal with this issue https://github.com/dask/dask/issues/8939.
            # TODO(kevjumba): remove try catch when fix is merged upstream in Dask.
            try:
                if created_timestamp_column:
                    source_df = source_df.sort_values(
                        by=created_timestamp_column,
                    )

                source_df = source_df.sort_values(by=timestamp_field)

            except ZeroDivisionError:
                # Use 1 partition to get around case where everything in timestamp column is the same so the partition algorithm doesn't
                # try to divide by zero.
                if created_timestamp_column:
                    source_df = source_df.sort_values(
                        by=created_timestamp_column, npartitions=1
                    )

                source_df = source_df.sort_values(by=timestamp_field, npartitions=1)

            source_df = source_df[
                (source_df[timestamp_field] >= start_date)
                & (source_df[timestamp_field] < end_date)
            ]

            source_df = source_df.persist()

            columns_to_extract = set(
                join_key_columns + feature_name_columns + ts_columns
            )
            if join_key_columns:
                source_df = source_df.drop_duplicates(
                    join_key_columns, keep="last", ignore_index=True
                )
            else:
                source_df[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL
                columns_to_extract.add(DUMMY_ENTITY_ID)

            source_df = source_df.persist()

            return source_df[list(columns_to_extract)].persist()

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return FileRetrievalJob(
            evaluation_function=evaluate_offline_job,
            full_feature_names=False,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="file")
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, FileOfflineStoreConfig)
        assert isinstance(data_source, FileSource)

        return FileOfflineStore.pull_latest_from_table_or_query(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns
            + [timestamp_field],  # avoid deduplication
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=None,
            start_date=start_date,
            end_date=end_date,
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        assert isinstance(config.offline_store, FileOfflineStoreConfig)
        destination = logging_config.destination
        assert isinstance(destination, FileLoggingDestination)

        if isinstance(data, Path):
            # Since this code will be mostly used from Go-created thread, it's better to avoid producing new threads
            data = pyarrow.parquet.read_table(data, use_threads=False, pre_buffer=False)

        filesystem, path = FileSource.create_filesystem_and_path(
            destination.path,
            destination.s3_endpoint_override,
        )

        pyarrow.dataset.write_dataset(
            data,
            base_dir=path,
            basename_template=f"{uuid.uuid4().hex}-{{i}}.parquet",
            partitioning=destination.partition_by,
            filesystem=filesystem,
            use_threads=False,
            format=pyarrow.dataset.ParquetFileFormat(),
            existing_data_behavior="overwrite_or_ignore",
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        assert isinstance(config.offline_store, FileOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, FileSource)

        pa_schema, column_names = get_pyarrow_schema_from_batch_source(
            config, feature_view.batch_source
        )
        if column_names != table.column_names:
            raise ValueError(
                f"The input pyarrow table has schema {table.schema} with the incorrect columns {table.column_names}. "
                f"The schema is expected to be {pa_schema} with the columns (in this exact order) to be {column_names}."
            )

        file_options = feature_view.batch_source.file_options
        filesystem, path = FileSource.create_filesystem_and_path(
            file_options.uri, file_options.s3_endpoint_override
        )
        prev_table = pyarrow.parquet.read_table(path, memory_map=True)
        if table.schema != prev_table.schema:
            table = table.cast(prev_table.schema)
        new_table = pyarrow.concat_tables([table, prev_table])
        writer = pyarrow.parquet.ParquetWriter(
            path, table.schema, filesystem=filesystem
        )
        writer.write_table(new_table)
        writer.close()


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
) -> Tuple[datetime, datetime]:
    if not isinstance(entity_df, pd.DataFrame):
        raise ValueError(
            f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
        )

    entity_df_event_timestamp = entity_df.loc[
        :, entity_df_event_timestamp_col
    ].infer_objects()
    if pd.api.types.is_string_dtype(entity_df_event_timestamp):
        entity_df_event_timestamp = pd.to_datetime(entity_df_event_timestamp, utc=True)

    return (
        entity_df_event_timestamp.min().to_pydatetime(),
        entity_df_event_timestamp.max().to_pydatetime(),
    )


def _read_datasource(data_source) -> dd.DataFrame:
    storage_options = (
        {
            "client_kwargs": {
                "endpoint_url": data_source.file_options.s3_endpoint_override
            }
        }
        if data_source.file_options.s3_endpoint_override
        else None
    )

    return dd.read_parquet(
        data_source.path,
        storage_options=storage_options,
    )


def _field_mapping(
    df_to_join: dd.DataFrame,
    feature_view: FeatureView,
    features: List[str],
    right_entity_key_columns: List[str],
    entity_df_event_timestamp_col: str,
    timestamp_field: str,
    full_feature_names: bool,
) -> Tuple[dd.DataFrame, str]:
    # Rename columns by the field mapping dictionary if it exists
    if feature_view.batch_source.field_mapping:
        df_to_join = _run_dask_field_mapping(
            df_to_join, feature_view.batch_source.field_mapping
        )
    # Rename entity columns by the join_key_map dictionary if it exists
    if feature_view.projection.join_key_map:
        df_to_join = _run_dask_field_mapping(
            df_to_join, feature_view.projection.join_key_map
        )

    # Build a list of all the features we should select from this source
    feature_names = []
    columns_map = {}
    for feature in features:
        # Modify the separator for feature refs in column names to double underscore. We are using
        # double underscore as separator for consistency with other databases like BigQuery,
        # where there are very few characters available for use as separators
        if full_feature_names:
            formatted_feature_name = (
                f"{feature_view.projection.name_to_use()}__{feature}"
            )
        else:
            formatted_feature_name = feature
        # Add the feature name to the list of columns
        feature_names.append(formatted_feature_name)
        columns_map[feature] = formatted_feature_name

    # Ensure that the source dataframe feature column includes the feature view name as a prefix
    df_to_join = _run_dask_field_mapping(df_to_join, columns_map)

    # Select only the columns we need to join from the feature dataframe
    df_to_join = df_to_join[right_entity_key_columns + feature_names]
    df_to_join = df_to_join.persist()

    # Make sure to not have duplicated columns
    if entity_df_event_timestamp_col == timestamp_field:
        df_to_join = _run_dask_field_mapping(
            df_to_join,
            {timestamp_field: f"__{timestamp_field}"},
        )
        timestamp_field = f"__{timestamp_field}"

    return df_to_join.persist(), timestamp_field


def _merge(
    entity_df_with_features: dd.DataFrame,
    df_to_join: dd.DataFrame,
    join_keys: List[str],
) -> dd.DataFrame:
    # tmp join keys needed for cross join with null join table view
    tmp_join_keys = []
    if not join_keys:
        entity_df_with_features["__tmp"] = 1
        df_to_join["__tmp"] = 1
        tmp_join_keys = ["__tmp"]

    # Get only data with requested entities
    df_to_join = dd.merge(
        entity_df_with_features,
        df_to_join,
        left_on=join_keys or tmp_join_keys,
        right_on=join_keys or tmp_join_keys,
        suffixes=("", "__"),
        how="left",
    )

    if tmp_join_keys:
        df_to_join = df_to_join.drop(tmp_join_keys, axis=1).persist()
    else:
        df_to_join = df_to_join.persist()

    return df_to_join


def _normalize_timestamp(
    df_to_join: dd.DataFrame,
    timestamp_field: str,
    created_timestamp_column: str,
) -> dd.DataFrame:
    df_to_join_types = df_to_join.dtypes
    timestamp_field_type = df_to_join_types[timestamp_field]

    if created_timestamp_column:
        created_timestamp_column_type = df_to_join_types[created_timestamp_column]

    if not hasattr(timestamp_field_type, "tz") or timestamp_field_type.tz != pytz.UTC:
        # Make sure all timestamp fields are tz-aware. We default tz-naive fields to UTC
        df_to_join[timestamp_field] = df_to_join[timestamp_field].apply(
            lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc),
            meta=(timestamp_field, "datetime64[ns, UTC]"),
        )

    if created_timestamp_column and (
        not hasattr(created_timestamp_column_type, "tz")
        or created_timestamp_column_type.tz != pytz.UTC
    ):
        df_to_join[created_timestamp_column] = df_to_join[
            created_timestamp_column
        ].apply(
            lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc),
            meta=(timestamp_field, "datetime64[ns, UTC]"),
        )

    return df_to_join.persist()


def _filter_ttl(
    df_to_join: dd.DataFrame,
    feature_view: FeatureView,
    entity_df_event_timestamp_col: str,
    timestamp_field: str,
) -> dd.DataFrame:
    # Filter rows by defined timestamp tolerance
    if feature_view.ttl and feature_view.ttl.total_seconds() != 0:
        df_to_join = df_to_join[
            # do not drop entity rows if one of the sources returns NaNs
            df_to_join[timestamp_field].isna()
            | (
                (
                    df_to_join[timestamp_field]
                    >= df_to_join[entity_df_event_timestamp_col] - feature_view.ttl
                )
                & (
                    df_to_join[timestamp_field]
                    <= df_to_join[entity_df_event_timestamp_col]
                )
            )
        ]

        df_to_join = df_to_join.persist()
    else:
        df_to_join = df_to_join[
            # do not drop entity rows if one of the sources returns NaNs
            df_to_join[timestamp_field].isna()
            | (df_to_join[timestamp_field] <= df_to_join[entity_df_event_timestamp_col])
        ]

        df_to_join = df_to_join.persist()

    return df_to_join


def _drop_duplicates(
    df_to_join: dd.DataFrame,
    all_join_keys: List[str],
    timestamp_field: str,
    created_timestamp_column: str,
    entity_df_event_timestamp_col: str,
) -> dd.DataFrame:
    column_order = df_to_join.columns

    # try-catch block is added to deal with this issue https://github.com/dask/dask/issues/8939.
    # TODO(kevjumba): remove try catch when fix is merged upstream in Dask.
    try:
        if created_timestamp_column:
            df_to_join = df_to_join.sort_values(
                by=created_timestamp_column, na_position="first"
            )
            df_to_join = df_to_join.persist()

        df_to_join = df_to_join.sort_values(by=timestamp_field, na_position="first")
        df_to_join = df_to_join.persist()

    except ZeroDivisionError:
        # Use 1 partition to get around case where everything in timestamp column is the same so the partition algorithm doesn't
        # try to divide by zero.
        if created_timestamp_column:
            df_to_join = df_to_join[column_order].sort_values(
                by=created_timestamp_column, na_position="first", npartitions=1
            )
            df_to_join = df_to_join.persist()

        df_to_join = df_to_join[column_order].sort_values(
            by=timestamp_field, na_position="first", npartitions=1
        )
        df_to_join = df_to_join.persist()

    df_to_join = df_to_join.drop_duplicates(
        all_join_keys + [entity_df_event_timestamp_col],
        keep="last",
        ignore_index=True,
    )

    return df_to_join.persist()


def _drop_columns(
    df_to_join: dd.DataFrame,
    timestamp_field: str,
    created_timestamp_column: str,
) -> dd.DataFrame:
    entity_df_with_features = df_to_join.drop([timestamp_field], axis=1).persist()

    if created_timestamp_column:
        entity_df_with_features = entity_df_with_features.drop(
            [created_timestamp_column], axis=1
        ).persist()

    return entity_df_with_features
