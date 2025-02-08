import contextlib
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type, Union, cast

import pandas as pd
import pyarrow as pa
from django.db import models
from django.db.models import QuerySet, Q
from django.db.models.options import Options

from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.offline_stores.contrib.django_offline_store.django_source import (
    DjangoSource,
    SavedDatasetDjangoStorage,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage


from typing import Literal, Optional

class DjangoOfflineStoreConfig(RepoConfig):
    """
    Configuration for the Django offline store.

    Attributes:
        type: Literal value indicating the type of offline store.
        connection_string: Optional connection string for Django database.
            If not provided, will use Django's default database configuration.
    """
    type: Literal["django"] = "django"
    connection_string: Optional[str] = None


class DjangoOfflineStore(OfflineStore):
    """
    Offline store for Django models.

    This offline store allows you to use Django models as feature sources in Feast.
    It integrates with Django's ORM to provide:
    - Historical feature retrieval using Django's QuerySet API
    - Support for point-in-time joins
    - Integration with Django's database configuration
    - Schema migration support through Django's migration system

    To use this offline store, configure your feature repository with:
    ```yaml
    project: my_feature_repo
    registry: data/registry.db
    provider: local
    offline_store:
        type: django
        connection_string: optional_database_url  # Optional
    ```
    """

    @staticmethod
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
        """
        Retrieves historical features from a Django model.

        Args:
            config: The configuration for the feature store.
            data_source: The data source to pull features from, must be a DjangoSource.
            join_key_columns: Columns used for joining tables.
            feature_name_columns: Names of feature columns to retrieve.
            timestamp_field: Name of the timestamp field.
            created_timestamp_column: Name of the created timestamp column for deduplication.
            start_date: Start date for retrieving features.
            end_date: End date for retrieving features.

        Returns:
            A RetrievalJob containing the historical features.

        Raises:
            AssertionError: If data_source is not a DjangoSource or config is not a DjangoOfflineStoreConfig.
        """
        assert isinstance(config.offline_store, DjangoOfflineStoreConfig)
        assert isinstance(data_source, DjangoSource)

        # Get the Django model
        django_source = cast(DjangoSource, data_source)
        model = django_source._django_options._model

        # Build query
        query: QuerySet = model.objects.all()

        # Add timestamp filters
        start_date = start_date.astimezone(tz=timezone.utc)
        end_date = end_date.astimezone(tz=timezone.utc)
        query = query.filter(**{
            f"{timestamp_field}__gte": start_date,
            f"{timestamp_field}__lte": end_date,
        })

        # Add ordering
        order_by = ["-" + timestamp_field]
        if created_timestamp_column:
            order_by.append("-" + created_timestamp_column)
        query = query.order_by(*order_by)

        # Get distinct rows based on join keys
        if join_key_columns:
            query = query.distinct(*join_key_columns)
        else:
            # If no join keys, add dummy entity
            feature_name_columns = [*feature_name_columns, DUMMY_ENTITY_ID]

        return DjangoRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        """
        Retrieves historical features from multiple feature views using an entity DataFrame.

        Args:
            config: The configuration for the feature store.
            feature_views: List of feature views to fetch features from.
            feature_refs: List of feature references of the form "feature_view:feature".
            entity_df: Entity DataFrame containing timestamps and entity IDs.
            registry: The registry for the feature store.
            project: The project name.
            full_feature_names: Whether to include the feature view name as a prefix.

        Returns:
            A RetrievalJob containing the historical features.

        Raises:
            ValueError: If entity_df is a string (SQL queries not supported).
            InvalidEntityType: If entity_df is not a pandas DataFrame.
        """
        assert isinstance(config.offline_store, DjangoOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, DjangoSource)

        # Create Django model for entity_df if it's a DataFrame
        if isinstance(entity_df, pd.DataFrame):
            # Convert DataFrame to Django model instances
            model = _create_django_model_from_df(entity_df)
            query = model.objects.all()
            fields = [f.name for f in model._meta.get_fields() if not f.is_relation]
        elif isinstance(entity_df, str):
            raise ValueError(
                f"Django offline store does not support SQL queries as entity_df: {entity_df}"
            )
        else:
            raise ValueError(
                f"entity_df must be a pandas DataFrame, got {type(entity_df)}"
            )

        return DjangoRetrievalJob(
            query=query,
            config=config,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            join_key_columns=fields,
            feature_name_columns=feature_refs,
            timestamp_field=None,  # Will be inferred from entity_df
        )


class DjangoRetrievalJob(RetrievalJob):
    """
    Handles retrieving feature values from Django models.

    This class implements the RetrievalJob interface for Django models, providing:
    - Conversion between Django QuerySets and pandas DataFrames
    - Support for on-demand feature views
    - Feature persistence using Django models
    - Arrow table conversion for efficient data transfer

    Attributes:
        query: The Django QuerySet or DataFrame to retrieve features from.
        config: The feature store configuration.
        full_feature_names: Whether to include feature view names as prefixes.
        on_demand_feature_views: Optional list of on-demand feature views.
        join_key_columns: Columns used for joining.
        feature_name_columns: Names of feature columns.
        timestamp_field: Name of timestamp field.
    """

    def __init__(
        self,
        query: Union[QuerySet, pd.DataFrame],
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        join_key_columns: Optional[List[str]] = None,
        feature_name_columns: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
    ):
        """Creates a DjangoRetrievalJob object.

        Args:
            query: Django QuerySet to execute.
            config: Configuration object.
            full_feature_names: Whether to include the feature view name as a prefix.
            on_demand_feature_views: Optional list of on demand feature views.
            join_key_columns: Columns used for joining.
            feature_name_columns: Names of feature columns.
            timestamp_field: Name of timestamp field.
        """
        self.query = query
        self.config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._join_key_columns = join_key_columns or []
        self._feature_name_columns = feature_name_columns or []
        self._timestamp_field = timestamp_field

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """
        Returns the features from Django models as a pandas DataFrame.

        Args:
            timeout: Optional timeout in seconds for the query execution.

        Returns:
            A pandas DataFrame containing the feature values.

        Note:
            If no join keys are present, a dummy entity column will be added.
        """
        # Convert QuerySet to DataFrame
        if isinstance(self.query, QuerySet):
            values = self.query.values(*self._join_key_columns, *self._feature_name_columns)
            df = pd.DataFrame.from_records(values)
        else:
            df = self.query

        # Add dummy entity if needed
        if not self._join_key_columns:
            df[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL

        return df

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        """
        Returns the features from Django models as a pyarrow Table.
        """
        df = self._to_df_internal(timeout)
        return pa.Table.from_pandas(df)

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        """
        Persists the feature data in the specified storage.

        Args:
            storage: Storage to persist the feature data.
            allow_overwrite: Whether to allow overwriting existing data.
            timeout: Optional timeout in seconds.
        """
        assert isinstance(storage, SavedDatasetDjangoStorage)
        django_storage = cast(SavedDatasetDjangoStorage, storage)

        # Convert to DataFrame and save to Django model
        df = self.to_df()
        model = django_storage.django_options._model

        # Clear existing data if overwrite allowed
        if allow_overwrite:
            model.objects.all().delete()

        # Bulk create new instances
        instances = [
            model(**row.to_dict())
            for _, row in df.iterrows()
        ]
        model.objects.bulk_create(instances)


def _create_django_model_from_df(df: pd.DataFrame) -> Type[models.Model]:
    """
    Creates a Django model class from a pandas DataFrame.

    This function dynamically creates an unmanaged Django model class that matches
    the structure of the provided DataFrame. The model is not managed by Django's
    migration system and is used only for feature retrieval.

    Args:
        df: DataFrame to convert to Django model. The column types will be mapped
            to appropriate Django model field types.

    Returns:
        A Django model class with fields matching the DataFrame columns.

    Note:
        - DateTime columns are converted to DateTimeField
        - Float columns are converted to FloatField
        - Integer columns are converted to IntegerField
        - All other types are converted to CharField(max_length=255)
    """
    # Create model fields based on DataFrame columns
    fields = {}
    for column in df.columns:
        dtype = df[column].dtype
        if pd.api.types.is_datetime64_any_dtype(dtype):
            fields[column] = models.DateTimeField()
        elif pd.api.types.is_float_dtype(dtype):
            fields[column] = models.FloatField()
        elif pd.api.types.is_integer_dtype(dtype):
            fields[column] = models.IntegerField()
        else:
            fields[column] = models.CharField(max_length=255)

    # Create model class
    model = type(
        "EntityModel",
        (models.Model,),
        {
            "__module__": "feast.infra.offline_stores.contrib.django_offline_store.models",
            **fields,
            "Meta": type("Meta", (), {"managed": False}),
        },
    )

    return model
