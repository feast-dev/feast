import json
from typing import Callable, Dict, Iterable, Optional, Tuple, Type, Any

from django.db import models
from django.db.models.options import Options
from typeguard import typechecked

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import pg_type_to_feast_value_type
from feast.value_type import ValueType


@typechecked
class DjangoSource(DataSource):
    """
    A DjangoSource object defines a source that can use Django models as a source of features.
    """

    def __init__(
        self,
        model: Type[models.Model],
        name: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """Creates a DjangoSource object.

        Args:
            model: Django model class to use as the source.
            name: Name of source, which should be unique within a project.
            timestamp_field (optional): Event timestamp field used for point-in-time joins.
            created_timestamp_column (optional): Timestamp column indicating when the row
                was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to feature names in a feature table or view. Only used for feature
                columns, not entity or timestamp columns.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the data source, typically the email of the primary
                maintainer.
        """
        # Store model class for later use
        self._model: Type[models.Model] = model
        meta: Options = self._model._meta
        
        # If no name, use the model's db_table as the default name
        if name is None and model is None:
            raise DataSourceNoNameException()
        name = name or meta.db_table
        assert name

        # Create Django options with model class
        self._django_options = DjangoOptions(
            model=self._model,
            name=name,
        )

        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, DjangoSource):
            raise TypeError(
                "Comparisons should only involve DjangoSource class objects."
            )

        return (
            super().__eq__(other)
            and self._django_options._model == other._django_options._model
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        """
        Creates a DjangoSource from a protobuf representation.

        Args:
            data_source: A protobuf representation of a DjangoSource.

        Returns:
            A DjangoSource object based on the data source protobuf.
        """
        # Import here to avoid circular import
        from django.apps import apps

        assert data_source.HasField("custom_options")
        django_options = DjangoOptions.from_proto(data_source.custom_options)

        # Get model class from app_label and model_name
        model = apps.get_model(django_options._app_label, django_options._model_name)

        return DjangoSource(
            name=django_options._name,
            model=model,
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def to_proto(self) -> DataSourceProto:
        """
        Converts a DjangoSource object to its protobuf representation.

        Returns:
            A DataSourceProto object.
        """
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.django_offline_store.django_source.DjangoSource",
            field_mapping=self.field_mapping,
            custom_options=self._django_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        # Validate that the model exists and has the required fields
        model = self._django_options._model
        if self.timestamp_field:
            assert hasattr(model, self.timestamp_field), (
                f"Timestamp field {self.timestamp_field} does not exist "
                f"in model {model.__name__}"
            )

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        """
        Returns the callable method that returns Feast type given the raw column type.
        """
        return pg_type_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """
        Returns the list of column names and raw column types.

        Args:
            config: Configuration object used to configure a feature store.
        """
        model = self._django_options._model
        meta: Options = model._meta
        return [
            (field.name, field.get_internal_type())
            for field in meta.get_fields()
            if not field.is_relation
        ]

    def get_table_query_string(self) -> str:
        """
        Returns a string that can directly be used to reference this table in SQL.
        """
        model = self._django_options._model
        meta: Options = model._meta
        return f"{meta.db_table}"


class DjangoOptions:
    """
    Configuration options for a Django data source.
    """

    def __init__(
        self,
        model: Type[models.Model],
        name: str,
    ):
        # Store model class and metadata
        self._model: Type[models.Model] = model
        self._name: str = name
        # Access model metadata through class
        meta: Options = self._model._meta
        self._app_label: str = meta.app_label
        self._model_name: str = meta.model_name

    @classmethod
    def from_proto(cls, django_options_proto: DataSourceProto.CustomSourceOptions):
        """
        Creates DjangoOptions from a protobuf representation.

        Args:
            django_options_proto: A protobuf representation of django options.

        Returns:
            Returns a DjangoOptions object based on the protobuf.
        """
        from django.apps import apps

        config = json.loads(django_options_proto.configuration.decode("utf8"))
        model = apps.get_model(config["app_label"], config["model_name"])

        return cls(
            model=model,
            name=config["name"],
        )

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        """
        Converts DjangoOptions to its protobuf representation.

        Returns:
            A CustomSourceOptions protobuf.
        """
        django_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {
                    "name": self._name,
                    "app_label": self._app_label,
                    "model_name": self._model_name,
                }
            ).encode()
        )
        return django_options_proto


class SavedDatasetDjangoStorage(SavedDatasetStorage):
    """
    Storage class for saved datasets using Django models.
    """

    _proto_attr_name = "custom_storage"

    django_options: DjangoOptions

    def __init__(self, model: Type[models.Model]):
        meta: Options = model._meta
        self.django_options = DjangoOptions(
            model=model,
            name=meta.db_table,
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        """
        Creates a SavedDatasetDjangoStorage object from a protobuf representation.

        Args:
            storage_proto: A protobuf representation of a saved dataset storage.

        Returns:
            Returns a SavedDatasetDjangoStorage object based on the protobuf.
        """
        django_options = DjangoOptions.from_proto(storage_proto.custom_storage)
        return SavedDatasetDjangoStorage(model=django_options._model)

    def to_proto(self) -> SavedDatasetStorageProto:
        """
        Converts a SavedDatasetDjangoStorage object to its protobuf representation.

        Returns:
            A SavedDatasetStorageProto protobuf.
        """
        return SavedDatasetStorageProto(
            custom_storage=self.django_options.to_proto()
        )

    def to_data_source(self) -> DataSource:
        """
        Returns a data source object based on this storage.

        Returns:
            A DjangoSource object.
        """
        return DjangoSource(model=self.django_options._model)
