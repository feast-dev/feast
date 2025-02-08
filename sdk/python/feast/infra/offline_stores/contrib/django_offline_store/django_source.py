import json
from typing import Callable, Dict, Iterable, Optional, Tuple, Type

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
from feast.value_type import ValueType


@typechecked
class DjangoSource(DataSource):
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
        self._django_options = DjangoOptions(model=model)
        self._model: Type[models.Model] = model
        meta: Options = self._model._meta

        # If no name, use the model's db_table as the default name
        if name is None and model is None:
            raise DataSourceNoNameException()
        name = name or meta.db_table
        assert name

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
        assert data_source.HasField("custom_options")

        django_options = json.loads(data_source.custom_options.configuration)

        return DjangoSource(
            model=django_options["model"],
            name=django_options["name"],
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def to_proto(self) -> DataSourceProto:
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
        if self.timestamp_field:
            assert hasattr(self._model, self.timestamp_field), (
                f"Timestamp field {self.timestamp_field} does not exist in "
                f"Django model {self._model.__name__}"
            )

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        FIELD_TYPE_MAPPING = {
            "IntegerField": ValueType.INT64,
            "FloatField": ValueType.FLOAT,
            "DecimalField": ValueType.FLOAT,
            "BooleanField": ValueType.BOOL,
            "CharField": ValueType.STRING,
            "TextField": ValueType.STRING,
            "DateTimeField": ValueType.UNIX_TIMESTAMP,
            "DateField": ValueType.UNIX_TIMESTAMP,
            "JSONField": ValueType.STRING,
            "BinaryField": ValueType.BYTES,
        }

        def get_value_type(field_type: str) -> ValueType:
            return FIELD_TYPE_MAPPING.get(field_type, ValueType.UNKNOWN)

        return get_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        fields = self._model._meta.fields
        return [(f.name, f.__class__.__name__) for f in fields]

    def get_table_query_string(self) -> str:
        return self._model._meta.db_table


class DjangoOptions:
    def __init__(self, model: Type[models.Model]):
        self._model = model

    @classmethod
    def from_proto(cls, django_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(django_options_proto.configuration.decode("utf8"))
        django_options = cls(model=config["model"])
        return django_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        django_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps({"model": self._model}).encode()
        )
        return django_options_proto


class SavedDatasetDjangoStorage(SavedDatasetStorage):
    _proto_attr_name = "custom_storage"

    django_options: DjangoOptions

    def __init__(self, model: Type[models.Model]):
        self.django_options = DjangoOptions(model=model)

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetDjangoStorage(
            model=DjangoOptions.from_proto(storage_proto.custom_storage)._model
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(custom_storage=self.django_options.to_proto())

    def to_data_source(self) -> DataSource:
        return DjangoSource(model=self.django_options._model)
