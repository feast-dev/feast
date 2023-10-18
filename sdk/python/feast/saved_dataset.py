from abc import abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Type, cast

import pandas as pd
import pyarrow
from google.protobuf.json_format import MessageToJson

from feast.data_source import DataSource
from feast.dqm.profilers.profiler import Profile, Profiler
from feast.importer import import_class
from feast.protos.feast.core.SavedDataset_pb2 import SavedDataset as SavedDatasetProto
from feast.protos.feast.core.SavedDataset_pb2 import SavedDatasetMeta, SavedDatasetSpec
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.protos.feast.core.ValidationProfile_pb2 import (
    ValidationReference as ValidationReferenceProto,
)

if TYPE_CHECKING:
    from feast.infra.offline_stores.offline_store import RetrievalJob


class _StorageRegistry(type):
    classes_by_proto_attr_name: Dict[str, Type["SavedDatasetStorage"]] = {}

    def __new__(cls, name, bases, dct):
        kls = type.__new__(cls, name, bases, dct)
        if dct.get("_proto_attr_name"):
            cls.classes_by_proto_attr_name[dct["_proto_attr_name"]] = kls
        return kls


_DATA_SOURCE_TO_SAVED_DATASET_STORAGE = {
    "FileSource": "feast.infra.offline_stores.file_source.SavedDatasetFileStorage",
}


def get_saved_dataset_storage_class_from_path(saved_dataset_storage_path: str):
    module_name, class_name = saved_dataset_storage_path.rsplit(".", 1)
    return import_class(module_name, class_name, "SavedDatasetStorage")


class SavedDatasetStorage(metaclass=_StorageRegistry):
    _proto_attr_name: str

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> "SavedDatasetStorage":
        proto_attr_name = cast(str, storage_proto.WhichOneof("kind"))
        return _StorageRegistry.classes_by_proto_attr_name[proto_attr_name].from_proto(
            storage_proto
        )

    @abstractmethod
    def to_proto(self) -> SavedDatasetStorageProto:
        pass

    @abstractmethod
    def to_data_source(self) -> DataSource:
        pass

    @staticmethod
    def from_data_source(data_source: DataSource) -> "SavedDatasetStorage":
        data_source_type = type(data_source).__name__
        if data_source_type in _DATA_SOURCE_TO_SAVED_DATASET_STORAGE:
            cls = get_saved_dataset_storage_class_from_path(
                _DATA_SOURCE_TO_SAVED_DATASET_STORAGE[data_source_type]
            )
            return cls.from_data_source(data_source)
        else:
            raise ValueError(
                f"This method currently does not support {data_source_type}."
            )


class SavedDataset:
    name: str
    features: List[str]
    join_keys: List[str]
    full_feature_names: bool
    storage: SavedDatasetStorage
    tags: Dict[str, str]
    feature_service_name: Optional[str] = None

    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None

    min_event_timestamp: Optional[datetime] = None
    max_event_timestamp: Optional[datetime] = None

    _retrieval_job: Optional["RetrievalJob"] = None

    def __init__(
        self,
        name: str,
        features: List[str],
        join_keys: List[str],
        storage: SavedDatasetStorage,
        full_feature_names: bool = False,
        tags: Optional[Dict[str, str]] = None,
        feature_service_name: Optional[str] = None,
    ):
        self.name = name
        self.features = features
        self.join_keys = join_keys
        self.storage = storage
        self.full_feature_names = full_feature_names
        self.tags = tags or {}
        self.feature_service_name = feature_service_name

        self._retrieval_job = None

    def __repr__(self):
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __hash__(self):
        return hash((self.name))

    def __eq__(self, other):
        if not isinstance(other, SavedDataset):
            raise TypeError(
                "Comparisons should only involve SavedDataset class objects."
            )

        if (
            self.name != other.name
            or sorted(self.features) != sorted(other.features)
            or sorted(self.join_keys) != sorted(other.join_keys)
            or self.storage != other.storage
            or self.full_feature_names != other.full_feature_names
            or self.tags != other.tags
            or self.feature_service_name != other.feature_service_name
        ):
            return False

        return True

    @staticmethod
    def from_proto(saved_dataset_proto: SavedDatasetProto):
        """
        Converts a SavedDatasetProto to a SavedDataset object.

        Args:
            saved_dataset_proto: A protobuf representation of a SavedDataset.
        """
        ds = SavedDataset(
            name=saved_dataset_proto.spec.name,
            features=list(saved_dataset_proto.spec.features),
            join_keys=list(saved_dataset_proto.spec.join_keys),
            full_feature_names=saved_dataset_proto.spec.full_feature_names,
            storage=SavedDatasetStorage.from_proto(saved_dataset_proto.spec.storage),
            tags=dict(saved_dataset_proto.spec.tags.items()),
        )

        if saved_dataset_proto.spec.feature_service_name:
            ds.feature_service_name = saved_dataset_proto.spec.feature_service_name

        if saved_dataset_proto.meta.HasField("created_timestamp"):
            ds.created_timestamp = (
                saved_dataset_proto.meta.created_timestamp.ToDatetime()
            )
        if saved_dataset_proto.meta.HasField("last_updated_timestamp"):
            ds.last_updated_timestamp = (
                saved_dataset_proto.meta.last_updated_timestamp.ToDatetime()
            )
        if saved_dataset_proto.meta.HasField("min_event_timestamp"):
            ds.min_event_timestamp = (
                saved_dataset_proto.meta.min_event_timestamp.ToDatetime()
            )
        if saved_dataset_proto.meta.HasField("max_event_timestamp"):
            ds.max_event_timestamp = (
                saved_dataset_proto.meta.max_event_timestamp.ToDatetime()
            )

        return ds

    def to_proto(self) -> SavedDatasetProto:
        """
        Converts a SavedDataset to its protobuf representation.

        Returns:
            A SavedDatasetProto protobuf.
        """
        meta = SavedDatasetMeta()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)
        if self.min_event_timestamp:
            meta.min_event_timestamp.FromDatetime(self.min_event_timestamp)
        if self.max_event_timestamp:
            meta.max_event_timestamp.FromDatetime(self.max_event_timestamp)

        spec = SavedDatasetSpec(
            name=self.name,
            features=self.features,
            join_keys=self.join_keys,
            full_feature_names=self.full_feature_names,
            storage=self.storage.to_proto(),
            tags=self.tags,
        )
        if self.feature_service_name:
            spec.feature_service_name = self.feature_service_name

        saved_dataset_proto = SavedDatasetProto(spec=spec, meta=meta)
        return saved_dataset_proto

    def with_retrieval_job(self, retrieval_job: "RetrievalJob") -> "SavedDataset":
        self._retrieval_job = retrieval_job
        return self

    def to_df(self) -> pd.DataFrame:
        if not self._retrieval_job:
            raise RuntimeError(
                "To load this dataset use FeatureStore.get_saved_dataset() "
                "instead of instantiating it directly."
            )

        return self._retrieval_job.to_df()

    def to_arrow(self) -> pyarrow.Table:
        if not self._retrieval_job:
            raise RuntimeError(
                "To load this dataset use FeatureStore.get_saved_dataset() "
                "instead of instantiating it directly."
            )

        return self._retrieval_job.to_arrow()

    def as_reference(self, name: str, profiler: "Profiler") -> "ValidationReference":
        return ValidationReference.from_saved_dataset(
            name=name, profiler=profiler, dataset=self
        )

    def get_profile(self, profiler: Profiler) -> Profile:
        return profiler.analyze_dataset(self.to_df())


class ValidationReference:
    name: str
    dataset_name: str
    description: str
    tags: Dict[str, str]
    profiler: Profiler

    _profile: Optional[Profile] = None
    _dataset: Optional[SavedDataset] = None

    def __init__(
        self,
        name: str,
        dataset_name: str,
        profiler: Profiler,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
    ):
        """
        Validation reference combines a reference dataset (currently only a saved dataset object can be used as
        a reference) and a profiler function to generate a validation profile.
        The validation profile can be cached in this object, and in this case
        the saved dataset retrieval and the profiler call will happen only once.

        Validation reference is being stored in the Feast registry and can be retrieved by its name, which
        must be unique within one project.

        Args:
            name: the unique name for validation reference
            dataset_name: the name of the saved dataset used as a reference
            description: a human-readable description
            tags: a dictionary of key-value pairs to store arbitrary metadata
            profiler: the profiler function used to generate profile from the saved dataset
        """
        self.name = name
        self.dataset_name = dataset_name
        self.profiler = profiler
        self.description = description
        self.tags = tags or {}

    @classmethod
    def from_saved_dataset(cls, name: str, dataset: SavedDataset, profiler: Profiler):
        """
        Internal constructor to create validation reference object with actual saved dataset object
        (regular constructor requires only its name).
        """
        ref = ValidationReference(name, dataset.name, profiler)
        ref._dataset = dataset
        return ref

    @property
    def profile(self) -> Profile:
        if not self._profile:
            if not self._dataset:
                raise RuntimeError(
                    "In order to calculate a profile validation reference must be instantiated from a saved dataset. "
                    "Use ValidationReference.from_saved_dataset constructor or FeatureStore.get_validation_reference "
                    "to get validation reference object."
                )

            self._profile = self.profiler.analyze_dataset(self._dataset.to_df())
        return self._profile

    @classmethod
    def from_proto(cls, proto: ValidationReferenceProto) -> "ValidationReference":
        profiler_attr = proto.WhichOneof("profiler")
        if profiler_attr == "ge_profiler":
            from feast.dqm.profilers.ge_profiler import GEProfiler

            profiler = GEProfiler.from_proto(proto.ge_profiler)
        else:
            raise RuntimeError("Unrecognized profiler")

        profile_attr = proto.WhichOneof("cached_profile")
        if profile_attr == "ge_profile":
            from feast.dqm.profilers.ge_profiler import GEProfile

            profile = GEProfile.from_proto(proto.ge_profile)
        elif not profile_attr:
            profile = None
        else:
            raise RuntimeError("Unrecognized profile")

        ref = ValidationReference(
            name=proto.name,
            dataset_name=proto.reference_dataset_name,
            profiler=profiler,
            description=proto.description,
            tags=dict(proto.tags),
        )
        ref._profile = profile

        return ref

    def to_proto(self) -> ValidationReferenceProto:
        from feast.dqm.profilers.ge_profiler import GEProfile, GEProfiler

        proto = ValidationReferenceProto(
            name=self.name,
            reference_dataset_name=self.dataset_name,
            tags=self.tags,
            description=self.description,
            ge_profiler=self.profiler.to_proto()
            if isinstance(self.profiler, GEProfiler)
            else None,
            ge_profile=self._profile.to_proto()
            if isinstance(self._profile, GEProfile)
            else None,
        )

        return proto
