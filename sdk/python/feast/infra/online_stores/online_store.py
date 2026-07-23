# Copyright 2021 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import asyncio
import logging
import time as _time_mod
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from google.protobuf.timestamp_pb2 import Timestamp

from feast import Entity, utils
from feast.batch_feature_view import BatchFeatureView
from feast.errors import VersionedOnlineReadNotSupported
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.filter_models import ComparisonFilter, CompoundFilter
from feast.infra.infra_object import InfraObject
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.supported_async_methods import SupportedAsyncMethods
from feast.online_response import OnlineResponse
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import RepeatedValue
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.value_type import ValueType

logger = logging.getLogger(__name__)


class OnlineStore(ABC):
    """
    The interface that Feast uses to interact with the storage system that handles online features.
    """

    @property
    def async_supported(self) -> SupportedAsyncMethods:
        return SupportedAsyncMethods()

    @abstractmethod
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Writes a batch of feature rows to the online store.

        If a tz-naive timestamp is passed to this method, it is assumed to be UTC.

        Args:
            config: The config for the current feature store.
            table: Feature view to which these feature rows correspond.
            data: A list of quadruplets containing feature data. Each quadruplet contains an entity
                key, a dict containing feature values, an event timestamp for the row, and the created
                timestamp for the row if it exists.
            progress: Function to be called once a batch of rows is written to the online store, used
                to show progress.
        """
        pass

    async def online_write_batch_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Writes a batch of feature rows to the online store asynchronously.

        If a tz-naive timestamp is passed to this method, it is assumed to be UTC.

        Args:
            config: The config for the current feature store.
            table: Feature view to which these feature rows correspond.
            data: A list of quadruplets containing feature data. Each quadruplet contains an entity
                key, a dict containing feature values, an event timestamp for the row, and the created
                timestamp for the row if it exists.
            progress: Function to be called once a batch of rows is written to the online store, used
                to show progress.
        """
        raise NotImplementedError(
            f"Online store {self.__class__.__name__} does not support online write batch async"
        )

    @abstractmethod
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Reads features values for the given entity keys.

        Args:
            config: The config for the current feature store.
            table: The feature view whose feature values should be read.
            entity_keys: The list of entity keys for which feature values should be read.
            requested_features: The list of features that should be read.

        Returns:
            A list of the same length as entity_keys. Each item in the list is a tuple where the first
            item is the event timestamp for the row, and the second item is a dict mapping feature names
            to values, which are returned in proto format.
        """
        pass

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Reads features values for the given entity keys asynchronously.

        Args:
            config: The config for the current feature store.
            table: The feature view whose feature values should be read.
            entity_keys: The list of entity keys for which feature values should be read.
            requested_features: The list of features that should be read.

        Returns:
            A list of the same length as entity_keys. Each item in the list is a tuple where the first
            item is the event timestamp for the row, and the second item is a dict mapping feature names
            to values, which are returned in proto format.
        """
        raise NotImplementedError(
            f"Online store {self.__class__.__name__} does not support online read async"
        )

    def get_online_features(
        self,
        config: RepoConfig,
        features: Union[List[str], FeatureService],
        entity_rows: Union[
            List[Dict[str, Any]],
            Mapping[str, Union[Sequence[Any], Sequence[ValueProto], RepeatedValue]],
        ],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
        include_feature_view_version_metadata: bool = False,
    ) -> OnlineResponse:
        if isinstance(entity_rows, list):
            columnar: Dict[str, List[Any]] = {k: [] for k in entity_rows[0].keys()}
            for entity_row in entity_rows:
                for key, value in entity_row.items():
                    try:
                        columnar[key].append(value)
                    except KeyError as e:
                        raise ValueError(
                            "All entity_rows must have the same keys."
                        ) from e

            entity_rows = columnar

        (
            join_key_values,
            grouped_refs,
            entity_name_to_join_key_map,
            requested_on_demand_feature_views,
            feature_refs,
            requested_result_row_names,
            online_features_response,
        ) = utils._prepare_entities_to_read_from_online_store(
            registry=registry,
            project=project,
            features=features,
            entity_values=entity_rows,
            full_feature_names=full_feature_names,
            native_entity_values=True,
        )

        # Check for versioned reads on unsupported stores
        self._check_versioned_read_support(grouped_refs)
        _track_read = False
        try:
            from feast.metrics import _config as _metrics_config

            _track_read = _metrics_config.online_features
        except Exception:
            pass

        if _track_read:
            import time as _time

            _read_start = _time.monotonic()

        use_precomputed = (
            isinstance(features, FeatureService)
            and getattr(features, "precompute_online", False)
            and not requested_on_demand_feature_views
        )

        precomputed_ok = False
        if use_precomputed and grouped_refs:
            assert isinstance(features, FeatureService)
            first_table = grouped_refs[0][0]
            first_entity_values, first_idxs, output_len = utils._get_unique_entities(
                first_table, join_key_values, entity_name_to_join_key_map
            )
            entity_key_protos = utils._get_entity_key_protos(first_entity_values)

            blobs = self.read_precomputed_vectors(
                config, features.name, project, entity_key_protos
            )
            expected_names = self._compute_expected_feature_names(
                grouped_refs, full_feature_names
            )
            precomputed_ok = self._try_precomputed_fast_path(
                blobs,
                expected_names,
                online_features_response,
                full_feature_names,
                output_len,
                grouped_refs,
                registry,
                project,
            )
            if not precomputed_ok:
                raise RuntimeError(
                    f"FeatureService '{features.name}' has precompute_online=True "
                    f"but pre-computed vectors could not be read. "
                    f"Run `feast precompute {features.name}` or materialize to "
                    f"populate vectors."
                )

        if not precomputed_ok:
            self._read_features_per_fv(
                config,
                grouped_refs,
                join_key_values,
                entity_name_to_join_key_map,
                online_features_response,
                full_feature_names,
                include_feature_view_version_metadata,
            )

        if _track_read:
            from feast.metrics import track_online_store_read

            track_online_store_read(_time.monotonic() - _read_start)

        feature_types = self._build_feature_types(grouped_refs)

        if requested_on_demand_feature_views:
            utils._augment_response_with_on_demand_transforms(
                online_features_response,
                feature_refs,
                requested_on_demand_feature_views,
                full_feature_names,
                feature_types=feature_types,
            )

        utils._drop_unneeded_columns(
            online_features_response, requested_result_row_names
        )
        return OnlineResponse(online_features_response, feature_types=feature_types)

    _versioned_read_supported: Optional[bool] = None

    def _check_versioned_read_support(self, grouped_refs):
        """Raise an error if versioned reads are attempted on unsupported stores."""
        if self._versioned_read_supported is None:
            self._versioned_read_supported = self._is_versioned_read_supported()

        if self._versioned_read_supported:
            return
        for table, _ in grouped_refs:
            version_tag = getattr(table.projection, "version_tag", None)
            if version_tag is not None:
                raise VersionedOnlineReadNotSupported(
                    self.__class__.__name__, version_tag
                )

    def _is_versioned_read_supported(self) -> bool:
        """Check if this store type supports versioned reads (resolved once, cached)."""
        from feast.infra.online_stores.sqlite import SqliteOnlineStore

        supported_types: list[type] = [SqliteOnlineStore]
        for module, cls_name in (
            ("feast.infra.online_stores.mysql_online_store.mysql", "MySQLOnlineStore"),
            (
                "feast.infra.online_stores.postgres_online_store.postgres",
                "PostgreSQLOnlineStore",
            ),
            ("feast.infra.online_stores.faiss_online_store", "FaissOnlineStore"),
            ("feast.infra.online_stores.redis", "RedisOnlineStore"),
            ("feast.infra.online_stores.dynamodb", "DynamoDBOnlineStore"),
            (
                "feast.infra.online_stores.milvus_online_store.milvus",
                "MilvusOnlineStore",
            ),
        ):
            try:
                import importlib

                mod = importlib.import_module(module)
                supported_types.append(getattr(mod, cls_name))
            except Exception:
                pass
        return isinstance(self, tuple(supported_types))

    def _read_features_per_fv(
        self,
        config: RepoConfig,
        grouped_refs: List,
        join_key_values: Dict,
        entity_name_to_join_key_map: Dict,
        online_features_response,
        full_feature_names: bool,
        include_feature_view_version_metadata: bool,
    ) -> None:
        """Read features one feature-view at a time (generic path).

        Subclasses may override to batch reads more efficiently (e.g. Redis
        pipeline).
        """
        for table, requested_features in grouped_refs:
            table_entity_values, idxs, output_len = utils._get_unique_entities(
                table, join_key_values, entity_name_to_join_key_map
            )
            entity_key_protos = utils._get_entity_key_protos(table_entity_values)

            read_rows = self.online_read(
                config=config,
                table=table,
                entity_keys=entity_key_protos,
                requested_features=requested_features,
            )

            utils._populate_response_from_feature_data(
                requested_features,
                read_rows,
                idxs,
                online_features_response,
                full_feature_names,
                table,
                output_len,
                include_feature_view_version_metadata,
            )

    async def _read_features_per_fv_async(
        self,
        config: RepoConfig,
        grouped_refs: List,
        join_key_values: Dict,
        entity_name_to_join_key_map: Dict,
        online_features_response,
        full_feature_names: bool,
        include_feature_view_version_metadata: bool,
    ) -> None:
        """Async version of :meth:`_read_features_per_fv`.

        Reads all feature views concurrently via ``asyncio.gather``.
        Subclasses may override to batch reads more efficiently.
        """

        async def query_table(table, requested_features):
            table_entity_values, idxs, output_len = utils._get_unique_entities(
                table, join_key_values, entity_name_to_join_key_map
            )
            entity_key_protos = utils._get_entity_key_protos(table_entity_values)
            read_rows = await self.online_read_async(
                config=config,
                table=table,
                entity_keys=entity_key_protos,
                requested_features=requested_features,
            )
            return idxs, read_rows, output_len

        all_responses = await asyncio.gather(
            *[
                query_table(table, requested_features)
                for table, requested_features in grouped_refs
            ]
        )

        for (idxs, read_rows, output_len), (table, requested_features) in zip(
            all_responses, grouped_refs
        ):
            utils._populate_response_from_feature_data(
                requested_features,
                read_rows,
                idxs,
                online_features_response,
                full_feature_names,
                table,
                output_len,
                include_feature_view_version_metadata,
            )

    async def read_precomputed_vectors_async(
        self,
        config: RepoConfig,
        feature_service_name: str,
        project: str,
        entity_keys: List[EntityKeyProto],
    ) -> List[Optional[bytes]]:
        """Async version of :meth:`read_precomputed_vectors`.

        The default implementation delegates to the sync method via the event
        loop executor.  Online stores with native async support should override.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.read_precomputed_vectors,
            config,
            feature_service_name,
            project,
            entity_keys,
        )

    async def get_online_features_async(
        self,
        config: RepoConfig,
        features: Union[List[str], FeatureService],
        entity_rows: Union[
            List[Dict[str, Any]],
            Mapping[str, Union[Sequence[Any], Sequence[ValueProto], RepeatedValue]],
        ],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
        include_feature_view_version_metadata: bool = False,
    ) -> OnlineResponse:
        if isinstance(entity_rows, list):
            columnar: Dict[str, List[Any]] = {k: [] for k in entity_rows[0].keys()}
            for entity_row in entity_rows:
                for key, value in entity_row.items():
                    try:
                        columnar[key].append(value)
                    except KeyError as e:
                        raise ValueError(
                            "All entity_rows must have the same keys."
                        ) from e

            entity_rows = columnar

        (
            join_key_values,
            grouped_refs,
            entity_name_to_join_key_map,
            requested_on_demand_feature_views,
            feature_refs,
            requested_result_row_names,
            online_features_response,
        ) = utils._prepare_entities_to_read_from_online_store(
            registry=registry,
            project=project,
            features=features,
            entity_values=entity_rows,
            full_feature_names=full_feature_names,
            native_entity_values=True,
        )

        # Check for versioned reads on unsupported stores
        self._check_versioned_read_support(grouped_refs)

        _track_read = False
        try:
            from feast.metrics import _config as _metrics_config

            _track_read = _metrics_config.online_features
        except Exception:
            pass

        if _track_read:
            import time as _time

            _read_start = _time.monotonic()

        use_precomputed = (
            isinstance(features, FeatureService)
            and getattr(features, "precompute_online", False)
            and not requested_on_demand_feature_views
        )

        precomputed_ok = False
        if use_precomputed and grouped_refs:
            assert isinstance(features, FeatureService)
            first_table = grouped_refs[0][0]
            first_entity_values, first_idxs, output_len = utils._get_unique_entities(
                first_table, join_key_values, entity_name_to_join_key_map
            )
            entity_key_protos = utils._get_entity_key_protos(first_entity_values)

            blobs = await self.read_precomputed_vectors_async(
                config, features.name, project, entity_key_protos
            )
            expected_names = self._compute_expected_feature_names(
                grouped_refs, full_feature_names
            )
            precomputed_ok = self._try_precomputed_fast_path(
                blobs,
                expected_names,
                online_features_response,
                full_feature_names,
                output_len,
                grouped_refs,
                registry,
                project,
            )
            if not precomputed_ok:
                raise RuntimeError(
                    f"FeatureService '{features.name}' has precompute_online=True "
                    f"but pre-computed vectors could not be read. "
                    f"Run `feast precompute {features.name}` or materialize to "
                    f"populate vectors."
                )

        if not precomputed_ok:
            await self._read_features_per_fv_async(
                config,
                grouped_refs,
                join_key_values,
                entity_name_to_join_key_map,
                online_features_response,
                full_feature_names,
                include_feature_view_version_metadata,
            )

        if _track_read:
            from feast.metrics import track_online_store_read

            track_online_store_read(_time.monotonic() - _read_start)

        feature_types = self._build_feature_types(grouped_refs)

        if requested_on_demand_feature_views:
            utils._augment_response_with_on_demand_transforms(
                online_features_response,
                feature_refs,
                requested_on_demand_feature_views,
                full_feature_names,
                feature_types=feature_types,
            )

        utils._drop_unneeded_columns(
            online_features_response, requested_result_row_names
        )
        return OnlineResponse(online_features_response, feature_types=feature_types)

    @staticmethod
    def _build_feature_types(
        grouped_refs: List,
    ) -> Dict[str, ValueType]:
        """Build a mapping of feature names to ValueType from grouped feature view refs.

        Includes both bare names and prefixed names (feature_view__feature) so that
        lookups succeed regardless of the full_feature_names setting.
        """
        feature_types: Dict[str, ValueType] = {}
        for table, requested_features in grouped_refs:
            table_name = table.projection.name_to_use()
            for field in table.features:
                if field.name in requested_features:
                    vtype = field.dtype.to_value_type()
                    feature_types[field.name] = vtype
                    feature_types[f"{table_name}__{field.name}"] = vtype
        return feature_types

    @staticmethod
    def _compute_expected_feature_names(
        grouped_refs: List, full_feature_names: bool
    ) -> List[str]:
        """Derive the deterministic feature name list for schema comparison."""
        names: List[str] = []
        for table, requested_features in grouped_refs:
            table_name = table.projection.name_to_use()
            for fn in requested_features:
                if fn.startswith("_ts:"):
                    continue
                names.append(f"{table_name}__{fn}" if full_feature_names else fn)
        return names

    @staticmethod
    def _try_precomputed_fast_path(
        blobs: List[Optional[bytes]],
        expected_feature_names: List[str],
        online_features_response: Any,
        full_feature_names: bool,
        num_rows: int,
        grouped_refs: List,
        registry: BaseRegistry,
        project: str,
    ) -> bool:
        """Build the response from pre-computed vectors.

        Returns True if the fast path succeeded for ALL entities, False otherwise
        (caller should fall back to per-FV reads).
        """
        from feast.protos.feast.core.PrecomputedFeatureVector_pb2 import (
            PrecomputedFeatureVector,
        )
        from feast.protos.feast.serving.ServingService_pb2 import (
            FieldStatus,
            GetOnlineFeaturesResponse,
        )

        for i, blob in enumerate(blobs):
            if blob is None:
                logger.warning(
                    "Pre-computed vector blob is None for entity index %d", i
                )
                return False

        n_features = len(expected_feature_names)
        expected_set = set(expected_feature_names)

        # Pre-compute feature-index-to-FV-name mapping once (not per entity).
        feat_fv_names: List[Optional[str]] = []
        for fname in expected_feature_names:
            feat_fv_names.append(fname.split("__", 1)[0] if "__" in fname else None)

        # Pre-compute FV TTLs once.
        fv_ttls: Dict[str, Optional[int]] = {}
        for table, _ in grouped_refs:
            ttl = table.ttl
            fv_ttls[table.projection.name_to_use()] = (
                int(ttl.total_seconds()) if ttl else None
            )

        # Check if any FV actually has a TTL — skip TTL logic entirely if not.
        any_ttl = any(v is not None for v in fv_ttls.values())

        # Parse all blobs, validate schema, build reorder map.
        # Schema is typically identical for all entities, so validate against the
        # first and then just verify the rest match the first (not the expected list).
        first_stored_names: Optional[List[str]] = None
        reorder_map: Optional[List[int]] = None
        vectors: List[PrecomputedFeatureVector] = []

        for blob in blobs:
            vec = PrecomputedFeatureVector()
            vec.ParseFromString(blob)  # type: ignore[arg-type]
            vectors.append(vec)

            if first_stored_names is None:
                first_stored_names = list(vec.feature_names)
                if set(first_stored_names) != expected_set:
                    logger.warning(
                        "Pre-computed vector schema mismatch: stored=%s, expected=%s",
                        first_stored_names,
                        expected_feature_names,
                    )
                    return False
                if first_stored_names != expected_feature_names:
                    name_to_idx = {n: i for i, n in enumerate(first_stored_names)}
                    reorder_map = [name_to_idx[n] for n in expected_feature_names]
            else:
                if (
                    len(vec.feature_names) != n_features
                    or list(vec.feature_names) != first_stored_names
                ):
                    logger.warning(
                        "Pre-computed vector schema varies across entities at index %d",
                        len(vectors) - 1,
                    )
                    return False

        PRESENT = FieldStatus.PRESENT
        OUTSIDE_MAX_AGE = FieldStatus.OUTSIDE_MAX_AGE
        null_value = ValueProto()
        null_ts = Timestamp()

        feat_values = [[null_value] * num_rows for _ in range(n_features)]
        feat_statuses = [[FieldStatus.NOT_FOUND] * num_rows for _ in range(n_features)]
        ts_list = [null_ts] * num_rows

        now_secs = _time_mod.time() if any_ttl else 0.0

        for row_idx, vec in enumerate(vectors):
            ts_list[row_idx] = vec.precomputed_at

            # Build per-FV expiry flags once per entity (not per feature).
            fv_expired: Optional[Dict[str, bool]] = None
            if any_ttl:
                fv_ts_map: Dict[str, Timestamp] = {
                    fvt.feature_view_name: fvt.event_timestamp
                    for fvt in vec.fv_timestamps
                }
                fv_expired = {}
                for fv_name, ttl_val in fv_ttls.items():
                    if ttl_val is not None:
                        event_ts = fv_ts_map.get(fv_name)
                        if event_ts:
                            event_secs = event_ts.seconds + event_ts.nanos / 1e9
                            fv_expired[fv_name] = (now_secs - event_secs) > ttl_val
                        else:
                            fv_expired[fv_name] = False
                    else:
                        fv_expired[fv_name] = False

            stored_values = vec.values
            for out_idx in range(n_features):
                src_idx = reorder_map[out_idx] if reorder_map else out_idx
                feat_values[out_idx][row_idx] = stored_values[src_idx]

                if fv_expired:
                    feat_fv = feat_fv_names[out_idx]
                    if feat_fv and fv_expired.get(feat_fv, False):
                        feat_statuses[out_idx][row_idx] = OUTSIDE_MAX_AGE
                        continue
                feat_statuses[out_idx][row_idx] = PRESENT

        online_features_response.metadata.feature_names.val.extend(
            expected_feature_names
        )
        for f_idx in range(n_features):
            online_features_response.results.append(
                GetOnlineFeaturesResponse.FeatureVector(
                    values=feat_values[f_idx],
                    statuses=feat_statuses[f_idx],
                    event_timestamps=ts_list,
                )
            )
        return True

    def write_precomputed_vector(
        self,
        config: RepoConfig,
        feature_service_name: str,
        project: str,
        entity_key: EntityKeyProto,
        vector_bytes: bytes,
    ) -> None:
        """Write a pre-computed feature vector blob for a single entity.

        Stores the blob as a ``bytes_val`` feature under a synthetic FeatureView
        named ``__precomputed__{service_name}``, using the generic
        ``online_write_batch`` API so this works for every online store without
        store-specific overrides.
        """
        from feast.field import Field
        from feast.types import Bytes

        synthetic_fv = FeatureView(
            name=f"__precomputed__{feature_service_name}",
            entities=[],
            schema=[Field(name="vector", dtype=Bytes)],
            source=None,
        )
        val = ValueProto(bytes_val=vector_bytes)
        ts = datetime.utcnow()
        self.online_write_batch(
            config=config,
            table=synthetic_fv,
            data=[(entity_key, {"vector": val}, ts, None)],
            progress=None,
        )

    def read_precomputed_vectors(
        self,
        config: RepoConfig,
        feature_service_name: str,
        project: str,
        entity_keys: List[EntityKeyProto],
    ) -> List[Optional[bytes]]:
        """Read pre-computed feature vector blobs for a batch of entities.

        Returns a list aligned with *entity_keys*.  Each element is either the
        serialized ``PrecomputedFeatureVector`` bytes or ``None`` when no
        pre-computed vector exists for that entity.

        Reads from the synthetic FeatureView written by
        :meth:`write_precomputed_vector` using the generic ``online_read`` API,
        so this works for every online store without store-specific overrides.
        """
        from feast.field import Field
        from feast.types import Bytes

        synthetic_fv = FeatureView(
            name=f"__precomputed__{feature_service_name}",
            entities=[],
            schema=[Field(name="vector", dtype=Bytes)],
            source=None,
        )
        try:
            rows = self.online_read(
                config=config,
                table=synthetic_fv,
                entity_keys=entity_keys,
                requested_features=["vector"],
            )
        except Exception:
            return [None] * len(entity_keys)

        result: List[Optional[bytes]] = []
        for _ts, feature_dict in rows:
            if feature_dict and "vector" in feature_dict:
                val = feature_dict["vector"]
                if val.HasField("bytes_val"):
                    result.append(val.bytes_val)
                else:
                    result.append(None)
            else:
                result.append(None)
        return result

    @abstractmethod
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        Reconciles cloud resources with the specified set of Feast objects.

        Args:
            config: The config for the current feature store.
            tables_to_delete: Feature views whose corresponding infrastructure should be deleted.
            tables_to_keep: Feature views whose corresponding infrastructure should not be deleted, and
                may need to be updated.
            entities_to_delete: Entities whose corresponding infrastructure should be deleted.
            entities_to_keep: Entities whose corresponding infrastructure should not be deleted, and
                may need to be updated.
            partial: If true, tables_to_delete and tables_to_keep are not exhaustive lists, so
                infrastructure corresponding to other feature views should be not be touched.
        """
        pass

    def plan(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> List[InfraObject]:
        """
        Returns the set of InfraObjects required to support the desired registry.

        Args:
            config: The config for the current feature store.
            desired_registry_proto: The desired registry, in proto form.
        """
        return []

    @abstractmethod
    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Tears down all cloud resources for the specified set of Feast objects.

        Args:
            config: The config for the current feature store.
            tables: Feature views whose corresponding infrastructure should be deleted.
            entities: Entities whose corresponding infrastructure should be deleted.
        """
        pass

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: List[float],
        top_k: int,
        distance_metric: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        """
        Retrieves online feature values for the specified embeddings.

        Args:
            distance_metric: distance metric to use for retrieval.
            config: The config for the current feature store.
            table: The feature view whose feature values should be read.
            requested_features: The list of features whose embeddings should be used for retrieval.
            embedding: The embeddings to use for retrieval.
            top_k: The number of documents to retrieve.

        Returns:
            object: A list of top k closest documents to the specified embedding. Each item in the list is a tuple
            where the first item is the event timestamp for the row, and the second item is a dict of feature
            name to embeddings.
        """
        if not requested_features:
            raise ValueError("Requested_features must be specified")
        raise NotImplementedError(
            f"Online store {self.__class__.__name__} does not support online retrieval"
        )

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
        filters: Optional[Union[ComparisonFilter, CompoundFilter]] = None,
        include_feature_view_version_metadata: bool = False,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """
        Retrieves online feature values for the specified embeddings.

        Args:
            distance_metric: distance metric to use for retrieval.
            config: The config for the current feature store.
            table: The feature view whose feature values should be read.
            requested_features: The list of features whose embeddings should be used for retrieval.
            embedding: The embeddings to use for retrieval (optional)
            top_k: The number of documents to retrieve.
            query_string: The query string to search for using keyword search (bm25) (optional)
            filters: Optional metadata filters (ComparisonFilter or CompoundFilter)
                to narrow results before ranking.

        Returns:
            object: A list of top k closest documents to the specified embedding. Each item in the list is a tuple
            where the first item is the event timestamp for the row, and the second item is a dict of feature
            name to embeddings.
        """
        assert embedding is not None or query_string is not None, (
            "Either embedding or query_string must be specified"
        )
        raise NotImplementedError(
            f"Online store {self.__class__.__name__} does not support online retrieval"
        )

    async def initialize(self, config: RepoConfig) -> None:
        pass

    async def close(self) -> None:
        pass
