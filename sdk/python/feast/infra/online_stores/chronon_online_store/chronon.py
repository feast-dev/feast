from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple
from urllib.parse import quote

from pydantic import StrictInt, StrictStr

from feast.data_source import DataSource
from feast.feature_view import FeatureView
from feast.infra.online_stores.online_store import OnlineStore
from feast.permissions.client.http_auth_requests_wrapper import HttpSessionManager
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.type_map import (
    feast_value_type_to_python_type,
    python_values_to_proto_values,
)
from feast.value_type import ValueType


class ChrononOnlineStoreConfig(FeastConfigBaseModel):
    type: Literal["chronon"] = "chronon"
    path: StrictStr = "http://localhost:8080"
    timeout: StrictInt = 30
    verify_ssl: bool = True


class ChrononOnlineStore(OnlineStore):
    @staticmethod
    def _get_chronon_source(table: FeatureView):
        source: Optional[DataSource] = getattr(table, "batch_source", None)
        if source is None or source.__class__.__name__ != "ChrononSource":
            raise ValueError(
                f"Feature view '{table.name}' is not backed by a ChrononSource."
            )
        return source

    @staticmethod
    def _build_url(config: RepoConfig, table: FeatureView) -> str:
        source = ChrononOnlineStore._get_chronon_source(table)
        base_url = getattr(source, "online_endpoint", "") or config.online_store.path
        object_name = getattr(source, "chronon_join", "") or getattr(
            source, "chronon_group_by", ""
        )
        if not object_name:
            raise ValueError(
                f"ChrononSource for feature view '{table.name}' must define either "
                "`chronon_join` or `chronon_group_by`."
            )
        object_type = "join" if getattr(source, "chronon_join", "") else "groupby"
        return (
            f"{base_url.rstrip('/')}/v1/features/"
            f"{object_type}/{quote(object_name, safe='')}"
        )

    @staticmethod
    def _entity_key_to_request_row(entity_key: EntityKeyProto) -> Dict[str, Any]:
        return {
            join_key: feast_value_type_to_python_type(value)
            for join_key, value in zip(entity_key.join_keys, entity_key.entity_values)
        }

    @staticmethod
    def _feature_values_to_proto(
        response_features: Dict[str, Any], requested_features: Optional[List[str]]
    ) -> Dict[str, ValueProto]:
        if requested_features is not None:
            response_features = {
                key: value
                for key, value in response_features.items()
                if key in requested_features
            }
        result: Dict[str, ValueProto] = {}
        for feature_name, value in response_features.items():
            result[feature_name] = python_values_to_proto_values(
                [value], ValueType.UNKNOWN
            )[0]
        return result

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        raise NotImplementedError(
            "ChrononOnlineStore does not support Feast-managed online writes. "
            "Chronon-backed features must be populated by Chronon."
        )

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        assert isinstance(config.online_store, ChrononOnlineStoreConfig)

        url = self._build_url(config, table)
        payload = [
            self._entity_key_to_request_row(entity_key) for entity_key in entity_keys
        ]
        session = HttpSessionManager.get_session(
            config.auth_config,
            max_retries=0,
        )
        response = session.post(
            url,
            json=payload,
            timeout=config.online_store.timeout,
            verify=config.online_store.verify_ssl,
        )
        response.raise_for_status()

        parsed = response.json()
        results = parsed.get("results", [])
        if len(results) != len(entity_keys):
            raise RuntimeError(
                f"Chronon returned {len(results)} rows for {len(entity_keys)} entity keys."
            )

        output: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for row in results:
            if row.get("status") != "Success":
                output.append((None, None))
                continue
            feature_values = self._feature_values_to_proto(
                row.get("features", {}), requested_features
            )
            output.append((None, feature_values))
        return output

    def update(
        self,
        config: RepoConfig,
        tables_to_delete,
        tables_to_keep,
        entities_to_delete,
        entities_to_keep,
        partial: bool,
    ):
        return None

    def teardown(
        self,
        config: RepoConfig,
        tables,
        entities,
    ):
        return None
