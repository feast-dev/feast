"""Integration tests for batched PostgreSQL online reads.

The batched path must return exactly what the generic per-feature-view path
returns. These run against a real database because the UNION ALL and its tag
column are the part worth checking, and neither is exercised by mocks.

Run with: pytest --integration sdk/python/tests/integration/online_store/test_postgres_batched_read.py
"""

import shutil
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import psycopg
import pytest

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.postgres_online_store.postgres import (
    MAX_BATCHED_READ_KEYS,
    PostgreSQLOnlineStore,
)
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig, RepoConfig
from feast.types import Int64
from feast.value_type import ValueType

DRIVER = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
CUSTOMER = Entity(
    name="customer", join_keys=["customer_id"], value_type=ValueType.INT64
)
JOIN_KEY_MAP = {"driver": "driver_id", "customer": "customer_id"}
DRIVER_IDS = [1001, 1002, 1003]


def _feature_view(name: str, feature: str, entity: Entity = DRIVER) -> FeatureView:
    join_key = entity.join_key
    return FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name=join_key, dtype=Int64), Field(name=feature, dtype=Int64)],
    )


def _entity_key(join_key: str, value: int) -> EntityKeyProto:
    key = EntityKeyProto()
    key.join_keys.append(join_key)
    key.entity_values.append(ValueProto(int64_val=value))
    return key


@pytest.mark.integration
@pytest.mark.skipif(not shutil.which("docker"), reason="Docker not available")
class TestPostgresBatchedRead:
    @pytest.fixture(autouse=True)
    def setup_postgres(self, tmp_path):
        try:
            from testcontainers.postgres import PostgresContainer
        except ImportError:
            pytest.skip("testcontainers[postgres] not installed")

        self.registry_path = str(tmp_path / "registry.pb")
        self.container = PostgresContainer(
            "postgres:16",
            username="root",
            password="testpass",  # pragma: allowlist secret
            dbname="test",
        ).with_exposed_ports(5432)
        self.container.start()
        self.port = self.container.get_exposed_port(5432)
        yield
        self.container.stop()

    def _config(self) -> RepoConfig:
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStoreConfig,
        )

        return RepoConfig(
            project="batched",
            provider="local",
            online_store=PostgreSQLOnlineStoreConfig(
                type="postgres",
                host="localhost",
                port=int(self.port),
                user="root",
                password="testpass",  # pragma: allowlist secret
                database="test",
                sslmode="disable",
            ),
            registry=RegistryConfig(path=self.registry_path),
            entity_key_serialization_version=3,
        )

    def _write(
        self, store, config, fv, feature, values, join_key="driver_id", ids=None
    ):
        now = datetime.now(tz=timezone.utc)
        ids = ids if ids is not None else DRIVER_IDS
        store.online_write_batch(
            config,
            fv,
            [
                (_entity_key(join_key, i), {feature: ValueProto(int64_val=v)}, now, now)
                for i, v in zip(ids, values)
            ],
            None,
        )

    def _args(self, config, grouped_refs, join_key_values, response):
        return dict(
            config=config,
            grouped_refs=grouped_refs,
            join_key_values=join_key_values,
            entity_name_to_join_key_map=JOIN_KEY_MAP,
            online_features_response=response,
            full_feature_names=False,
            include_feature_view_version_metadata=False,
        )

    def _read(self, store, config, grouped_refs, join_key_values, generic=False):
        response = GetOnlineFeaturesResponse()
        args = self._args(config, grouped_refs, join_key_values, response)
        if generic:
            OnlineStore._read_features_per_fv(store, **args)
        else:
            store._read_features_per_fv(**args)
        return response

    async def _read_async(
        self, store, config, grouped_refs, join_key_values, generic=False
    ):
        response = GetOnlineFeaturesResponse()
        args = self._args(config, grouped_refs, join_key_values, response)
        if generic:
            await OnlineStore._read_features_per_fv_async(store, **args)
        else:
            await store._read_features_per_fv_async(**args)
        return response

    def _drivers(self, ids=None):
        return {
            "driver_id": [ValueProto(int64_val=i) for i in (ids or DRIVER_IDS)],
        }

    def _setup_three_views(self, store, config, feature_name=None):
        """Three views; by default they share one feature name so a demux slip shows."""
        specs = [("fv_a", 10), ("fv_b", 20), ("fv_c", 30)]
        views = []
        for name, base in specs:
            feature = feature_name or f"f_{name}"
            fv = _feature_view(name, feature)
            views.append((fv, feature, base))
        store.update(config, [], [v[0] for v in views], [], [], False)
        for fv, feature, base in views:
            self._write(store, config, fv, feature, [base, base + 1, base + 2])
        return [(fv, [feature]) for fv, feature, _ in views]

    def test_batched_matches_generic_path(self):
        config = self._config()
        store = PostgreSQLOnlineStore()
        grouped_refs = self._setup_three_views(store, config, feature_name="feat")

        batched = self._read(store, config, grouped_refs, self._drivers())
        generic = self._read(store, config, grouped_refs, self._drivers(), generic=True)

        assert batched == generic
        assert [v.int64_val for v in batched.results[0].values] == [10, 11, 12]
        assert [v.int64_val for v in batched.results[1].values] == [20, 21, 22]
        assert [v.int64_val for v in batched.results[2].values] == [30, 31, 32]

    def test_batched_issues_one_execute_generic_issues_three(self):
        """Count executes on the real driver, and prove the improvement."""
        config = self._config()
        store = PostgreSQLOnlineStore()
        grouped_refs = self._setup_three_views(store, config, feature_name="feat")

        real_execute = psycopg.Cursor.execute

        def counted(executes):
            def execute(self, query, params=None, **kwargs):
                executes.append(query)
                return real_execute(self, query, params, **kwargs)

            return execute

        batched_executes: list = []
        with patch.object(psycopg.Cursor, "execute", counted(batched_executes)):
            self._read(store, config, grouped_refs, self._drivers())

        generic_executes: list = []
        with patch.object(psycopg.Cursor, "execute", counted(generic_executes)):
            self._read(store, config, grouped_refs, self._drivers(), generic=True)

        assert len(batched_executes) == 1
        assert len(generic_executes) == 3

    def test_missing_entities_report_not_found_like_generic(self):
        """An entity absent from one view must not shift another view's values."""
        config = self._config()
        store = PostgreSQLOnlineStore()

        fv_full = _feature_view("fv_full", "feat")
        fv_sparse = _feature_view("fv_sparse", "feat")
        store.update(config, [], [fv_full, fv_sparse], [], [], False)
        self._write(store, config, fv_full, "feat", [100, 101, 102])
        # Only the middle driver exists in the sparse view.
        self._write(store, config, fv_sparse, "feat", [7], ids=[DRIVER_IDS[1]])

        grouped_refs = [(fv_full, ["feat"]), (fv_sparse, ["feat"])]
        batched = self._read(store, config, grouped_refs, self._drivers())
        generic = self._read(store, config, grouped_refs, self._drivers(), generic=True)

        assert batched == generic
        # Status, not value: a stored zero and a missing row both read as 0.
        assert list(batched.results[1].statuses) == [
            FieldStatus.NOT_FOUND,
            FieldStatus.PRESENT,
            FieldStatus.NOT_FOUND,
        ]
        assert batched.results[1].values[1].int64_val == 7

    def test_views_on_different_entities_match_generic(self):
        """Per-view entity bookkeeping against a real database."""
        config = self._config()
        store = PostgreSQLOnlineStore()

        fv_driver = _feature_view("fv_driver", "feat", DRIVER)
        fv_customer = _feature_view("fv_customer", "feat", CUSTOMER)
        store.update(config, [], [fv_driver, fv_customer], [], [], False)
        self._write(store, config, fv_driver, "feat", [10, 11, 12])
        self._write(
            store,
            config,
            fv_customer,
            "feat",
            [70, 71],
            join_key="customer_id",
            ids=[7, 8],
        )

        grouped_refs = [(fv_driver, ["feat"]), (fv_customer, ["feat"])]
        # Customer 7 appears twice, so the two views resolve different key counts.
        join_key_values = {
            "driver_id": [ValueProto(int64_val=i) for i in DRIVER_IDS],
            "customer_id": [ValueProto(int64_val=i) for i in (7, 7, 8)],
        }

        batched = self._read(store, config, grouped_refs, join_key_values)
        generic = self._read(store, config, grouped_refs, join_key_values, generic=True)

        assert batched == generic
        assert [v.int64_val for v in batched.results[1].values] == [70, 70, 71]

    def test_large_request_falls_back_and_still_matches_generic(self):
        """Over the key threshold the generic path runs, and results are unchanged."""
        config = self._config()
        store = PostgreSQLOnlineStore()

        fv_a = _feature_view("fv_big_a", "feat")
        fv_b = _feature_view("fv_big_b", "feat")
        store.update(config, [], [fv_a, fv_b], [], [], False)
        ids = list(range(MAX_BATCHED_READ_KEYS + 1))
        self._write(store, config, fv_a, "feat", ids, ids=ids)
        self._write(store, config, fv_b, "feat", [i * 2 for i in ids], ids=ids)

        grouped_refs = [(fv_a, ["feat"]), (fv_b, ["feat"])]
        join_key_values = self._drivers(ids)

        real_execute = psycopg.Cursor.execute
        executes: list = []

        def execute(self, query, params=None, **kwargs):
            executes.append(query)
            return real_execute(self, query, params, **kwargs)

        with patch.object(psycopg.Cursor, "execute", execute):
            batched = self._read(store, config, grouped_refs, join_key_values)
        generic = self._read(store, config, grouped_refs, join_key_values, generic=True)

        # Two executes, not one: the guard sent this down the per-view path.
        assert len(executes) == 2
        assert batched == generic

    async def test_async_batched_matches_generic(self):
        config = self._config()
        store = PostgreSQLOnlineStore()
        grouped_refs = self._setup_three_views(store, config, feature_name="feat")

        batched = await self._read_async(store, config, grouped_refs, self._drivers())
        generic = await self._read_async(
            store, config, grouped_refs, self._drivers(), generic=True
        )

        assert batched == generic
        assert [v.int64_val for v in batched.results[0].values] == [10, 11, 12]
        assert [v.int64_val for v in batched.results[1].values] == [20, 21, 22]
