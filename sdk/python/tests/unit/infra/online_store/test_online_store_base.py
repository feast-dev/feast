from unittest.mock import MagicMock

import pytest

from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.supported_async_methods import SupportedAsyncMethods


class ConcreteOnlineStore(OnlineStore):
    """Minimal concrete implementation for testing the base class."""

    def online_write_batch(self, config, table, data, progress):
        pass

    def online_read(self, config, table, entity_keys, requested_features):
        return []

    def update(
        self,
        config,
        tables_to_delete,
        tables_to_keep,
        entities_to_delete,
        entities_to_keep,
        partial,
    ):
        pass

    def teardown(self, config, tables, entities, registry: BaseRegistry = None):
        pass


class TestOnlineStoreBase:
    def test_async_supported_returns_default(self):
        store = ConcreteOnlineStore()
        result = store.async_supported
        assert isinstance(result, SupportedAsyncMethods)

    def test_concrete_implementation_instantiates(self):
        store = ConcreteOnlineStore()
        assert isinstance(store, OnlineStore)

    def test_online_read_returns_list(self):
        store = ConcreteOnlineStore()
        result = store.online_read(
            config=MagicMock(),
            table=MagicMock(),
            entity_keys=[],
            requested_features=[],
        )
        assert result == []

    def test_cannot_instantiate_abstract(self):
        with pytest.raises(TypeError):
            OnlineStore()
