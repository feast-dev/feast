"""Tests for entity_rows keys that are both request data and a join key.

Regression coverage for the case where an OnDemandFeatureView's RequestSource
declares a field whose name matches a FeatureView's join key: the value must be
used for both purposes, the way get_historical_features already does.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

import feast.utils as utils
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


@pytest.fixture(autouse=True)
def _clear_cache():
    with utils._feature_resolution_cache_lock:
        utils._feature_resolution_cache.clear()
        utils._feature_resolution_registry_ts = None
    yield
    with utils._feature_resolution_cache_lock:
        utils._feature_resolution_cache.clear()
        utils._feature_resolution_registry_ts = None


def _make_registry():
    reg = MagicMock()
    reg.cached_registry_proto_created = datetime.now(tz=timezone.utc)
    reg.cached_registry_proto_ttl = timedelta(seconds=600)
    reg.is_cache_valid.return_value = True
    return reg


def _context(*, join_keys, needed_request_data):
    """Fake _get_online_request_context output."""
    return (
        ["fv:feat1"],  # feature_refs
        [],  # requested_on_demand_feature_views
        {},  # entity_name_to_join_key_map
        {},  # entity_type_map
        join_keys,  # join_keys_set
        [("fv_table", ["feat1"])],  # grouped_refs
        {"feat1"},  # requested_result_row_names
        needed_request_data,  # needed_request_data
        False,  # entityless_case
    )


def _prepare(entity_values):
    return utils._prepare_entities_to_read_from_online_store(
        _make_registry(),
        "proj",
        ["fv:feat1"],
        entity_values,
        native_entity_values=False,
    )


class TestSharedRequestDataAndJoinKey:
    @patch("feast.utils._get_online_request_context")
    def test_shared_name_is_used_as_join_key(self, mock_ctx):
        """A name needed as request data must still populate join_key_values
        when a FeatureView in the same request uses it as a join key."""
        mock_ctx.return_value = _context(
            join_keys={"user_id"}, needed_request_data={"user_id"}
        )
        values = [ValueProto(int64_val=1), ValueProto(int64_val=2)]

        join_key_values = _prepare({"user_id": values})[0]

        assert "user_id" in join_key_values
        assert join_key_values["user_id"] == values

    @patch("feast.utils._get_online_request_context")
    def test_shared_name_appears_in_result_row_names(self, mock_ctx):
        """The shared name must be echoed back in the response rows."""
        mock_ctx.return_value = _context(
            join_keys={"user_id"}, needed_request_data={"user_id"}
        )

        requested_result_row_names = _prepare({"user_id": [ValueProto(int64_val=1)]})[5]

        assert "user_id" in requested_result_row_names

    @patch("feast.utils._get_online_request_context")
    def test_shared_name_populates_response_once(self, mock_ctx):
        """Building the response must not raise on the duplicated name, and the
        value must appear exactly once in the response metadata."""
        mock_ctx.return_value = _context(
            join_keys={"user_id"}, needed_request_data={"user_id"}
        )

        response = _prepare({"user_id": [ValueProto(int64_val=7)]})[6]

        assert list(response.metadata.feature_names.val).count("user_id") == 1

    @patch("feast.utils._get_online_request_context")
    def test_pure_request_data_is_not_a_join_key(self, mock_ctx):
        """A request-data-only name must not leak into join_key_values."""
        mock_ctx.return_value = _context(
            join_keys={"user_id"}, needed_request_data={"txn_amount"}
        )

        join_key_values = _prepare(
            {
                "user_id": [ValueProto(int64_val=1)],
                "txn_amount": [ValueProto(double_val=9.5)],
            }
        )[0]

        assert "user_id" in join_key_values
        assert "txn_amount" not in join_key_values

    @patch("feast.utils._get_online_request_context")
    def test_pure_join_key_still_resolves(self, mock_ctx):
        """The ordinary case must be unchanged."""
        mock_ctx.return_value = _context(
            join_keys={"user_id"}, needed_request_data=set()
        )
        values = [ValueProto(int64_val=3)]

        join_key_values = _prepare({"user_id": values})[0]

        assert join_key_values == {"user_id": values}
