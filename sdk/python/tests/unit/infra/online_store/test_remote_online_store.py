import inspect
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.feature_service import FeatureService
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.remote import RemoteOnlineStore, RemoteOnlineStoreConfig
from feast.online_response import OnlineResponse
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Float32, Int64, String
from feast.value_type import ValueType


class TestRemoteOnlineStoreRetrieveDocuments:
    """Test suite for retrieve_online_documents and retrieve_online_documents_v2 methods."""

    @pytest.fixture
    def remote_store(self):
        """Create a RemoteOnlineStore instance for testing."""
        return RemoteOnlineStore()

    @pytest.fixture
    def config(self):
        """Create a RepoConfig with RemoteOnlineStoreConfig."""
        return RepoConfig(
            project="test_project",
            online_store=RemoteOnlineStoreConfig(
                type="remote", path="http://localhost:6566"
            ),
            registry="dummy_registry",
        )

    @pytest.fixture
    def config_with_cert(self):
        """Create a RepoConfig with RemoteOnlineStoreConfig including TLS cert."""
        return RepoConfig(
            project="test_project",
            online_store=RemoteOnlineStoreConfig(
                type="remote", path="http://localhost:6566", cert="/path/to/cert.pem"
            ),
            registry="dummy_registry",
        )

    @pytest.fixture
    def feature_view(self):
        """Create a test FeatureView."""
        entity = Entity(
            name="user_id", description="User ID", value_type=ValueType.INT64
        )
        source = FileSource(path="test.parquet", timestamp_field="event_timestamp")
        return FeatureView(
            name="test_feature_view",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="user_id", dtype=Int64),  # Entity field
                Field(name="feature1", dtype=String),
                Field(name="embedding", dtype=Float32),
            ],
            source=source,
        )

    @pytest.fixture
    def mock_successful_response(self):
        """Create a mock successful HTTP response for documents retrieval."""
        return {
            "metadata": {
                "feature_names": ["feature1", "embedding", "distance", "user_id"]
            },
            "results": [
                {
                    "values": ["test_value_1", "test_value_2"],
                    "statuses": ["PRESENT", "PRESENT"],
                },  # feature1
                {
                    "values": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
                    "statuses": ["PRESENT", "PRESENT"],
                    "event_timestamps": [
                        "2023-01-01T00:00:00Z",
                        "2023-01-01T01:00:00Z",
                    ],
                },  # embedding
                {
                    "values": [0.85, 0.92],
                    "statuses": ["PRESENT", "PRESENT"],
                },  # distance
                {"values": [123, 456], "statuses": ["PRESENT", "PRESENT"]},  # user_id
            ],
        }

    @pytest.fixture
    def mock_successful_response_v2(self):
        """Create a mock successful HTTP response for documents retrieval v2."""
        return {
            "metadata": {"feature_names": ["user_id", "feature1"]},
            "results": [
                {"values": [123, 456], "statuses": ["PRESENT", "PRESENT"]},  # user_id
                {
                    "values": ["test_value_1", "test_value_2"],
                    "statuses": ["PRESENT", "PRESENT"],
                    "event_timestamps": [
                        "2023-01-01T00:00:00Z",
                        "2023-01-01T01:00:00Z",
                    ],
                },  # feature1
            ],
        }

    @patch("feast.infra.online_stores.remote.get_remote_online_documents")
    def test_retrieve_online_documents_success(
        self,
        mock_get_remote_online_documents,
        remote_store,
        config,
        feature_view,
        mock_successful_response,
    ):
        """Test successful retrieve_online_documents call."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(mock_successful_response)
        mock_get_remote_online_documents.return_value = mock_response

        # Call the method
        result = remote_store.retrieve_online_documents(
            config=config,
            table=feature_view,
            requested_features=["feature1"],
            embedding=[0.1, 0.2, 0.3],
            top_k=2,
            distance_metric="L2",
        )

        # Verify the call was made correctly
        mock_get_remote_online_documents.assert_called_once()
        call_args = mock_get_remote_online_documents.call_args
        assert call_args[1]["config"] == config

        # Verify the request body dict is correct
        req_body = call_args[1]["req_body"]
        assert req_body["features"] == ["test_feature_view:feature1"]
        assert req_body["query"] == [0.1, 0.2, 0.3]
        assert req_body["top_k"] == 2
        assert req_body["distance_metric"] == "L2"

        # Verify the result
        assert len(result) == 2
        event_ts, entity_key_proto, feature_val, vector_value, distance_val = result[0]

        # Check event timestamp
        assert isinstance(event_ts, datetime)

        # Check that we got ValueProto objects
        assert isinstance(feature_val, ValueProto)
        assert isinstance(vector_value, ValueProto)
        assert isinstance(distance_val, ValueProto)

    @patch("feast.infra.online_stores.remote.get_remote_online_documents")
    def test_retrieve_online_documents_v2_success(
        self,
        mock_get_remote_online_documents,
        remote_store,
        config,
        feature_view,
        mock_successful_response_v2,
    ):
        """Test successful retrieve_online_documents_v2 call."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(mock_successful_response_v2)
        mock_get_remote_online_documents.return_value = mock_response

        # Call the method
        result = remote_store.retrieve_online_documents_v2(
            config=config,
            table=feature_view,
            requested_features=["feature1"],
            embedding=[0.1, 0.2, 0.3],
            top_k=2,
            distance_metric="cosine",
            query_string="test query",
        )

        # Verify the call was made correctly
        mock_get_remote_online_documents.assert_called_once()
        call_args = mock_get_remote_online_documents.call_args
        assert call_args[1]["config"] == config

        # Verify the request body dict is correct
        req_body = call_args[1]["req_body"]
        assert req_body["features"] == ["test_feature_view:feature1"]
        assert req_body["query"] == [0.1, 0.2, 0.3]
        assert req_body["top_k"] == 2
        assert req_body["distance_metric"] == "cosine"
        assert req_body["query_string"] == "test query"
        assert req_body["api_version"] == 2

        # Verify the result
        assert len(result) == 2
        event_ts, entity_key_proto, feature_values_dict = result[0]

        # Check event timestamp
        assert isinstance(event_ts, datetime)

        # Check entity key proto
        assert isinstance(entity_key_proto, EntityKeyProto)

        # Check feature values dictionary
        assert isinstance(feature_values_dict, dict)
        assert "feature1" in feature_values_dict
        assert isinstance(feature_values_dict["feature1"], ValueProto)

    @patch("feast.infra.online_stores.remote.get_remote_online_documents")
    def test_retrieve_online_documents_with_cert(
        self,
        mock_get_remote_online_documents,
        remote_store,
        config_with_cert,
        feature_view,
        mock_successful_response,
    ):
        """Test retrieve_online_documents with TLS certificate."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(mock_successful_response)
        mock_get_remote_online_documents.return_value = mock_response

        # Call the method
        result = remote_store.retrieve_online_documents(
            config=config_with_cert,
            table=feature_view,
            requested_features=["feature1"],
            embedding=[0.1, 0.2, 0.3],
            top_k=1,
        )

        # Verify the call was made
        mock_get_remote_online_documents.assert_called_once()
        assert len(result) == 2

    @patch("feast.infra.online_stores.remote.get_remote_online_documents")
    def test_retrieve_online_documents_error_response(
        self, mock_get_remote_online_documents, remote_store, config, feature_view
    ):
        """Test retrieve_online_documents with error response."""
        # Setup mock error response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_get_remote_online_documents.return_value = mock_response

        # Call the method and expect RuntimeError
        with pytest.raises(
            RuntimeError,
            match="Unable to retrieve the online documents using feature server API",
        ):
            remote_store.retrieve_online_documents(
                config=config,
                table=feature_view,
                requested_features=["feature1"],
                embedding=[0.1, 0.2, 0.3],
                top_k=1,
            )

    @patch("feast.infra.online_stores.remote.get_remote_online_documents")
    def test_retrieve_online_documents_v2_error_response(
        self, mock_get_remote_online_documents, remote_store, config, feature_view
    ):
        """Test retrieve_online_documents_v2 with error response."""
        # Setup mock error response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_get_remote_online_documents.return_value = mock_response

        # Call the method and expect RuntimeError
        with pytest.raises(
            RuntimeError,
            match="Unable to retrieve the online documents using feature server API",
        ):
            remote_store.retrieve_online_documents_v2(
                config=config,
                table=feature_view,
                requested_features=["feature1"],
                embedding=[0.1, 0.2, 0.3],
                top_k=1,
            )

    def test_construct_online_documents_api_json_request(
        self, remote_store, feature_view
    ):
        """Test _construct_online_documents_api_json_request method."""
        result = remote_store._construct_online_documents_api_json_request(
            table=feature_view,
            requested_features=["feature1", "feature2"],
            embedding=[0.1, 0.2, 0.3],
            top_k=5,
            distance_metric="cosine",
        )

        assert result["features"] == [
            "test_feature_view:feature1",
            "test_feature_view:feature2",
        ]
        assert result["query"] == [0.1, 0.2, 0.3]
        assert result["top_k"] == 5
        assert result["distance_metric"] == "cosine"

    def test_construct_online_documents_v2_api_json_request(
        self, remote_store, feature_view
    ):
        """Test _construct_online_documents_v2_api_json_request method."""
        result = remote_store._construct_online_documents_v2_api_json_request(
            table=feature_view,
            requested_features=["feature1"],
            embedding=[0.1, 0.2],
            top_k=3,
            distance_metric="L2",
            query_string="test query",
            api_version=2,
        )

        assert result["features"] == ["test_feature_view:feature1"]
        assert result["query"] == [0.1, 0.2]
        assert result["top_k"] == 3
        assert result["distance_metric"] == "L2"
        assert result["query_string"] == "test query"
        assert result["api_version"] == 2

    def test_extract_requested_feature_value(self, remote_store):
        """Test _extract_requested_feature_value helper method."""
        response_json = {
            "results": [{"values": ["test_value"], "statuses": ["PRESENT"]}]
        }
        feature_name_to_index = {"feature1": 0}

        result = remote_store._extract_requested_feature_value(
            response_json, feature_name_to_index, ["feature1"], 0
        )
        assert isinstance(result, ValueProto)

    def test_is_feature_present(self, remote_store):
        """Test _is_feature_present helper method."""
        response_json = {"results": [{"statuses": ["PRESENT", "NOT_FOUND"]}]}

        assert remote_store._is_feature_present(response_json, 0, 0)
        assert not remote_store._is_feature_present(response_json, 0, 1)


class TestRemoteOnlineStoreGetOnlineFeatures:
    """Tests for RemoteOnlineStore.get_online_features to guard against
    drift from the base OnlineStore class and verify single-request batching."""

    @pytest.fixture
    def remote_store(self):
        return RemoteOnlineStore()

    @pytest.fixture
    def config(self):
        return RepoConfig(
            project="test_project",
            online_store=RemoteOnlineStoreConfig(
                type="remote", path="http://localhost:6566"
            ),
            registry="dummy_registry",
        )

    @pytest.fixture
    def server_response_json(self):
        return {
            "metadata": {"feature_names": ["user_id", "feature1", "feature2"]},
            "results": [
                {"values": [1001, 1002], "statuses": ["PRESENT", "PRESENT"]},
                {"values": [0.5, 0.6], "statuses": ["PRESENT", "PRESENT"]},
                {"values": ["a", "b"], "statuses": ["PRESENT", "NOT_FOUND"]},
            ],
        }

    # ── Signature compatibility ───────────────────────────────────────

    def test_signature_matches_base_class(self):
        """RemoteOnlineStore.get_online_features must accept at least the
        same parameters as OnlineStore.get_online_features so callers
        never hit a TypeError after an upstream signature change."""
        base_sig = inspect.signature(OnlineStore.get_online_features)
        remote_sig = inspect.signature(RemoteOnlineStore.get_online_features)

        base_params = set(base_sig.parameters.keys()) - {"self"}
        remote_params = set(remote_sig.parameters.keys()) - {"self"}

        missing = base_params - remote_params
        assert not missing, (
            f"RemoteOnlineStore.get_online_features is missing parameters "
            f"that the base class defines: {missing}"
        )

    def test_return_type_is_online_response(self):
        """Return annotation must remain OnlineResponse."""
        hints = inspect.get_annotations(RemoteOnlineStore.get_online_features)
        assert hints.get("return") is OnlineResponse

    # ── Single HTTP request ───────────────────────────────────────────

    @patch("feast.infra.online_stores.remote.get_remote_online_features")
    def test_single_http_call_with_feature_list(
        self, mock_get, remote_store, config, server_response_json
    ):
        """Passing a list of features should produce exactly one HTTP call
        with all features in the request body."""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = server_response_json
        mock_get.return_value = mock_resp

        features = ["fv1:feature1", "fv2:feature2"]
        entity_rows = [{"user_id": 1001}, {"user_id": 1002}]

        result = remote_store.get_online_features(
            config=config,
            features=features,
            entity_rows=entity_rows,
            registry=Mock(),
            project="test_project",
        )

        mock_get.assert_called_once()
        req_body = mock_get.call_args[1]["req_body"]
        assert req_body["features"] == features
        assert "feature_service" not in req_body
        assert req_body["entities"] == {"user_id": [1001, 1002]}
        assert isinstance(result, OnlineResponse)

    @patch("feast.infra.online_stores.remote.get_remote_online_features")
    def test_single_http_call_with_feature_service(
        self, mock_get, remote_store, config, server_response_json
    ):
        """Passing a FeatureService should send its name, not expand FVs."""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = server_response_json
        mock_get.return_value = mock_resp

        fs = Mock(spec=FeatureService)
        fs.name = "credit_scoring_v1"
        entity_rows = [{"user_id": 1001}]

        remote_store.get_online_features(
            config=config,
            features=fs,
            entity_rows=entity_rows,
            registry=Mock(),
            project="test_project",
        )

        mock_get.assert_called_once()
        req_body = mock_get.call_args[1]["req_body"]
        assert req_body["feature_service"] == "credit_scoring_v1"
        assert "features" not in req_body

    # ── Entity row format handling ────────────────────────────────────

    @patch("feast.infra.online_stores.remote.get_remote_online_features")
    def test_list_of_dicts_entity_rows(
        self, mock_get, remote_store, config, server_response_json
    ):
        """List-of-dicts entity rows should be converted to columnar."""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = server_response_json
        mock_get.return_value = mock_resp

        entity_rows = [{"user_id": 1}, {"user_id": 2}]
        remote_store.get_online_features(
            config=config,
            features=["fv:f1"],
            entity_rows=entity_rows,
            registry=Mock(),
            project="test_project",
        )

        req_body = mock_get.call_args[1]["req_body"]
        assert req_body["entities"] == {"user_id": [1, 2]}

    @patch("feast.infra.online_stores.remote.get_remote_online_features")
    def test_columnar_entity_rows(
        self, mock_get, remote_store, config, server_response_json
    ):
        """Columnar (dict-of-lists) entity rows should be passed through."""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = server_response_json
        mock_get.return_value = mock_resp

        entity_rows = {"user_id": [1, 2]}
        remote_store.get_online_features(
            config=config,
            features=["fv:f1"],
            entity_rows=entity_rows,
            registry=Mock(),
            project="test_project",
        )

        req_body = mock_get.call_args[1]["req_body"]
        assert req_body["entities"] == {"user_id": [1, 2]}

    # ── Response parsing ──────────────────────────────────────────────

    def test_build_online_response_present_values(self, remote_store):
        """_build_online_response_from_json should reconstruct an
        OnlineResponse with correct feature names and values."""
        resp_json = {
            "metadata": {"feature_names": ["user_id", "score"]},
            "results": [
                {"values": [101], "statuses": ["PRESENT"]},
                {"values": [0.95], "statuses": ["PRESENT"]},
            ],
        }

        result = remote_store._build_online_response_from_json(resp_json)
        assert isinstance(result, OnlineResponse)
        proto = result.proto
        assert list(proto.metadata.feature_names.val) == ["user_id", "score"]
        assert len(proto.results) == 2

    def test_build_online_response_null_values(self, remote_store):
        """Null values in the response should become empty ValueProto."""
        resp_json = {
            "metadata": {"feature_names": ["f1"]},
            "results": [
                {"values": [None], "statuses": ["NOT_FOUND"]},
            ],
        }

        result = remote_store._build_online_response_from_json(resp_json)
        proto = result.proto
        assert proto.results[0].values[0] == ValueProto()

    def test_build_online_response_status_mapping(self, remote_store):
        """All known status strings should map to correct FieldStatus enums."""
        from feast.protos.feast.serving.ServingService_pb2 import FieldStatus

        resp_json = {
            "metadata": {"feature_names": ["f1", "f2", "f3", "f4"]},
            "results": [
                {"values": [1], "statuses": ["PRESENT"]},
                {"values": [None], "statuses": ["NOT_FOUND"]},
                {"values": [None], "statuses": ["NULL_VALUE"]},
                {"values": [None], "statuses": ["OUTSIDE_MAX_AGE"]},
            ],
        }

        result = remote_store._build_online_response_from_json(resp_json)
        proto = result.proto
        assert proto.results[0].statuses[0] == FieldStatus.PRESENT
        assert proto.results[1].statuses[0] == FieldStatus.NOT_FOUND
        assert proto.results[2].statuses[0] == FieldStatus.NULL_VALUE
        assert proto.results[3].statuses[0] == FieldStatus.OUTSIDE_MAX_AGE

    # ── Error handling ────────────────────────────────────────────────

    @patch("feast.infra.online_stores.remote.get_remote_online_features")
    def test_non_200_raises_runtime_error(self, mock_get, remote_store, config):
        """Non-200 responses from the server should raise RuntimeError."""
        mock_resp = Mock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_get.return_value = mock_resp

        with pytest.raises(RuntimeError, match="Failed to get online features"):
            remote_store.get_online_features(
                config=config,
                features=["fv:f1"],
                entity_rows=[{"user_id": 1}],
                registry=Mock(),
                project="test_project",
            )

    # ── full_feature_names passthrough ────────────────────────────────

    @patch("feast.infra.online_stores.remote.get_remote_online_features")
    def test_full_feature_names_passed_to_server(
        self, mock_get, remote_store, config, server_response_json
    ):
        """full_feature_names should be forwarded in the request body."""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = server_response_json
        mock_get.return_value = mock_resp

        remote_store.get_online_features(
            config=config,
            features=["fv:f1"],
            entity_rows=[{"user_id": 1}],
            registry=Mock(),
            project="test_project",
            full_feature_names=True,
        )

        req_body = mock_get.call_args[1]["req_body"]
        assert req_body["full_feature_names"] is True

    @patch("feast.infra.online_stores.remote.get_remote_online_features")
    def test_version_metadata_passed_to_server(
        self, mock_get, remote_store, config, server_response_json
    ):
        """include_feature_view_version_metadata must be forwarded to the
        server so versioned reads work end-to-end."""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = server_response_json
        mock_get.return_value = mock_resp

        remote_store.get_online_features(
            config=config,
            features=["fv:f1"],
            entity_rows=[{"user_id": 1}],
            registry=Mock(),
            project="test_project",
            include_feature_view_version_metadata=True,
        )

        req_body = mock_get.call_args[1]["req_body"]
        assert req_body["include_feature_view_version_metadata"] is True

    @patch("feast.infra.online_stores.remote.get_remote_online_features")
    def test_all_base_class_params_forwarded(
        self, mock_get, remote_store, config, server_response_json
    ):
        """Every parameter that the server's GetOnlineFeaturesRequest
        accepts must appear in the request body when set."""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = server_response_json
        mock_get.return_value = mock_resp

        remote_store.get_online_features(
            config=config,
            features=["fv:f1"],
            entity_rows=[{"user_id": 1}],
            registry=Mock(),
            project="test_project",
            full_feature_names=True,
            include_feature_view_version_metadata=True,
        )

        req_body = mock_get.call_args[1]["req_body"]
        expected_keys = {
            "entities",
            "features",
            "full_feature_names",
            "include_feature_view_version_metadata",
        }
        assert expected_keys.issubset(req_body.keys()), (
            f"Missing keys in req_body: {expected_keys - req_body.keys()}"
        )
