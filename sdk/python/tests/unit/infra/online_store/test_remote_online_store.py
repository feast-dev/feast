import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from typing import List, Optional

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.infra.online_stores.remote import RemoteOnlineStore, RemoteOnlineStoreConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Float32, String, Int64
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
                type="remote",
                path="http://localhost:6566"
            ),
            registry="dummy_registry"
        )

    @pytest.fixture
    def config_with_cert(self):
        """Create a RepoConfig with RemoteOnlineStoreConfig including TLS cert."""
        return RepoConfig(
            project="test_project",
            online_store=RemoteOnlineStoreConfig(
                type="remote",
                path="http://localhost:6566",
                cert="/path/to/cert.pem"
            ),
            registry="dummy_registry"
        )

    @pytest.fixture
    def feature_view(self):
        """Create a test FeatureView."""
        entity = Entity(name="user_id", description="User ID", value_type=ValueType.INT64)
        source = FileSource(
            path="test.parquet",
            timestamp_field="event_timestamp"
        )
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
                    "statuses": ["PRESENT", "PRESENT"]
                },  # feature1
                {
                    "values": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
                    "statuses": ["PRESENT", "PRESENT"],
                    "event_timestamps": ["2023-01-01T00:00:00Z", "2023-01-01T01:00:00Z"]
                },  # embedding
                {
                    "values": [0.85, 0.92],
                    "statuses": ["PRESENT", "PRESENT"]
                },  # distance
                {
                    "values": [123, 456],
                    "statuses": ["PRESENT", "PRESENT"]
                }   # user_id
            ]
        }

    @pytest.fixture
    def mock_successful_response_v2(self):
        """Create a mock successful HTTP response for documents retrieval v2."""
        return {
            "metadata": {
                "feature_names": ["user_id", "feature1"]
            },
            "results": [
                {
                    "values": [123, 456],
                    "statuses": ["PRESENT", "PRESENT"]
                },  # user_id
                {
                    "values": ["test_value_1", "test_value_2"],
                    "statuses": ["PRESENT", "PRESENT"],
                    "event_timestamps": ["2023-01-01T00:00:00Z", "2023-01-01T01:00:00Z"]
                }   # feature1
            ]
        }

    @patch('feast.infra.online_stores.remote.get_remote_online_documents')
    def test_retrieve_online_documents_success(
        self, 
        mock_get_remote_online_documents, 
        remote_store, 
        config, 
        feature_view, 
        mock_successful_response
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
            distance_metric="L2"
        )

        # Verify the call was made correctly
        mock_get_remote_online_documents.assert_called_once()
        call_args = mock_get_remote_online_documents.call_args
        assert call_args[1]['config'] == config
        
        # Parse the request body to verify it's correct
        req_body = json.loads(call_args[1]['req_body'])
        assert req_body['features'] == ['test_feature_view:feature1']
        assert req_body['query'] == [0.1, 0.2, 0.3]
        assert req_body['top_k'] == 2
        assert req_body['distance_metric'] == "L2"

        # Verify the result
        assert len(result) == 2
        event_ts, entity_key_proto, feature_val, vector_value, distance_val = result[0]
        
        # Check event timestamp
        assert isinstance(event_ts, datetime)
        
        # Check that we got ValueProto objects
        assert isinstance(feature_val, ValueProto)
        assert isinstance(vector_value, ValueProto)
        assert isinstance(distance_val, ValueProto)

    @patch('feast.infra.online_stores.remote.get_remote_online_documents')
    def test_retrieve_online_documents_v2_success(
        self, 
        mock_get_remote_online_documents, 
        remote_store, 
        config, 
        feature_view, 
        mock_successful_response_v2
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
            query_string="test query"
        )

        # Verify the call was made correctly
        mock_get_remote_online_documents.assert_called_once()
        call_args = mock_get_remote_online_documents.call_args
        assert call_args[1]['config'] == config
        
        # Parse the request body to verify it's correct
        req_body = json.loads(call_args[1]['req_body'])
        assert req_body['features'] == ['test_feature_view:feature1']
        assert req_body['query'] == [0.1, 0.2, 0.3]
        assert req_body['top_k'] == 2
        assert req_body['distance_metric'] == "cosine"
        assert req_body['query_string'] == "test query"
        assert req_body['api_version'] == 2

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

    @patch('feast.infra.online_stores.remote.get_remote_online_documents')
    def test_retrieve_online_documents_with_cert(
        self, 
        mock_get_remote_online_documents, 
        remote_store, 
        config_with_cert, 
        feature_view, 
        mock_successful_response
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
            top_k=1
        )

        # Verify the call was made
        mock_get_remote_online_documents.assert_called_once()
        assert len(result) == 2

    @patch('feast.infra.online_stores.remote.get_remote_online_documents')
    def test_retrieve_online_documents_error_response(
        self, 
        mock_get_remote_online_documents, 
        remote_store, 
        config, 
        feature_view
    ):
        """Test retrieve_online_documents with error response."""
        # Setup mock error response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_get_remote_online_documents.return_value = mock_response

        # Call the method and expect RuntimeError
        with pytest.raises(RuntimeError, match="Unable to retrieve the online documents using feature server API"):
            remote_store.retrieve_online_documents(
                config=config,
                table=feature_view,
                requested_features=["feature1"],
                embedding=[0.1, 0.2, 0.3],
                top_k=1
            )

    @patch('feast.infra.online_stores.remote.get_remote_online_documents')
    def test_retrieve_online_documents_v2_error_response(
        self, 
        mock_get_remote_online_documents, 
        remote_store, 
        config, 
        feature_view
    ):
        """Test retrieve_online_documents_v2 with error response."""
        # Setup mock error response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_get_remote_online_documents.return_value = mock_response

        # Call the method and expect RuntimeError
        with pytest.raises(RuntimeError, match="Unable to retrieve the online documents using feature server API"):
            remote_store.retrieve_online_documents_v2(
                config=config,
                table=feature_view,
                requested_features=["feature1"],
                embedding=[0.1, 0.2, 0.3],
                top_k=1
            )

    def test_construct_online_documents_api_json_request(self, remote_store, feature_view):
        """Test _construct_online_documents_api_json_request method."""
        result = remote_store._construct_online_documents_api_json_request(
            table=feature_view,
            requested_features=["feature1", "feature2"],
            embedding=[0.1, 0.2, 0.3],
            top_k=5,
            distance_metric="cosine"
        )
        
        parsed_result = json.loads(result)
        assert parsed_result["features"] == ["test_feature_view:feature1", "test_feature_view:feature2"]
        assert parsed_result["query"] == [0.1, 0.2, 0.3]
        assert parsed_result["top_k"] == 5
        assert parsed_result["distance_metric"] == "cosine"

    def test_construct_online_documents_v2_api_json_request(self, remote_store, feature_view):
        """Test _construct_online_documents_v2_api_json_request method."""
        result = remote_store._construct_online_documents_v2_api_json_request(
            table=feature_view,
            requested_features=["feature1"],
            embedding=[0.1, 0.2],
            top_k=3,
            distance_metric="L2",
            query_string="test query",
            api_version=2
        )
        
        parsed_result = json.loads(result)
        assert parsed_result["features"] == ["test_feature_view:feature1"]
        assert parsed_result["query"] == [0.1, 0.2]
        assert parsed_result["top_k"] == 3
        assert parsed_result["distance_metric"] == "L2"
        assert parsed_result["query_string"] == "test query"
        assert parsed_result["api_version"] == 2


    def test_extract_requested_feature_value(self, remote_store):
        """Test _extract_requested_feature_value helper method."""
        response_json = {
            "results": [
                {
                    "values": ["test_value"],
                    "statuses": ["PRESENT"]
                }
            ]
        }
        feature_name_to_index = {"feature1": 0}
        
        result = remote_store._extract_requested_feature_value(
            response_json, feature_name_to_index, ["feature1"], 0
        )
        assert isinstance(result, ValueProto)

    def test_is_feature_present(self, remote_store):
        """Test _is_feature_present helper method."""
        response_json = {
            "results": [
                {
                    "statuses": ["PRESENT", "NOT_FOUND"]
                }
            ]
        }
        
        assert remote_store._is_feature_present(response_json, 0, 0) == True
        assert remote_store._is_feature_present(response_json, 0, 1) == False 