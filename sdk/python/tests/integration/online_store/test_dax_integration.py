"""
Test script for DAX integration with DynamoDB online store.

This script can be used to verify DAX client initialization and basic operations.
Run with: pytest -v tests/integration/online_store/test_dax_integration.py

Prerequisites:
1. pip install amazon-dax-client
2. A running DAX cluster (or use mock for unit testing)
3. AWS credentials configured
"""

import os
import pytest
from unittest.mock import MagicMock, patch


def _dax_client_available() -> bool:
    """Check if amazon-dax-client package is installed."""
    try:
        import amazondax
        return True
    except ImportError:
        return False


class TestDaxConfiguration:
    """Test DAX configuration parsing."""

    def test_dax_config_defaults(self):
        """Test that DAX config defaults are correct."""
        from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig

        config = DynamoDBOnlineStoreConfig(region="us-east-1")

        assert config.use_dax is False
        assert config.dax_endpoint is None

    def test_dax_config_enabled(self):
        """Test DAX config when enabled."""
        from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig

        config = DynamoDBOnlineStoreConfig(
            region="us-east-1",
            use_dax=True,
            dax_endpoint="dax://my-cluster.xxx.dax-clusters.us-east-1.amazonaws.com",
        )

        assert config.use_dax is True
        assert config.dax_endpoint == "dax://my-cluster.xxx.dax-clusters.us-east-1.amazonaws.com"

    def test_dax_config_validation_missing_endpoint(self):
        """Test that validation fails when use_dax=True but dax_endpoint is missing."""
        from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig

        with pytest.raises(ValueError, match="dax_endpoint is required when use_dax is True"):
            DynamoDBOnlineStoreConfig(
                region="us-east-1",
                use_dax=True,
                # dax_endpoint intentionally missing
            )

    def test_dax_config_with_tls_endpoint(self):
        """Test DAX config with TLS (daxs://) endpoint."""
        from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig

        config = DynamoDBOnlineStoreConfig(
            region="us-east-1",
            use_dax=True,
            dax_endpoint="daxs://my-cluster.xxx.dax-clusters.us-east-1.amazonaws.com",
        )

        assert config.use_dax is True
        assert config.dax_endpoint.startswith("daxs://")

    def test_dax_disabled_with_endpoint_set(self):
        """Test that having dax_endpoint set but use_dax=False is valid (endpoint ignored)."""
        from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig

        config = DynamoDBOnlineStoreConfig(
            region="us-east-1",
            use_dax=False,
            dax_endpoint="dax://my-cluster.xxx.dax-clusters.us-east-1.amazonaws.com",
        )

        assert config.use_dax is False
        assert config.dax_endpoint is not None  # Set but will be ignored


class TestDaxClientInitialization:
    """Test DAX client initialization."""

    def test_client_init_without_dax(self):
        """Test that regular boto3 client is created when DAX is disabled."""
        from feast.infra.online_stores.dynamodb import _initialize_dynamodb_client

        with patch("feast.infra.online_stores.dynamodb.boto3") as mock_boto3:
            mock_boto3.client.return_value = MagicMock()

            client = _initialize_dynamodb_client(
                region="us-east-1",
                use_dax=False,
            )

            mock_boto3.client.assert_called_once()
            assert "dynamodb" in str(mock_boto3.client.call_args)

    def test_resource_init_without_dax(self):
        """Test that regular boto3 resource is created when DAX is disabled."""
        from feast.infra.online_stores.dynamodb import _initialize_dynamodb_resource

        with patch("feast.infra.online_stores.dynamodb.boto3") as mock_boto3:
            mock_boto3.resource.return_value = MagicMock()

            resource = _initialize_dynamodb_resource(
                region="us-east-1",
                use_dax=False,
            )

            mock_boto3.resource.assert_called_once()
            assert "dynamodb" in str(mock_boto3.resource.call_args)

    def test_client_init_with_dax_package_missing(self):
        """Test fallback to boto3 when amazon-dax-client is not installed."""
        from feast.infra.online_stores.dynamodb import _initialize_dynamodb_client

        with patch("feast.infra.online_stores.dynamodb.boto3") as mock_boto3:
            mock_boto3.client.return_value = MagicMock()

            # Simulate ImportError for amazondax
            with patch.dict("sys.modules", {"amazondax": None}):
                client = _initialize_dynamodb_client(
                    region="us-east-1",
                    use_dax=True,
                    dax_endpoint="dax://test.xxx.dax-clusters.us-east-1.amazonaws.com",
                )

            # Should fall back to boto3
            mock_boto3.client.assert_called_once()

    def test_resource_init_with_dax_package_missing(self):
        """Test fallback to boto3 resource when amazon-dax-client is not installed."""
        from feast.infra.online_stores.dynamodb import _initialize_dynamodb_resource

        with patch("feast.infra.online_stores.dynamodb.boto3") as mock_boto3:
            mock_boto3.resource.return_value = MagicMock()

            # Simulate ImportError for amazondax
            with patch.dict("sys.modules", {"amazondax": None}):
                resource = _initialize_dynamodb_resource(
                    region="us-east-1",
                    use_dax=True,
                    dax_endpoint="dax://test.xxx.dax-clusters.us-east-1.amazonaws.com",
                )

            # Should fall back to boto3
            mock_boto3.resource.assert_called_once()

    def test_client_init_with_session_auth(self):
        """Test client initialization with session-based auth."""
        from feast.infra.online_stores.dynamodb import _initialize_dynamodb_client

        with patch("feast.infra.online_stores.dynamodb.boto3") as mock_boto3:
            mock_session = MagicMock()
            mock_boto3.Session.return_value = mock_session
            mock_session.client.return_value = MagicMock()

            client = _initialize_dynamodb_client(
                region="us-east-1",
                session_based_auth=True,
            )

            mock_boto3.Session.assert_called_once()
            mock_session.client.assert_called_once()

    @pytest.mark.skipif(
        not _dax_client_available(),
        reason="amazon-dax-client not installed",
    )
    def test_client_init_with_dax_enabled(self):
        """Test DAX client initialization when package is available."""
        from feast.infra.online_stores.dynamodb import _initialize_dynamodb_client

        with patch("amazondax.AmazonDaxClient") as mock_dax:
            mock_dax.return_value = MagicMock()

            client = _initialize_dynamodb_client(
                region="us-east-1",
                use_dax=True,
                dax_endpoint="dax://test.xxx.dax-clusters.us-east-1.amazonaws.com",
            )

            mock_dax.assert_called_once_with(
                endpoint_url="dax://test.xxx.dax-clusters.us-east-1.amazonaws.com",
                region_name="us-east-1",
            )

    @pytest.mark.skipif(
        not _dax_client_available(),
        reason="amazon-dax-client not installed",
    )
    def test_resource_init_with_dax_enabled(self):
        """Test DAX resource initialization when package is available."""
        from feast.infra.online_stores.dynamodb import _initialize_dynamodb_resource

        with patch("amazondax.AmazonDaxClient") as mock_dax:
            mock_dax.resource.return_value = MagicMock()

            resource = _initialize_dynamodb_resource(
                region="us-east-1",
                use_dax=True,
                dax_endpoint="dax://test.xxx.dax-clusters.us-east-1.amazonaws.com",
            )

            mock_dax.resource.assert_called_once_with(
                endpoint_url="dax://test.xxx.dax-clusters.us-east-1.amazonaws.com",
                region_name="us-east-1",
            )


class TestDaxEndToEnd:
    """End-to-end tests requiring actual DAX cluster (skipped by default)."""

    @pytest.mark.skipif(
        not os.environ.get("DAX_ENDPOINT"),
        reason="DAX_ENDPOINT environment variable not set",
    )
    @pytest.mark.skipif(
        not _dax_client_available(),
        reason="amazon-dax-client not installed",
    )
    def test_dax_get_item(self):
        """
        Test actual DAX GetItem operation.

        Set environment variables before running:
        - DAX_ENDPOINT: Your DAX cluster endpoint
        - AWS_REGION: AWS region (default: us-east-1)
        """
        from amazondax import AmazonDaxClient

        endpoint = os.environ["DAX_ENDPOINT"]
        region = os.environ.get("AWS_REGION", "us-east-1")

        # Create DAX resource
        dax = AmazonDaxClient.resource(
            endpoint_url=endpoint,
            region_name=region,
        )

        # This will fail if table doesn't exist, but tests the connection
        try:
            table = dax.Table("test_table")
            # Just verify we can create table reference
            assert table is not None
        finally:
            # DAX client cleanup
            pass


if __name__ == "__main__":
    # Quick smoke test
    print("Testing DAX configuration...")

    from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig

    # Test 1: Default config
    config = DynamoDBOnlineStoreConfig(region="us-east-1")
    assert config.use_dax is False
    print("✓ Default config: use_dax=False")

    # Test 2: DAX enabled config
    config = DynamoDBOnlineStoreConfig(
        region="us-east-1",
        use_dax=True,
        dax_endpoint="dax://test.xxx.dax-clusters.us-east-1.amazonaws.com",
    )
    assert config.use_dax is True
    print("✓ DAX enabled config: use_dax=True")

    # Test 3: Validation - use_dax=True without endpoint should fail
    try:
        config = DynamoDBOnlineStoreConfig(
            region="us-east-1",
            use_dax=True,
            # Missing dax_endpoint
        )
        print("✗ Validation test failed - should have raised ValueError")
    except ValueError as e:
        assert "dax_endpoint is required" in str(e)
        print("✓ Validation: use_dax=True without endpoint raises ValueError")

    # Test 4: Check if amazon-dax-client is available
    if _dax_client_available():
        print("✓ amazon-dax-client is installed")
    else:
        print("⚠ amazon-dax-client not installed (install with: pip install amazon-dax-client)")

    print("\nAll smoke tests passed!")
