"""Test OpenLineage configuration in RepoConfig."""

from feast.repo_config import OpenLineageConfig, RepoConfig


def test_openlineage_config_in_repo_config():
    """Test that OpenLineageConfig can be used in RepoConfig."""
    # Test with default OpenLineage config
    repo_config_dict = {
        "project": "test_project",
        "registry": "data/registry.db",
        "provider": "local",
        "online_store": {"type": "sqlite"},
    }
    
    repo_config = RepoConfig(**repo_config_dict)
    
    # Should have default OpenLineage config
    assert hasattr(repo_config, "openlineage_config")
    assert isinstance(repo_config.openlineage_config, OpenLineageConfig)
    assert repo_config.openlineage_config.enabled is False
    assert repo_config.openlineage_config.namespace == "feast"


def test_openlineage_config_with_custom_values():
    """Test OpenLineageConfig with custom values in RepoConfig."""
    repo_config_dict = {
        "project": "test_project",
        "registry": "data/registry.db",
        "provider": "local",
        "online_store": {"type": "sqlite"},
        "openlineage": {
            "enabled": True,
            "transport_type": "http",
            "transport_config": {"url": "http://localhost:5000"},
            "namespace": "my_feast_project",
            "emit_materialization_events": True,
            "emit_retrieval_events": False,
        },
    }
    
    repo_config = RepoConfig(**repo_config_dict)
    
    # Should have custom OpenLineage config
    assert repo_config.openlineage_config.enabled is True
    assert repo_config.openlineage_config.transport_type == "http"
    assert repo_config.openlineage_config.transport_config["url"] == "http://localhost:5000"
    assert repo_config.openlineage_config.namespace == "my_feast_project"
    assert repo_config.openlineage_config.emit_materialization_events is True
    assert repo_config.openlineage_config.emit_retrieval_events is False


def test_openlineage_config_standalone():
    """Test standalone OpenLineageConfig."""
    config = OpenLineageConfig()
    
    # Test defaults
    assert config.enabled is False
    assert config.transport_type == "http"
    assert config.transport_config == {}
    assert config.namespace == "feast"
    assert config.emit_materialization_events is True
    assert config.emit_retrieval_events is False
    
    # Test with custom values
    custom_config = OpenLineageConfig(
        enabled=True,
        transport_type="kafka",
        transport_config={"bootstrap.servers": "localhost:9092"},
        namespace="custom_namespace",
    )
    
    assert custom_config.enabled is True
    assert custom_config.transport_type == "kafka"
    assert custom_config.transport_config["bootstrap.servers"] == "localhost:9092"
    assert custom_config.namespace == "custom_namespace"
