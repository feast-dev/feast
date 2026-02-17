# Copyright 2026 The Feast Authors
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

"""
Configuration classes for Feast OpenLineage integration.
"""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class OpenLineageConfig:
    """
    Configuration for OpenLineage integration.

    Attributes:
        enabled: Whether OpenLineage integration is enabled
        transport_type: Type of transport (http, console, file, kafka)
        transport_url: URL for HTTP transport
        transport_endpoint: API endpoint for HTTP transport
        api_key: Optional API key for authentication
        namespace: Default namespace for Feast jobs and datasets
        producer: Producer identifier for OpenLineage events
        emit_on_apply: Emit lineage events when feast apply is called
        emit_on_materialize: Emit lineage events during materialization
        additional_config: Additional transport-specific configuration
    """

    enabled: bool = True
    transport_type: str = "console"
    transport_url: Optional[str] = None
    transport_endpoint: str = "api/v1/lineage"
    api_key: Optional[str] = None
    namespace: str = "feast"
    producer: str = "feast"
    emit_on_apply: bool = True
    emit_on_materialize: bool = True
    additional_config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "OpenLineageConfig":
        """
        Create OpenLineageConfig from a dictionary.

        Args:
            config_dict: Dictionary containing configuration values

        Returns:
            OpenLineageConfig instance
        """
        return cls(
            enabled=config_dict.get("enabled", True),
            transport_type=config_dict.get("transport_type", "console"),
            transport_url=config_dict.get("transport_url"),
            transport_endpoint=config_dict.get("transport_endpoint", "api/v1/lineage"),
            api_key=config_dict.get("api_key"),
            namespace=config_dict.get("namespace", "feast"),
            producer=config_dict.get("producer", "feast"),
            emit_on_apply=config_dict.get("emit_on_apply", True),
            emit_on_materialize=config_dict.get("emit_on_materialize", True),
            additional_config=config_dict.get("additional_config", {}),
        )

    @classmethod
    def from_env(cls) -> "OpenLineageConfig":
        """
        Create OpenLineageConfig from environment variables.

        Environment variables:
            FEAST_OPENLINEAGE_ENABLED: Enable/disable OpenLineage (default: true)
            FEAST_OPENLINEAGE_TRANSPORT_TYPE: Transport type (default: console)
            FEAST_OPENLINEAGE_URL: HTTP transport URL
            FEAST_OPENLINEAGE_ENDPOINT: API endpoint (default: api/v1/lineage)
            FEAST_OPENLINEAGE_API_KEY: API key for authentication
            FEAST_OPENLINEAGE_NAMESPACE: Default namespace (default: feast)
            FEAST_OPENLINEAGE_PRODUCER: Producer identifier

        Returns:
            OpenLineageConfig instance
        """
        return cls(
            enabled=os.getenv("FEAST_OPENLINEAGE_ENABLED", "true").lower() == "true",
            transport_type=os.getenv("FEAST_OPENLINEAGE_TRANSPORT_TYPE", "console"),
            transport_url=os.getenv("FEAST_OPENLINEAGE_URL"),
            transport_endpoint=os.getenv(
                "FEAST_OPENLINEAGE_ENDPOINT", "api/v1/lineage"
            ),
            api_key=os.getenv("FEAST_OPENLINEAGE_API_KEY"),
            namespace=os.getenv("FEAST_OPENLINEAGE_NAMESPACE", "feast"),
            producer=os.getenv("FEAST_OPENLINEAGE_PRODUCER", "feast"),
            emit_on_apply=os.getenv("FEAST_OPENLINEAGE_EMIT_ON_APPLY", "true").lower()
            == "true",
            emit_on_materialize=os.getenv(
                "FEAST_OPENLINEAGE_EMIT_ON_MATERIALIZE", "true"
            ).lower()
            == "true",
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.

        Returns:
            Dictionary representation of the configuration
        """
        return {
            "enabled": self.enabled,
            "transport_type": self.transport_type,
            "transport_url": self.transport_url,
            "transport_endpoint": self.transport_endpoint,
            "api_key": self.api_key,
            "namespace": self.namespace,
            "producer": self.producer,
            "emit_on_apply": self.emit_on_apply,
            "emit_on_materialize": self.emit_on_materialize,
            "additional_config": self.additional_config,
        }

    def get_transport_config(self) -> Dict[str, Any]:
        """
        Get transport-specific configuration for OpenLineage client.

        Returns:
            Dictionary with transport configuration
        """
        config: Dict[str, Any] = {"type": self.transport_type}

        if self.transport_type == "http":
            if not self.transport_url:
                raise ValueError("transport_url is required for HTTP transport")
            config["url"] = self.transport_url
            config["endpoint"] = self.transport_endpoint
            if self.api_key:
                config["auth"] = {
                    "type": "api_key",
                    "apiKey": self.api_key,
                }
        elif self.transport_type == "file":
            config["log_file_path"] = self.additional_config.get(
                "log_file_path", "openlineage_events.json"
            )
        elif self.transport_type == "kafka":
            config["bootstrap_servers"] = self.additional_config.get(
                "bootstrap_servers"
            )
            config["topic"] = self.additional_config.get("topic", "openlineage.events")

        # Merge additional config
        config.update(self.additional_config)

        return config
