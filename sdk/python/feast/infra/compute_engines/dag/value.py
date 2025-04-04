from typing import Any, Optional

from feast.infra.compute_engines.dag.model import DAGFormat


class DAGValue:
    def __init__(self, data: Any, format: DAGFormat, metadata: Optional[dict] = None):
        self.data = data
        self.format = format
        self.metadata = metadata or {}

    def assert_format(self, expected: DAGFormat):
        if self.format != expected:
            raise ValueError(f"Expected format {expected}, but got {self.format}")
