import pyarrow as pa

from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.value import DAGValue


class ArrowTableValue(DAGValue):
    def __init__(self, data: pa.Table):
        super().__init__(data, DAGFormat.ARROW)

    def __repr__(self):
        return f"ArrowTableValue(schema={self.data.schema}, rows={self.data.num_rows})"
