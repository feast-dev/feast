import pyarrow as pa
import pytest

from feast.type_map import feast_value_type_to_python_type
from feast.utils import _columns_to_proto_values
from feast.value_type import ValueType

STRING_MAP = pa.map_(pa.string(), pa.string())
MAPS = [[("tier", "gold")], None, [("tier", "silver"), ("city", "x")], []]
EXPECTED = [{"tier": "gold"}, None, {"tier": "silver", "city": "x"}, {}]


def _map_values(batch_type, array):
    data = batch_type.from_pydict({"value": array})
    protos = _columns_to_proto_values(data, [("value", ValueType.MAP)])["value"]
    return [feast_value_type_to_python_type(p) for p in protos]


@pytest.mark.parametrize("batch_type", [pa.RecordBatch, pa.Table])
def test_map_column_converts_to_map_values(batch_type):
    assert _map_values(batch_type, pa.array(MAPS, STRING_MAP)) == EXPECTED


def test_map_column_read_back_through_pandas_converts_to_map_values():
    # The file offline store reads Parquet through pandas, which turns a map
    # column into a list<list<string>> column of key/value pairs.
    pairs = [None if m is None else [list(p) for p in m] for m in MAPS]
    array = pa.array(pairs, pa.list_(pa.list_(pa.string())))

    assert _map_values(pa.RecordBatch, array) == EXPECTED


def test_map_column_rejects_values_that_are_not_pairs():
    array = pa.array([["tier", "gold", "x"]], pa.list_(pa.string()))

    with pytest.raises(TypeError, match="Expected dict for MAP type"):
        _map_values(pa.RecordBatch, array)
