"""Unit tests for Ray list-column dtype normalization."""

from __future__ import annotations

from typing import Any, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pandas.api.extensions import ExtensionArray, register_extension_dtype

from feast.infra.ray_shared_utils import (
    _cell_to_python,
    _is_nested_arrow_dtype,
    _is_ray_extension_dtype,
    normalize_arrow_dtypes,
)


@register_extension_dtype
class FakeRayTensorDtype(pd.api.extensions.ExtensionDtype):
    """Mimics ray.data TensorDtype (numeric-looking, Ray module path)."""

    name = "fake_ray_tensor"
    type = object
    kind = "O"
    na_value = None

    @classmethod
    def construct_array_type(cls):
        return FakeRayTensorArray

    @classmethod
    def construct_from_string(cls, string: str):
        if string == cls.name:
            return cls()
        raise TypeError(f"Cannot construct FakeRayTensorDtype from {string!r}")


# Match Ray's module path so _is_ray_extension_dtype returns True.
FakeRayTensorDtype.__module__ = "ray.data._internal.object_extensions.pandas"


class FakeTensorElement:
    """Mimics Ray TensorArrayElement: looks like ndarray, isn't one."""

    def __init__(self, data):
        self._data = np.asarray(data)

    def __repr__(self):
        return repr(self._data)

    def tolist(self):
        return self._data.tolist()

    def __array__(self, dtype=None):
        return np.asarray(self._data, dtype=dtype)

    def __eq__(self, other):
        if isinstance(other, FakeTensorElement):
            return np.array_equal(self._data, other._data)
        return NotImplemented


class FakeRayTensorArray(ExtensionArray):
    """Minimal ExtensionArray backing FakeRayTensorDtype."""

    def __init__(self, values: Sequence[Any]):
        self._values = list(values)

    @property
    def dtype(self):
        return FakeRayTensorDtype()

    @property
    def nbytes(self) -> int:
        return len(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(self, item):
        if isinstance(item, (int, np.integer)):
            return self._values[int(item)]
        if isinstance(item, slice):
            return FakeRayTensorArray(self._values[item])
        return FakeRayTensorArray([self._values[i] for i in item])

    def __array__(self, dtype=None):
        return np.asarray(self._values, dtype=object)

    def isna(self):
        return np.array([v is None for v in self._values], dtype=bool)

    def take(self, indices, allow_fill=False, fill_value=None):
        vals = []
        for idx in indices:
            if allow_fill and idx == -1:
                vals.append(fill_value)
            else:
                vals.append(self._values[idx])
        return FakeRayTensorArray(vals)

    def copy(self):
        return FakeRayTensorArray(list(self._values))

    @classmethod
    def _from_sequence(cls, scalars, dtype=None, copy=False):
        return cls(list(scalars))

    def _from_factor(cls, data, dtype=None, copy=False):
        return cls(list(data))


def test_is_ray_extension_dtype_detects_ray_module():
    dtype = FakeRayTensorDtype()
    assert _is_ray_extension_dtype(dtype)
    assert not _is_ray_extension_dtype(pd.Int64Dtype())
    assert not _is_ray_extension_dtype(np.dtype("int64"))


def test_is_nested_arrow_dtype():
    if not hasattr(pd, "ArrowDtype"):
        pytest.skip("pandas ArrowDtype not available")
    assert _is_nested_arrow_dtype(pd.ArrowDtype(pa.list_(pa.int64())))
    assert _is_nested_arrow_dtype(pd.ArrowDtype(pa.large_list(pa.float64())))
    assert not _is_nested_arrow_dtype(pd.ArrowDtype(pa.int64()))
    assert not _is_nested_arrow_dtype(pd.Int64Dtype())


def test_cell_to_python_converts_array_likes():
    assert _cell_to_python(np.array([1, 2])) == [1, 2]
    value = FakeTensorElement([1, 1])
    assert not isinstance(value, (np.ndarray, list))
    assert _cell_to_python(value) == [1, 1]
    assert isinstance(_cell_to_python(value), list)
    assert _cell_to_python([3, 4]) == [3, 4]
    assert _cell_to_python(None) is None


def test_normalize_ray_tensor_column_end_to_end():
    arr = FakeRayTensorArray([FakeTensorElement([1, 1]), FakeTensorElement([2, 2])])
    df = pd.DataFrame({"value": arr})

    # Sanity: this is the dtype shape Ray 2.58 returns for uniform lists.
    assert isinstance(df["value"].dtype, FakeRayTensorDtype)
    assert _is_ray_extension_dtype(df["value"].dtype)
    assert not isinstance(df["value"].iloc[0], (np.ndarray, list))

    out = normalize_arrow_dtypes(df)
    assert pd.api.types.is_object_dtype(out["value"].dtype)
    assert isinstance(out["value"].iloc[0], list)
    assert out["value"].iloc[0] == [1, 1]

    table = pa.Table.from_pandas(out)
    assert pa.types.is_list(table.schema.field("value").type)


def test_normalize_arrow_list_column_to_object_and_arrow_list():
    if not hasattr(pd, "ArrowDtype"):
        pytest.skip("pandas ArrowDtype not available")

    arrow_lists = pd.Series(
        [[1, 1], [], [2, 2]],
        dtype=pd.ArrowDtype(pa.list_(pa.int64())),
    )
    df = pd.DataFrame({"value": arrow_lists, "id": [1, 2, 3]})
    out = normalize_arrow_dtypes(df)

    assert pd.api.types.is_object_dtype(out["value"].dtype)
    assert pd.api.types.is_integer_dtype(out["id"].dtype)
    assert isinstance(out["value"].iloc[0], list)
    assert out["value"].iloc[1] == []

    table = pa.Table.from_pandas(out)
    assert pa.types.is_list(table.schema.field("value").type)


def test_normalize_preserves_scalar_extension_dtypes():
    df = pd.DataFrame(
        {
            "ints": pd.Series([1, 2], dtype="Int64"),
            "floats": pd.Series([1.0, 2.0], dtype="Float64"),
        }
    )
    out = normalize_arrow_dtypes(df)
    assert str(out["ints"].dtype) == "Int64"
    assert str(out["floats"].dtype) == "Float64"


def test_normalize_preserves_plain_numpy_columns():
    df = pd.DataFrame({"value": np.array([1, 2], dtype="int64")})
    out = normalize_arrow_dtypes(df)
    assert pd.api.types.is_integer_dtype(out["value"].dtype)
