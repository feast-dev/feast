"""Tests for feast.null_utils — safe null checking for scalar columns.

Reproduces the crash from https://github.com/feast-dev/feast/issues/6255
and verifies the fix handles all edge cases.
"""

import numpy as np
import pytest

from feast.null_utils import is_scalar_null


class TestIsScalarNull:
    """Tests for is_scalar_null."""

    def test_none_is_null(self):
        assert is_scalar_null(None) is True

    def test_nan_is_null(self):
        assert is_scalar_null(float("nan")) is True

    def test_np_nan_is_null(self):
        assert is_scalar_null(np.nan) is True

    def test_empty_numpy_array_is_null(self):
        """This is the exact crash scenario from issue #6255."""
        assert is_scalar_null(np.array([])) is True

    def test_numpy_array_with_nan_is_null(self):
        assert is_scalar_null(np.array([np.nan])) is True

    def test_numpy_array_with_values_is_not_null(self):
        assert is_scalar_null(np.array([1.0, 2.0])) is False

    def test_int_is_not_null(self):
        assert is_scalar_null(42) is False

    def test_zero_is_not_null(self):
        assert is_scalar_null(0) is False

    def test_float_is_not_null(self):
        assert is_scalar_null(3.14) is False

    def test_string_is_not_null(self):
        assert is_scalar_null("hello") is False

    def test_empty_string_is_not_null(self):
        assert is_scalar_null("") is False

    def test_bytes_is_not_null(self):
        assert is_scalar_null(b"data") is False

    def test_bool_true_is_not_null(self):
        assert is_scalar_null(True) is False

    def test_bool_false_is_not_null(self):
        assert is_scalar_null(False) is False

    def test_np_bool_is_not_null(self):
        assert is_scalar_null(np.bool_(True)) is False

    def test_empty_list_is_null(self):
        """Empty list in a scalar column should be treated as null."""
        assert is_scalar_null([]) is True

    def test_list_with_values_is_not_null(self):
        assert is_scalar_null([1, 2, 3]) is False
