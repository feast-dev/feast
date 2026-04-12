"""Utilities for safely checking null/missing values in scalar columns.

The standard pd.isnull() is vectorized: when given a numpy array (including
an empty one), it returns an array of booleans instead of a scalar bool.
Applying Python's `not` operator to such an array raises:

    ValueError: The truth value of an empty array is ambiguous.

These helpers wrap pd.isnull() to handle array-like values safely.

See: https://github.com/feast-dev/feast/issues/6255
"""

import numpy as np
import pandas as pd
from typing import Any


def is_scalar_null(value: Any) -> bool:
    """Check if a scalar value is null, safely handling array-like values.

    Args:
        value: A scalar value that might be None, NaN, or an array-like
               object that ended up in a scalar feature column.

    Returns:
        True if the value should be treated as null/missing.
    """
    # Fast path for common cases
    if value is None:
        return True
    if isinstance(value, (str, bytes)):
        return False

    # Handle numpy arrays (including empty ones)
    if isinstance(value, np.ndarray):
        if value.size == 0:
            return True
        result = pd.isnull(value)
        return bool(result.any()) if hasattr(result, "any") else bool(result)

    # Handle other array-like objects (lists, tuples, etc.)
    if hasattr(value, "__len__") and not isinstance(value, (str, bytes)):
        try:
            result = pd.isnull(value)
            if hasattr(result, "any"):
                return bool(result.any())
            return bool(result)
        except (ValueError, TypeError):
            return False

    # Plain scalar
    try:
        return bool(pd.isnull(value))
    except (ValueError, TypeError):
        return False
