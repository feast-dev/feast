"""Timestamp normalization must produce tz-aware UTC even for an empty frame.

A zero-row ``entity_df`` is a normal degenerate case in batch scoring, when the
upstream query matched nothing for that run.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import dask.dataframe as dd
import pandas as pd
import pytest

from feast.infra.offline_stores.dask import _filter_ttl, _normalize_timestamp

ENTITY_TS = "entity_timestamp"
EVENT_TS = "event_timestamp"


def _frame(n: int, tz: str | None = None) -> dd.DataFrame:
    stamps = pd.to_datetime([datetime(2026, 2, 1)] * n)
    if tz is not None:
        stamps = stamps.tz_localize(tz)
    return dd.from_pandas(
        pd.DataFrame(
            {
                EVENT_TS: stamps,
                ENTITY_TS: pd.to_datetime([datetime(2026, 2, 1)] * n, utc=True),
                "conv_rate": [0.5] * n,
            }
        ),
        npartitions=1,
    )


@pytest.mark.parametrize("rows", [0, 1])
def test_normalize_timestamp_is_utc_aware_regardless_of_row_count(rows):
    normalized = _normalize_timestamp(_frame(rows), EVENT_TS).compute()
    assert isinstance(normalized[EVENT_TS].dtype, pd.DatetimeTZDtype)
    assert str(normalized[EVENT_TS].dtype.tz) == "UTC"


@pytest.mark.parametrize("rows", [0, 1])
def test_filter_ttl_on_empty_frame_does_not_raise(rows):
    """The tz-naive/tz-aware mismatch used to surface here as a TypeError."""
    feature_view = MagicMock()
    feature_view.ttl = timedelta(days=3650)

    normalized = _normalize_timestamp(_frame(rows), EVENT_TS)
    result = _filter_ttl(normalized, feature_view, ENTITY_TS, EVENT_TS).compute()

    assert len(result) == rows


def test_non_utc_timezone_is_converted_not_passed_through():
    """A non-UTC column also diverged from the declared UTC meta."""
    normalized = _normalize_timestamp(_frame(1, tz="US/Eastern"), EVENT_TS).compute()
    assert str(normalized[EVENT_TS].dtype.tz) == "UTC"
