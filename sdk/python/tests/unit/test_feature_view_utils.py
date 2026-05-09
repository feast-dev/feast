"""Unit tests for feast.feature_view_utils helper functions."""

from datetime import datetime, timedelta, timezone

import pytest

from feast.feature_view_utils import generate_time_chunks

# ---------------------------------------------------------------------------
# generate_time_chunks
# ---------------------------------------------------------------------------


def _utc(dt: datetime) -> datetime:
    """Attach UTC timezone to a naive datetime for easier comparisons."""
    return dt.replace(tzinfo=timezone.utc)


class TestGenerateTimeChunks:
    """Tests for generate_time_chunks()."""

    def test_exact_multiple_of_chunk_size(self):
        """Range that divides evenly into chunks should produce no remainder."""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 1, 12)  # 12 hours
        chunks = list(generate_time_chunks(start, end, timedelta(hours=4)))
        assert len(chunks) == 3
        assert chunks[0] == (datetime(2024, 1, 1, 0), datetime(2024, 1, 1, 4))
        assert chunks[1] == (datetime(2024, 1, 1, 4), datetime(2024, 1, 1, 8))
        assert chunks[2] == (datetime(2024, 1, 1, 8), datetime(2024, 1, 1, 12))

    def test_remainder_chunk_is_smaller(self):
        """When the range is not evenly divisible the last chunk should be shorter."""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 1, 10)  # 10 hours
        chunks = list(generate_time_chunks(start, end, timedelta(hours=4)))
        assert len(chunks) == 3
        # last chunk is only 2 hours
        assert chunks[-1] == (datetime(2024, 1, 1, 8), datetime(2024, 1, 1, 10))

    def test_chunk_size_larger_than_range(self):
        """A single chunk should be returned when chunk_size >= total range."""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 1, 6)
        chunks = list(generate_time_chunks(start, end, timedelta(days=1)))
        assert len(chunks) == 1
        assert chunks[0] == (start, end)

    def test_single_chunk_exact_fit(self):
        """chunk_size == total range should produce exactly one chunk."""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 2)
        chunks = list(generate_time_chunks(start, end, timedelta(days=1)))
        assert len(chunks) == 1
        assert chunks[0] == (start, end)

    def test_minute_granularity(self):
        """Minute-level chunking should work correctly."""
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 1, 0, 30, 0)  # 30 minutes
        chunks = list(generate_time_chunks(start, end, timedelta(minutes=10)))
        assert len(chunks) == 3
        assert chunks[0][0] == start
        assert chunks[-1][1] == end

    def test_second_granularity(self):
        """Sub-minute chunking should work correctly."""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 1, 0, 0, 10)  # 10 seconds
        chunks = list(generate_time_chunks(start, end, timedelta(seconds=3)))
        assert len(chunks) == 4  # 3+3+3+1
        assert chunks[-1][1] == end

    def test_chunks_are_contiguous_and_non_overlapping(self):
        """Each chunk's end must equal the next chunk's start."""
        start = datetime(2024, 3, 1)
        end = datetime(2024, 3, 8)
        chunks = list(generate_time_chunks(start, end, timedelta(hours=17)))
        for i in range(len(chunks) - 1):
            assert chunks[i][1] == chunks[i + 1][0], (
                f"Gap/overlap between chunk {i} and {i + 1}"
            )

    def test_full_coverage(self):
        """Union of all chunks must exactly cover [start, end)."""
        start = datetime(2024, 2, 1)
        end = datetime(2024, 2, 15)
        chunks = list(generate_time_chunks(start, end, timedelta(days=3)))
        assert chunks[0][0] == start
        assert chunks[-1][1] == end

    def test_timezone_aware_datetimes(self):
        """Timezone-aware datetimes should work the same way."""
        start = _utc(datetime(2024, 1, 1))
        end = _utc(datetime(2024, 1, 1, 8))
        chunks = list(generate_time_chunks(start, end, timedelta(hours=3)))
        assert len(chunks) == 3
        assert chunks[0][0].tzinfo is not None
        assert chunks[-1][1] == end

    def test_empty_range_yields_no_chunks(self):
        """start == end should produce no chunks without raising."""
        start = datetime(2024, 1, 1)
        chunks = list(generate_time_chunks(start, start, timedelta(hours=1)))
        assert chunks == []

    def test_zero_chunk_size_raises(self):
        """A non-positive chunk_size must raise ValueError."""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 2)
        with pytest.raises(ValueError, match="chunk_size must be positive"):
            list(generate_time_chunks(start, end, timedelta(0)))

    def test_negative_chunk_size_raises(self):
        """A negative chunk_size must raise ValueError."""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 2)
        with pytest.raises(ValueError, match="chunk_size must be positive"):
            list(generate_time_chunks(start, end, timedelta(seconds=-1)))

    def test_returns_iterator(self):
        """generate_time_chunks should return a lazy iterator, not a list."""
        import types

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 2)
        result = generate_time_chunks(start, end, timedelta(hours=6))
        assert isinstance(result, types.GeneratorType)
