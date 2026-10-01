"""Tests for vector length validation on both the write and materialization paths.

Covers three fixes:
  * the vector field is resolved from the schema, not assumed to be ``features[0]``
  * the check is reachable from the materialization path via ``_convert_arrow_to_proto``
  * length checking is a single vectorized pass rather than a per-row loop
"""

import numpy as np
import pandas as pd
import pyarrow
import pytest

from feast import Entity, FeatureView, Field, FileSource, RequestSource
from feast.feature_store import FeatureStore
from feast.types import Array, Float32, String
from feast.utils import _convert_arrow_to_proto, _validate_vector_field_lengths
from feast.value_type import ValueType

VECTOR_LENGTH = 4


def _feature_view(vector_first: bool = True, vector_length: int = VECTOR_LENGTH):
    """Build a feature view whose vector field is first or second in the schema."""
    vector = Field(
        name="embedding",
        dtype=Array(Float32),
        vector_index=True,
        vector_length=vector_length,
        vector_search_metric="COSINE",
    )
    label = Field(name="label", dtype=String)
    return FeatureView(
        name="fv",
        entities=[
            Entity(name="item_id", join_keys=["item_id"], value_type=ValueType.INT64)
        ],
        schema=[vector, label] if vector_first else [label, vector],
        source=FileSource(
            path="/tmp/unused.parquet", timestamp_field="event_timestamp"
        ),
    )


def _fixed_size_list(dim: int, num_rows: int = 3) -> pyarrow.Array:
    values = pyarrow.array(np.zeros(dim * num_rows, dtype=np.float32))
    return pyarrow.FixedSizeListArray.from_arrays(values, dim)


def _variable_list(lengths) -> pyarrow.Array:
    return pyarrow.array(
        [None if length is None else [0.0] * length for length in lengths],
        pyarrow.list_(pyarrow.float32()),
    )


def _table(embedding: pyarrow.Array) -> pyarrow.Table:
    num_rows = len(embedding)
    return pyarrow.table(
        {
            "embedding": embedding,
            "label": pyarrow.array(["a"] * num_rows),
            "item_id": pyarrow.array(list(range(num_rows))),
        }
    )


def _validate_df(feature_view, df: pd.DataFrame) -> None:
    """Invoke the DataFrame validator without standing up a FeatureStore.

    ``_validate_vector_features`` does not touch instance state, so an
    uninitialized instance is enough.
    """
    store = FeatureStore.__new__(FeatureStore)
    store._validate_vector_features(feature_view, df)


class TestArrowVectorLengthValidation:
    """``_validate_vector_field_lengths`` guards the materialization path."""

    def test_fixed_size_list_matching_length_passes(self):
        _validate_vector_field_lengths(
            _table(_fixed_size_list(VECTOR_LENGTH)), _feature_view()
        )

    def test_fixed_size_list_wrong_length_raises(self):
        with pytest.raises(ValueError, match="does not match expected 4"):
            _validate_vector_field_lengths(_table(_fixed_size_list(8)), _feature_view())

    def test_variable_list_reports_first_offending_row(self):
        table = _table(_variable_list([4, 4, 7, 4]))
        with pytest.raises(ValueError, match="Row 2: Vector length 7"):
            _validate_vector_field_lengths(table, _feature_view())

    def test_vector_field_not_first_is_still_validated(self):
        """Regression: the old check read features[0] and skipped this case."""
        with pytest.raises(ValueError, match="does not match expected 4"):
            _validate_vector_field_lengths(
                _table(_fixed_size_list(8)), _feature_view(vector_first=False)
            )

    def test_unset_vector_length_is_a_no_op(self):
        _validate_vector_field_lengths(
            _table(_fixed_size_list(8)), _feature_view(vector_length=0)
        )

    def test_null_rows_are_tolerated(self):
        table = _table(_variable_list([4, None, 4]))
        _validate_vector_field_lengths(table, _feature_view())

    def test_missing_column_is_a_no_op(self):
        table = pyarrow.table({"label": pyarrow.array(["a", "b"])})
        _validate_vector_field_lengths(table, _feature_view())

    def test_non_list_arrow_type_raises(self):
        table = pyarrow.table(
            {
                "embedding": pyarrow.array([1.0, 2.0]),
                "label": pyarrow.array(["a", "b"]),
            }
        )
        with pytest.raises(ValueError, match="non-list Arrow type"):
            _validate_vector_field_lengths(table, _feature_view())

    def test_record_batch_is_accepted(self):
        batch = _table(_fixed_size_list(8)).to_batches()[0]
        with pytest.raises(ValueError, match="does not match expected 4"):
            _validate_vector_field_lengths(batch, _feature_view())


class TestDataFrameVectorLengthValidation:
    """``_validate_vector_features`` guards the online write path."""

    def test_matching_lengths_pass(self):
        df = pd.DataFrame({"embedding": [[0.0] * 4] * 3, "label": list("abc")})
        _validate_df(_feature_view(), df)

    def test_wrong_length_reports_first_offending_row(self):
        df = pd.DataFrame(
            {"embedding": [[0.0] * 4, [0.0] * 9, [0.0] * 4], "label": list("abc")}
        )
        with pytest.raises(ValueError, match="Row 1: Vector length 9"):
            _validate_df(_feature_view(), df)

    def test_non_sequence_value_reports_its_type(self):
        df = pd.DataFrame(
            {"embedding": [[0.0] * 4, 3.14, [0.0] * 4], "label": list("abc")}
        )
        with pytest.raises(ValueError, match="is not a sequence"):
            _validate_df(_feature_view(), df)

    def test_vector_field_not_first_is_still_validated(self):
        """Regression: the old check read features[0] and skipped this case."""
        df = pd.DataFrame(
            {"embedding": [[0.0] * 4, [0.0] * 9, [0.0] * 4], "label": list("abc")}
        )
        with pytest.raises(ValueError, match="Row 1: Vector length 9"):
            _validate_df(_feature_view(vector_first=False), df)

    def test_unset_vector_length_is_a_no_op(self):
        df = pd.DataFrame({"embedding": [[0.0] * 9] * 3, "label": list("abc")})
        _validate_df(_feature_view(vector_length=0), df)

    def test_missing_column_is_a_no_op(self):
        _validate_df(_feature_view(), pd.DataFrame({"label": list("abc")}))

    def test_numpy_vectors_are_accepted(self):
        df = pd.DataFrame(
            {
                "embedding": [np.zeros(VECTOR_LENGTH, dtype=np.float32)] * 3,
                "label": list("abc"),
            }
        )
        _validate_df(_feature_view(), df)


class TestReviewFeedback:
    """Cases raised in review of #6909."""

    def test_null_vectors_are_tolerated_on_the_dataframe_path(self):
        """A genuine null has no length, and must not be called 'not a sequence'.

        Matches the Arrow path, which already tolerates null rows.
        """
        df = pd.DataFrame(
            {
                "embedding": [[0.0] * VECTOR_LENGTH, None, np.nan],
                "label": list("abc"),
            }
        )
        _validate_df(_feature_view(), df)

    def test_first_offending_row_wins_across_both_failure_modes(self):
        """A bad length in row 0 outranks a non-sequence in row 1."""
        df = pd.DataFrame(
            {"embedding": [[0.0] * 9, 3.14, [0.0] * 4], "label": list("abc")}
        )
        with pytest.raises(ValueError, match="Row 0: Vector length 9"):
            _validate_df(_feature_view(), df)

    def test_non_sequence_wins_when_it_comes_first(self):
        df = pd.DataFrame(
            {"embedding": [3.14, [0.0] * 9, [0.0] * 4], "label": list("abc")}
        )
        with pytest.raises(
            ValueError, match="Row 0: Vector feature .* is not a sequence"
        ):
            _validate_df(_feature_view(), df)

    def test_duplicate_index_reports_a_scalar_type_not_a_series(self):
        """Positional access keeps a duplicate index from yielding a Series."""
        df = pd.DataFrame(
            {"embedding": [[0.0] * 4, 3.14, [0.0] * 4], "label": list("abc")},
            index=[7, 7, 7],
        )
        with pytest.raises(ValueError, match=r"Got: <class 'float'>"):
            _validate_df(_feature_view(), df)

    def test_on_demand_feature_view_output_is_validated(self):
        """_convert_arrow_to_proto must validate before it branches to the ODFV path."""
        from feast.on_demand_feature_view import on_demand_feature_view

        request_source = RequestSource(
            name="req",
            schema=[Field(name="raw", dtype=String)],
        )

        @on_demand_feature_view(
            sources=[request_source],
            schema=[
                Field(
                    name="embedding",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=VECTOR_LENGTH,
                )
            ],
            mode="python",
            write_to_online_store=True,
        )
        def odfv(inputs):
            return {"embedding": inputs["raw"]}

        # Width 8 against a declared 4 must be rejected on the ODFV branch too.
        table = pyarrow.table(
            {
                "embedding": _fixed_size_list(8, num_rows=2),
                "req": pyarrow.array(["a", "b"]),
            }
        )
        with pytest.raises(ValueError, match="does not match expected 4"):
            _convert_arrow_to_proto(table, odfv, {})
