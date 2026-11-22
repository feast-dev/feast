"""Unit tests for the MAJORITY_VOTE resolution of LabelView labels."""

from datetime import datetime

import pandas as pd
import pytest

from feast.labeling.conflict_policy import ConflictPolicy
from feast.labeling.conflict_resolver import resolve_conflicts


def _labels(labeler_column, **extra_columns):
    return pd.DataFrame(
        {
            "chunk_id": ["c1", "c1", "c1"],
            "label": ["good", "good", "bad"],
            labeler_column: ["alice", "bob", "alice"],
            "ts": [datetime(2026, 1, day) for day in (1, 2, 3)],
            **extra_columns,
        }
    )


def _resolve(df, labeler_field, feature_names):
    return resolve_conflicts(
        df=df,
        join_key_columns=["chunk_id"],
        feature_name_columns=feature_names,
        timestamp_field="ts",
        labeler_field=labeler_field,
        conflict_policy=ConflictPolicy.MAJORITY_VOTE,
    )


@pytest.mark.parametrize("labeler_field", ["labeler", "annotator"])
def test_majority_vote_sets_the_labeler_field(labeler_field):
    # the labeler field is part of the schema of a LabelView
    result = _resolve(_labels(labeler_field), labeler_field, ["label", labeler_field])

    assert result["label"].tolist() == ["good"]
    assert result[labeler_field].tolist() == ["majority_vote"]


@pytest.mark.parametrize("labeler_field", ["labeler", "annotator"])
def test_majority_vote_keeps_the_labeler_column(labeler_field):
    df = _labels(labeler_field)
    result = _resolve(df, labeler_field, ["label"])

    assert result.columns.tolist() == df.columns.tolist()
    assert result[labeler_field].tolist() == ["majority_vote"]


def test_majority_vote_resolves_a_label_called_labeler():
    df = _labels("annotator", labeler=["vendor_a", "vendor_a", "vendor_b"])
    result = _resolve(df, "annotator", ["label", "annotator", "labeler"])

    assert result["labeler"].tolist() == ["vendor_a"]
    assert result["annotator"].tolist() == ["majority_vote"]
