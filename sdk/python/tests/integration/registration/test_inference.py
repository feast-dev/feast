from copy import deepcopy

import pytest

from feast import RepoConfig
from feast.errors import RegistryInferenceFailure
from feast.inference import update_data_sources_with_inferred_event_timestamp_col
from tests.utils.data_source_test_creator import prep_file_source


@pytest.mark.integration
def test_update_file_data_source_with_inferred_event_timestamp_col(simple_dataset_1):
    df_with_two_viable_timestamp_cols = simple_dataset_1.copy(deep=True)
    df_with_two_viable_timestamp_cols["ts_2"] = simple_dataset_1["ts_1"]

    with prep_file_source(df=simple_dataset_1) as file_source:
        data_sources = [
            file_source,
        ]
        update_data_sources_with_inferred_event_timestamp_col(
            data_sources,
            RepoConfig(
                provider="local",
                project="test",
                registry="test.pb",
                entity_key_serialization_version=2,
            ),
        )
        actual_event_timestamp_cols = [
            source.timestamp_field for source in data_sources
        ]

        assert actual_event_timestamp_cols == ["ts_1"]

    with prep_file_source(df=df_with_two_viable_timestamp_cols) as file_source:
        with pytest.raises(RegistryInferenceFailure):
            # two viable timestamp_fields
            update_data_sources_with_inferred_event_timestamp_col(
                [file_source],
                RepoConfig(
                    provider="local",
                    project="test",
                    registry="test.pb",
                    entity_key_serialization_version=2,
                ),
            )


@pytest.mark.integration
def test_update_data_sources_with_inferred_event_timestamp_col(universal_data_sources):
    (_, _, data_sources) = universal_data_sources
    data_sources_copy = deepcopy(data_sources)

    # remove defined timestamp_field to allow for inference
    for data_source in data_sources_copy.values():
        data_source.timestamp_field = None
        data_source.event_timestamp_column = None

    update_data_sources_with_inferred_event_timestamp_col(
        data_sources_copy.values(),
        RepoConfig(
            provider="local",
            project="test",
            registry="test.pb",
            entity_key_serialization_version=2,
        ),
    )
    actual_event_timestamp_cols = [
        source.timestamp_field for source in data_sources_copy.values()
    ]

    assert actual_event_timestamp_cols == ["event_timestamp"] * len(
        data_sources_copy.values()
    )
