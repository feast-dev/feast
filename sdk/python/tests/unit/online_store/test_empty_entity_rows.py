"""An empty batch of entities must not surface internal errors.

A caller whose upstream query matched nothing sends zero rows. That used to hit
``IndexError: list index out of range`` or ``KeyError: 'pop from an empty set'``,
which the feature server turned into a 500.
"""

import asyncio

import pytest
from fastapi import status as HttpStatusCode

from feast.errors import MissingJoinKeyValuesException
from feast.utils import _validate_entity_values
from tests.utils.cli_repo_creator import CliRunner, get_example_repo

FEATURES = ["driver_locations:lat", "driver_locations:lon"]


def test_validate_entity_values_treats_no_columns_as_zero_rows():
    """Popping the empty set raised instead of reporting zero rows."""
    assert _validate_entity_values({}) == 0


def test_missing_join_key_values_is_a_client_error():
    exc = MissingJoinKeyValuesException(["driver_id"], [], [])
    # Callers caught KeyError before this carried a status; keep that working.
    assert isinstance(exc, KeyError)
    assert exc.http_status_code() == HttpStatusCode.HTTP_400_BAD_REQUEST


def test_join_key_present_with_zero_values_returns_an_empty_response():
    """A well-formed request that simply has no rows this run."""
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "file"
    ) as store:
        response = store.get_online_features(
            features=FEATURES, entity_rows={"driver_id": []}
        ).to_dict()

    assert set(response.keys()) == {"driver_id", "lat", "lon"}
    assert all(values == [] for values in response.values())


@pytest.mark.parametrize("entity_rows", [[], {}], ids=["empty_list", "no_columns"])
def test_request_without_join_keys_raises_a_client_error(entity_rows):
    """No column names at all: report the missing join key, not an internal error."""
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "file"
    ) as store:
        with pytest.raises(MissingJoinKeyValuesException) as excinfo:
            store.get_online_features(features=FEATURES, entity_rows=entity_rows)

    assert "driver_id" in str(excinfo.value)


@pytest.mark.parametrize("entity_rows", [[], {}], ids=["empty_list", "no_columns"])
def test_request_without_join_keys_raises_a_client_error_async(entity_rows):
    """``entity_rows=[]`` indexed ``[0]`` on the async path too."""
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"), "file"
    ) as store:
        with pytest.raises(MissingJoinKeyValuesException) as excinfo:
            asyncio.run(
                store.get_online_features_async(
                    features=FEATURES, entity_rows=entity_rows
                )
            )

    assert "driver_id" in str(excinfo.value)
