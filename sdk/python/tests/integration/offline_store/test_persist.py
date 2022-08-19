import pytest

from feast.errors import SavedDatasetLocationAlreadyExists
from feast.saved_dataset import SavedDatasetStorage
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    location,
)


@pytest.mark.integration
@pytest.mark.universal_offline_stores(only=["file"])
def test_persist_does_not_overwrite(environment, universal_data_sources):
    """
    Tests that the persist method does not overwrite an existing location in the offline store.

    This test currently is only run against the file offline store as it is the only implementation
    that prevents overwriting. As more offline stores add this check, they should be added to this test.
    """
    store = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)
    store.apply([driver(), customer(), location(), *feature_views.values()])

    features = [
        "customer_profile:current_balance",
        "customer_profile:avg_passenger_count",
        "customer_profile:lifetime_trip_count",
    ]

    entity_df = datasets.entity_df.drop(
        columns=["order_id", "origin_id", "destination_id"]
    )
    job = store.get_historical_features(
        entity_df=entity_df,
        features=features,
    )

    with pytest.raises(SavedDatasetLocationAlreadyExists):
        # Copy data source destination to a saved dataset destination.
        saved_dataset_destination = SavedDatasetStorage.from_data_source(
            data_sources.customer
        )

        # This should fail since persisting to a preexisting location is not allowed.
        store.create_saved_dataset(
            from_=job,
            name="my_training_dataset",
            storage=saved_dataset_destination,
        )
