import random

import pytest

from feast import FeatureService
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import customer, driver


@pytest.mark.benchmark
@pytest.mark.integration
def test_online_retrieval(environment, universal_data_sources, benchmark):

    fs = environment.feature_store
    entities, datasets, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    feature_service = FeatureService(
        "convrate_plus100",
        features=[feature_views["driver"][["conv_rate"]], feature_views["driver_odfv"]],
    )

    feast_objects = []
    feast_objects.extend(feature_views.values())
    feast_objects.extend([driver(), customer(), feature_service])
    fs.apply(feast_objects)
    fs.materialize(environment.start_date, environment.end_date)

    sample_drivers = random.sample(entities["driver"], 10)

    sample_customers = random.sample(entities["customer"], 10)

    entity_rows = [
        {"driver": d, "customer_id": c, "val_to_add": 50}
        for (d, c) in zip(sample_drivers, sample_customers)
    ]

    feature_refs = [
        "driver_stats:conv_rate",
        "driver_stats:avg_daily_trips",
        "customer_profile:current_balance",
        "customer_profile:avg_passenger_count",
        "customer_profile:lifetime_trip_count",
        "conv_rate_plus_100:conv_rate_plus_100",
        "conv_rate_plus_100:conv_rate_plus_val_to_add",
        "global_stats:num_rides",
        "global_stats:avg_ride_length",
    ]
    unprefixed_feature_refs = [f.rsplit(":", 1)[-1] for f in feature_refs if ":" in f]
    # Remove the on demand feature view output features, since they're not present in the source dataframe
    unprefixed_feature_refs.remove("conv_rate_plus_100")
    unprefixed_feature_refs.remove("conv_rate_plus_val_to_add")

    benchmark(
        fs.get_online_features, features=feature_refs, entity_rows=entity_rows,
    )
