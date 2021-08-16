import random

from tests.integration.feature_repos.test_repo_configuration import (
    Environment,
    parametrize_online_test,
)


@parametrize_online_test
def test_online_retrieval(environment: Environment):
    fs = environment.feature_store
    full_feature_names = environment.test_repo_config.full_feature_names

    sample_drivers = random.sample(environment.driver_entities, 10)
    # drivers_df = environment.driver_df[environment.driver_df['driver_id'].isin(sample_drivers)]
    # print(drivers_df.to_dict())

    sample_customers = random.sample(environment.customer_entities, 10)
    # customers_df = environment.customer_df[environment.customer_df['customer_id'].isin(sample_customers)]
    # print(customers_df.to_dict())

    entity_rows = [{'driver': d, 'customer_id': c} for (d, c) in zip(sample_drivers, sample_customers)]

    online_features = fs.get_online_features(
        features=[
            "driver_stats:conv_rate",
            "driver_stats:avg_daily_trips",
            "customer_profile:current_balance",
            "customer_profile:avg_passenger_count",
            "customer_profile:lifetime_trip_count",
        ],
        entity_rows=entity_rows,
        full_feature_names=full_feature_names
    )
    assert online_features is not None
