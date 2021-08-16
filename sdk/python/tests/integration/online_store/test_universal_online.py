from tests.integration.feature_repos.test_repo_configuration import (
    Environment,
    parametrize_online_test,
)


@parametrize_online_test
def test_online_retrieval(environment: Environment):
    fs = environment.feature_store

    online_features = fs.get_online_features(
        features=[
            "driver_stats:conv_rate",
            "driver_stats:avg_daily_trips",
            "customer_profile:current_balance",
            "customer_profile:avg_passenger_count",
            "customer_profile:lifetime_trip_count",
        ],
        entity_rows=[{"driver": 5001, "customer_id": 1001}],
    )
    assert online_features is not None
    print(online_features.to_dict())
