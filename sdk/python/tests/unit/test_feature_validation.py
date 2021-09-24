import pytest

from feast.errors import FeatureNameCollisionError
from feast.feature_store import _validate_feature_refs


def test_feature_name_collision_on_historical_retrieval():
    # _validate_feature_refs is the function that checks for colliding feature names
    # check when feature names collide and 'full_feature_names=False'
    with pytest.raises(FeatureNameCollisionError) as error:
        _validate_feature_refs(
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "customer_profile:avg_daily_trips",
            ],
            full_feature_names=False,
        )

        expected_error_message = (
            "Duplicate features named avg_daily_trips found.\n"
            "To resolve this collision, either use the full feature name by setting "
            "'full_feature_names=True', or ensure that the features in question have different names."
        )

        assert str(error.value) == expected_error_message

    # check when feature names collide and 'full_feature_names=True'
    with pytest.raises(FeatureNameCollisionError) as error:
        _validate_feature_refs(
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "customer_profile:avg_daily_trips",
            ],
            full_feature_names=True,
        )

        expected_error_message = (
            "Duplicate features named driver_stats__avg_daily_trips found.\n"
            "To resolve this collision, please ensure that the feature views or their own features "
            "have different names. If you're intentionally joining the same feature view twice on "
            "different sets of entities, please rename one of the feature views with '.with_name'."
        )
        assert str(error.value) == expected_error_message
