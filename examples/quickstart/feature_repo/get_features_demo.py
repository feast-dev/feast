import pandas as pd
from datetime import datetime, timedelta
from feast import FeatureStore

store = FeatureStore(".")


def demo_batch_feature_view():
    entity_df = pd.DataFrame.from_dict({
        "driver_id": [1001, 1002],
        "event_timestamp": [datetime.utcnow() - timedelta(days=2), datetime.utcnow() - timedelta(days=2)],
        "input_value": [100, 200],
    })

    features = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:avg_daily_trips",
            "driver_hourly_stats:created",
        ]
    )
    print('here are the features!\n', features.to_df())


def demo_online_feature_view():
    payload = [
        {
            "driver_id": 1001,
            "int_val": 100
        },
    ]
    features = store.get_online_features(
        features=store.get_feature_service("output_service"),
        entity_rows=payload,
    ).to_dict()

    features['created'] = [_to_ts(j) for j in features['created']]
    print(features)


def _to_ts(t: int) -> datetime:
    return datetime.utcfromtimestamp(t / 1e9)


def main():
    demo_batch_feature_view()
    demo_online_feature_view()


if __name__ == '__main__':
    main()
