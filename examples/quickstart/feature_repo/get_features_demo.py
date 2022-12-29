import pandas as pd
from datetime import datetime, timedelta
from feast import FeatureStore

store = FeatureStore(".")


def demo_batch_feature_view():
    entity_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1001],
            "event_timestamp": [datetime.utcnow() - timedelta(days=2)],
            "int_val": [100],
        }
    )

    features = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:avg_daily_trips",
            "driver_hourly_stats:created",
            "driver_hourly_stats:event_timestamp",
        ],
    )
    print("batch_features =", features.to_df().to_dict('list'), "\n")


def demo_online_feature_view():
    payload = [
        {"driver_id": 1001, "int_val": 100},
    ]
    fs = store.get_feature_service("output_service")
    features = store.get_online_features(
        features=fs,
        entity_rows=payload,
    ).to_dict()

    print("on demand features =", features, "\n")


def demo_stream_feature_view():
    ts = datetime.utcnow() - timedelta(minutes=16)
    # This is simulating a push to the stream
    event_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1001],
            "event_timestamp": [
                ts,
            ],
            "created": [
                ts,
            ],
            "conv_rate": [1.0],
            "acc_rate": [1.0],
            "avg_daily_trips": [1000],
        }
    )
    event_df["created"] = pd.to_datetime(event_df["created"], utc=True)
    event_df["event_timestamp"] = pd.to_datetime(event_df["event_timestamp"], utc=True)
    store.push("driver_hourly_stats_push_source", event_df)

    payload = [
        {"driver_id": 1001, "int_val": 100},
    ]
    fs = store.get_feature_service("output_stream_service")
    features = store.get_online_features(
        features=fs,
        entity_rows=payload,
    ).to_dict()

    # features['created_ts'] = [_to_ts(j) for j in features['created']]
    print("stream features =", features, "\n")


def _to_ts(t: int) -> datetime:
    return datetime.utcfromtimestamp(t / 1e9)


def main():
    print("\nbegin demo...")
    demo_batch_feature_view()
    demo_online_feature_view()
    demo_stream_feature_view()
    print("...end demo")


if __name__ == "__main__":
    main()
