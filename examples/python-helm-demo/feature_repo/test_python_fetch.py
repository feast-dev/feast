from feast import FeatureStore
import requests
import json


def run_demo_http():
    print("\n--- Online features with HTTP endpoint ---")
    online_request = {
        "features": [
            "driver_hourly_stats:conv_rate",
        ],
        "entities": {"driver_id": [1001, 1002]},
    }
    r = requests.post(
        "http://localhost:6566/get-online-features", data=json.dumps(online_request)
    )
    print(json.dumps(r.json(), indent=4, sort_keys=True))


def run_demo_sdk():
    store = FeatureStore(repo_path=".")

    print("\n--- Online features with SDK ---")
    features = store.get_online_features(
        features=[
            "driver_hourly_stats:conv_rate",
        ],
        entity_rows=[
            {
                "driver_id": 1001,
            },
            {
                "driver_id": 1002,
            },
        ],
    ).to_dict()
    for key, value in sorted(features.items()):
        print(key, " : ", value)


if __name__ == "__main__":
    run_demo_sdk()
    run_demo_http()
