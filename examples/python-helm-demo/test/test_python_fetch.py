from feast import FeatureStore
import requests
import json
import pandas as pd


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

    resp_data = json.loads(r.text)
    records = pd.DataFrame.from_records(
        columns=resp_data["metadata"]["feature_names"], 
        data=[[r["values"][i] for r in resp_data["results"]] for i in range(len(resp_data["results"]))]
    )
    for col in sorted(records.columns):
        print(col, " : ", records[col].values)


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
