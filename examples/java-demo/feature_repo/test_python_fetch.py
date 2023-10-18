from feast import FeatureStore


def run_demo():
    store = FeatureStore(repo_path=".")

    print("\n--- Online features ---")
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
            }
        ],
    ).to_dict()
    for key, value in sorted(features.items()):
        print(key, " : ", value)


if __name__ == "__main__":
    run_demo()
