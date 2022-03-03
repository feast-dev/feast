import pandas as pd
from feast import FeatureStore
from feast.infra.offline_stores.file import SavedDatasetFileStorage

from feast.dqm.profilers.ge_profiler import ge_profiler

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.dataset import PandasDataset

DELTA = 0.1  # controlling allowed window in fraction of the value on scale [0, 1]
# Note: the GE integration allows asserting differences between datasets. The "ds" below is the reference dataset to check and this generates the expectation suite which can be used against future datasets.
# It's used via ge.validate(new_dataset, ExpectationSuite)
# For this demo though, we ignore this and


@ge_profiler
def credit_profiler(ds: PandasDataset) -> ExpectationSuite:
    # simple checks on data consistency
    ds.expect_column_values_to_be_between(
        "credit_card_due", min_value=0, mostly=0.99,  # allow some outliers
    )

    ds.expect_column_values_to_be_between(
        "missed_payments_1y",
        min_value=0,
        max_value=5,
        mostly=0.99,  # allow some outliers
    )

    return ds.get_expectation_suite()


def generate_saved_dataset():
    store = FeatureStore(repo_path=".")
    entity_df = pd.read_parquet(path="data/loan_table.parquet")

    fs = store.get_feature_service("credit_score_v1")
    job = store.get_historical_features(entity_df=entity_df, features=fs,)
    store.create_saved_dataset(
        from_=job,
        name="my_training_ds",
        storage=SavedDatasetFileStorage(path="my_training_ds.parquet"),
        feature_service=fs,
        profiler=credit_profiler,
    )


def get_latest_timestamps():
    store = FeatureStore(repo_path=".")
    feature_views = store.list_feature_views()
    for fv in feature_views:
        print(
            f"Data source latest event for {fv.name} is {fv.batch_source._meta.latest_event_timestamp}"
        )


def test_ge():
    store = FeatureStore(repo_path=".")

    print("--- Historical features (from saved dataset) ---")
    ds = store.get_saved_dataset("my_training_ds")
    print(ds._profile)


def run_demo():
    store = FeatureStore(repo_path=".")

    print("--- Historical features (from saved dataset) ---")
    ds = store.get_saved_dataset("my_training_ds")
    print(ds.to_df())

    print("\n--- Online features ---")
    features = store.get_online_features(
        features=store.get_feature_service("credit_score_v3"),
        entity_rows=[
            {"zipcode": 30721, "dob_ssn": "19530219_5179", "transaction_amt": 1023}
        ],
    ).to_dict()
    for key, value in sorted(features.items()):
        print(key, " : ", value)


if __name__ == "__main__":
    generate_saved_dataset()
    get_latest_timestamps()
    # test_ge()
    run_demo()
