import pandas as pd
import pytest
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset

from feast.dqm.errors import ValidationFailed
from feast.dqm.profilers.ge_profiler import ge_profiler
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    location,
)

_features = [
    "customer_profile:current_balance",
    "customer_profile:avg_passenger_count",
    "customer_profile:lifetime_trip_count",
    "order:order_is_success",
    "global_stats:num_rides",
    "global_stats:avg_ride_length",
]


@ge_profiler
def configurable_profiler(dataset: PandasDataset) -> ExpectationSuite:
    from great_expectations.profile.user_configurable_profiler import (
        UserConfigurableProfiler,
    )

    return UserConfigurableProfiler(
        profile_dataset=dataset,
        excluded_expectations=[
            "expect_table_columns_to_match_ordered_list",
            "expect_table_row_count_to_be_between",
        ],
        value_set_threshold="few",
    ).build_suite()


@ge_profiler
def profiler_with_unrealistic_expectations(dataset: PandasDataset) -> ExpectationSuite:
    # need to create dataframe with corrupted data first
    df = pd.DataFrame()
    df["current_balance"] = [-100]
    df["avg_passenger_count"] = [0]

    other_ds = PandasDataset(df)
    other_ds.expect_column_max_to_be_between("current_balance", -1000, -100)
    other_ds.expect_column_values_to_be_in_set("avg_passenger_count", value_set={0})

    # this should pass
    other_ds.expect_column_min_to_be_between("avg_passenger_count", 0, 1000)

    return other_ds.get_expectation_suite()


@pytest.mark.integration
@pytest.mark.universal
def test_historical_retrieval_with_validation(environment, universal_data_sources):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    store.apply([driver(), customer(), location(), *feature_views.values()])

    entity_df = datasets["entity"].drop(
        columns=["order_id", "origin_id", "destination_id"]
    )

    reference_job = store.get_historical_features(
        entity_df=entity_df, features=_features,
    )

    store.create_saved_dataset(
        from_=reference_job,
        name="my_training_dataset",
        storage=environment.data_source_creator.create_saved_dataset_destination(),
    )

    job = store.get_historical_features(entity_df=entity_df, features=_features,)

    # if validation pass there will be no exceptions on this point
    job.to_df(
        validation_reference=store.get_saved_dataset(
            "my_training_dataset"
        ).as_reference(profiler=configurable_profiler)
    )


@pytest.mark.integration
@pytest.mark.universal
def test_historical_retrieval_fails_on_validation(environment, universal_data_sources):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    store.apply([driver(), customer(), location(), *feature_views.values()])

    entity_df = datasets["entity"].drop(
        columns=["order_id", "origin_id", "destination_id"]
    )

    reference_job = store.get_historical_features(
        entity_df=entity_df, features=_features,
    )

    store.create_saved_dataset(
        from_=reference_job,
        name="my_other_dataset",
        storage=environment.data_source_creator.create_saved_dataset_destination(),
    )

    job = store.get_historical_features(entity_df=entity_df, features=_features,)

    with pytest.raises(ValidationFailed) as exc_info:
        job.to_df(
            validation_reference=store.get_saved_dataset(
                "my_other_dataset"
            ).as_reference(profiler=profiler_with_unrealistic_expectations)
        )

    failed_expectations = exc_info.value.report.errors
    assert len(failed_expectations) == 2

    assert failed_expectations[0].check_name == "expect_column_max_to_be_between"
    assert failed_expectations[0].column_name == "current_balance"

    assert failed_expectations[1].check_name == "expect_column_values_to_be_in_set"
    assert failed_expectations[1].column_name == "avg_passenger_count"
