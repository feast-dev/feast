import io
import json
import os
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pandas as pd

from feast.constants import ConfigOptions
from feast.contrib.validation.base import serialize_udf
from feast.staging.storage_client import get_staging_client

try:
    from great_expectations.core import ExpectationConfiguration, ExpectationSuite
    from great_expectations.dataset import PandasDataset
except ImportError:
    raise ImportError(
        "great_expectations must be installed to enable validation functionality. "
        "Please install feast[validation]"
    )

try:
    from pyspark.sql.types import BooleanType
except ImportError:
    raise ImportError(
        "pyspark must be installed to enable validation functionality. "
        "Please install feast[validation]"
    )


if TYPE_CHECKING:
    from feast import Client, FeatureTable


GE_PACKED_ARCHIVE = "https://storage.googleapis.com/feast-jobs/spark/validation/pylibs-ge-%(platform)s.tar.gz"
_UNSET = object()


class ValidationUDF:
    def __init__(self, name: str, pickled_code: bytes):
        self.name = name
        self.pickled_code = pickled_code


def drop_feature_table_prefix(
    expectation_configuration: ExpectationConfiguration, prefix
):
    kwargs = expectation_configuration.kwargs
    for arg_name in ("column", "column_A", "column_B"):
        if arg_name not in kwargs:
            continue

        if kwargs[arg_name].startswith(prefix):
            kwargs[arg_name] = kwargs[arg_name][len(prefix) :]


def prepare_expectations(suite: ExpectationSuite, feature_table: "FeatureTable"):
    for expectation in suite.expectations:
        drop_feature_table_prefix(expectation, f"{feature_table.name}__")

    return suite


def create_validation_udf(
    name: str, expectations: ExpectationSuite, feature_table: "FeatureTable",
) -> ValidationUDF:
    """
    Wraps your expectations into Spark UDF.

    Expectations should be generated & validated using training dataset:
    >>> from great_expectations.dataset import PandasDataset
    >>> ds = PandasDataset.from_dataset(you_training_df)
    >>> ds.expect_column_values_to_be_between('column', 0, 100)

    >>> expectations = ds.get_expectation_suite()

    Important: you expectations should pass on training dataset, only successful checks
    will be converted and stored in ExpectationSuite.

    Now you can create UDF that will validate data during ingestion:
    >>> create_validation_udf("myValidation", expectations)

    :param name
    :param expectations: collection of expectation gathered on training dataset
    :param feature_table
    :return: ValidationUDF with serialized code
    """

    expectations = prepare_expectations(expectations, feature_table)

    def udf(df: pd.DataFrame) -> pd.Series:
        from datadog.dogstatsd import DogStatsd

        reporter = (
            DogStatsd(
                host=os.environ["STATSD_HOST"],
                port=int(os.environ["STATSD_PORT"]),
                telemetry_min_flush_interval=0,
            )
            if os.getenv("STATSD_HOST") and os.getenv("STATSD_PORT")
            else DogStatsd()
        )

        ds = PandasDataset.from_dataset(df)
        result = ds.validate(expectations, result_format="COMPLETE")
        valid_rows = pd.Series([True] * df.shape[0])

        for check in result.results:
            if check.success:
                continue

            unexpected_count = (
                check.result["unexpected_count"]
                if "unexpected_count" in check.result
                else df.shape[0]
            )

            check_kwargs = check.expectation_config.kwargs
            check_kwargs.pop("result_format", None)
            check_name = "_".join(
                [check.expectation_config.expectation_type]
                + [
                    str(v)
                    for v in check_kwargs.values()
                    if isinstance(v, (str, int, float))
                ]
            )

            reporter.increment(
                "feast_feature_validation_check_failed",
                value=unexpected_count,
                tags=[
                    f"feature_table:{os.getenv('FEAST_INGESTION_FEATURE_TABLE', 'unknown')}",
                    f"project:{os.getenv('FEAST_INGESTION_PROJECT_NAME', 'default')}",
                    f"check:{check_name}",
                ],
            )

            if check.exception_info["raised_exception"]:
                # ToDo: probably we should mark all rows as invalid
                continue

            valid_rows.iloc[check.result["unexpected_index_list"]] = False

        return valid_rows

    pickled_code = serialize_udf(udf, BooleanType())
    return ValidationUDF(name, pickled_code)


def apply_validation(
    client: "Client",
    feature_table: "FeatureTable",
    udf: ValidationUDF,
    validation_window_secs: int,
    include_py_libs=_UNSET,
):
    """
    Uploads validation udf code to staging location &
    stores path to udf code and required python libraries as FeatureTable labels.
    """
    include_py_libs = (
        include_py_libs if include_py_libs is not _UNSET else GE_PACKED_ARCHIVE
    )

    staging_location = client._config.get(ConfigOptions.SPARK_STAGING_LOCATION).rstrip(
        "/"
    )
    staging_scheme = urlparse(staging_location).scheme
    staging_client = get_staging_client(staging_scheme, client._config)

    pickled_code_fp = io.BytesIO(udf.pickled_code)
    remote_path = f"{staging_location}/udfs/{feature_table.name}/{udf.name}.pickle"
    staging_client.upload_fileobj(
        pickled_code_fp, f"{udf.name}.pickle", remote_uri=urlparse(remote_path)
    )

    feature_table.labels.update(
        {
            "_validation": json.dumps(
                dict(
                    name=udf.name,
                    pickled_code_path=remote_path,
                    include_archive_path=include_py_libs,
                )
            ),
            "_streaming_trigger_secs": str(validation_window_secs),
        }
    )
    client.apply_feature_table(feature_table)
