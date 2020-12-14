import io
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from feast.constants import ConfigOptions
from feast.contrib.validation.base import serialize_udf
from feast.staging.storage_client import get_staging_client

try:
    from great_expectations.core import ExpectationSuite
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
    import pandas as pd

    from feast import Client, FeatureTable


GE_PACKED_ARCHIVE = ""


@dataclass
class ValidationUDF:
    name: str
    pickled_code: bytes


def create_validation_udf(name: str, expectations: ExpectationSuite) -> ValidationUDF:
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
    :return: ValidationUDF with serialized code
    """

    def udf(df: pd.DataFrame) -> pd.Series:
        ds = PandasDataset.from_dataset(df)
        result = ds.validate(expectations, result_format="COMPLETE")
        valid_rows = pd.Series([True] * df.shape[0])

        for check in result.results:
            if check.success:
                continue

            if check.raised_exception:
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
):
    """
    Uploads validation udf code to staging location &
    stores path to udf code and required python libraries as FeatureTable labels.
    """
    staging_location = client._config.get(ConfigOptions.SPARK_STAGING_LOCATION).rstrip(
        "/"
    )
    staging_scheme = urlparse(staging_location).scheme
    staging_client = get_staging_client(staging_scheme)

    pickled_code_fp = io.BytesIO(udf.pickled_code)
    remote_path = f"{staging_location}/udfs/{udf.name}.pickle"
    staging_client.upload_fileobj(
        pickled_code_fp, f"{udf.name}.pickle", remote_uri=urlparse(remote_path)
    )

    feature_table.labels.update(
        {
            "_validation": json.dumps(
                dict(
                    name=udf.name,
                    pickled_code_path=remote_path,
                    include_archive_path=GE_PACKED_ARCHIVE,
                )
            ),
            "_streaming_trigger_secs": validation_window_secs,
        }
    )
    client.apply_feature_table(feature_table)
