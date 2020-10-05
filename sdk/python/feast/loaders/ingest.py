from typing import Dict, List

GRPC_CONNECTION_TIMEOUT_DEFAULT = 3  # type: int
GRPC_CONNECTION_TIMEOUT_APPLY = 300  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str
BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS = 300
BATCH_INGESTION_PRODUCTION_TIMEOUT = 120  # type: int


def check_field_mappings(
    column_names: List[str],
    feature_table_name: str,
    feature_table_field_mappings: Dict[str, str],
) -> None:
    """
        Checks that all specified field mappings in FeatureTable can be found in
        column names of specified ingestion source.

        Args:
            column_names: Column names in provided ingestion source
            feature_table_name: Name of FeatureTable
            feature_table_field_mappings: Field mappings of FeatureTable
    """

    if "datetime" not in column_names:
        raise ValueError(
            f'Provided data source does not contain entity "datetime" in columns {column_names}'
        )

    specified_field_mappings = [v for k, v in feature_table_field_mappings.items()]

    is_valid = all(col_name in column_names for col_name in specified_field_mappings)

    if not is_valid:
        raise Exception(
            f"Provided data source does not contain all field mappings previously "
            f"defined for FeatureTable, {feature_table_name}."
        )
