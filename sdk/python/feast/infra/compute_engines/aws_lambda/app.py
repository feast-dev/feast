import base64
import logging
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

from feast import FeatureStore
from feast.constants import FEATURE_STORE_YAML_ENV_NAME
from feast.infra.compute_engines.aws_lambda.lambda_engine import DEFAULT_BATCH_SIZE
from feast.utils import _convert_arrow_to_proto, _run_pyarrow_field_mapping

logger = logging.getLogger()
logger.setLevel("INFO")


def handler(event, context):
    """Load a parquet file and write the feature values to the online store.

    Args:
        event (dict): payload containing the following keys:
            FEATURE_STORE_YAML_ENV_NAME: Base64 encoded feature store config
            view_name: Name of FeatureView to be materialized
            view_type: Type of FeatureView
            path: Path to parquet batch file on S3 bucket
        context (dict): Lambda runtime context, not used.
    """
    logger.info(f"Received event: {event}")

    try:
        config_base64 = event[FEATURE_STORE_YAML_ENV_NAME]

        config_bytes = base64.b64decode(config_base64)

        # Create a new unique directory for writing feature_store.yaml
        with tempfile.TemporaryDirectory() as repo_posix_path:
            repo_path = Path(repo_posix_path)

            with open(repo_path / "feature_store.yaml", "wb") as f:
                f.write(config_bytes)

            # Initialize the feature store
            store = FeatureStore(repo_path=str(repo_path.resolve()))

            view_name = event["view_name"]
            view_type = event["view_type"]
            path = event["path"]

            bucket, key = path[len("s3://") :].split("/", 1)
            logger.info(f"Inferred Bucket: `{bucket}` Key: `{key}`")

            if view_type == "batch":
                # TODO: This probably needs to be become `store.get_batch_feature_view` at some point. # noqa: E501,W505
                feature_view = store.get_feature_view(view_name)
            else:
                feature_view = store.get_stream_feature_view(view_name)

            logger.info(
                f"Got Feature View: `{feature_view.name}`, \
                last updated: {feature_view.last_updated_timestamp}"
            )

            table = pq.read_table(path)
            if feature_view.batch_source.field_mapping is not None:
                table = _run_pyarrow_field_mapping(
                    table, feature_view.batch_source.field_mapping
                )

            join_key_to_value_type = {
                entity.name: entity.dtype.to_value_type()
                for entity in feature_view.entity_columns
            }

            written_rows = 0

            for batch in table.to_batches(DEFAULT_BATCH_SIZE):
                rows_to_write = _convert_arrow_to_proto(
                    batch, feature_view, join_key_to_value_type
                )
                store._provider.online_write_batch(
                    store.config,
                    feature_view,
                    rows_to_write,
                    lambda x: None,
                )
                written_rows += len(rows_to_write)
            logger.info(
                f"Successfully updated {written_rows} rows.",
                extra={"num_updated_rows": written_rows, "feature_view": view_name},
            )
    except Exception:
        logger.exception("Error in processing materialization.")
        raise
