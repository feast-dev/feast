import base64
import json
import sys
import tempfile
import traceback
from pathlib import Path

import pyarrow.parquet as pq

from feast import FeatureStore
from feast.constants import FEATURE_STORE_YAML_ENV_NAME
from feast.infra.materialization.local_engine import DEFAULT_BATCH_SIZE
from feast.utils import _convert_arrow_to_proto, _run_pyarrow_field_mapping


def handler(event, context):
    """Provide an event that contains the following keys:

    - operation: one of the operations in the operations dict below
    - tableName: required for operations that interact with DynamoDB
    - payload: a parameter to pass to the operation being performed
    """
    print("Received event: " + json.dumps(event, indent=2), flush=True)

    try:
        config_base64 = event[FEATURE_STORE_YAML_ENV_NAME]

        config_bytes = base64.b64decode(config_base64)

        # Create a new unique directory for writing feature_store.yaml
        repo_path = Path(tempfile.mkdtemp())

        with open(repo_path / "feature_store.yaml", "wb") as f:
            f.write(config_bytes)

        # Initialize the feature store
        store = FeatureStore(repo_path=str(repo_path.resolve()))

        view_name = event["view_name"]
        view_type = event["view_type"]
        path = event["path"]

        bucket = path[len("s3://") :].split("/", 1)[0]
        key = path[len("s3://") :].split("/", 1)[1]
        print(f"Inferred Bucket: `{bucket}` Key: `{key}`", flush=True)

        if view_type == "batch":
            # TODO: This probably needs to be become `store.get_batch_feature_view` at some point.
            feature_view = store.get_feature_view(view_name)
        else:
            feature_view = store.get_stream_feature_view(view_name)

        print(f"Got Feature View: `{feature_view}`", flush=True)

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
        return {"written_rows": written_rows}
    except Exception as e:
        print(f"Exception: {e}", flush=True)
        print("Traceback:", flush=True)
        print(traceback.format_exc(), flush=True)
        sys.exit(1)
