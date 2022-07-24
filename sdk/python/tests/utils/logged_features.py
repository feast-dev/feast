import contextlib
import datetime
import tempfile
import uuid
from pathlib import Path
from typing import Iterator, Union

import numpy as np
import pandas as pd
import pyarrow

from feast import FeatureService, FeatureStore, FeatureView
from feast.errors import FeatureViewNotFoundException
from feast.feature_logging import LOG_DATE_FIELD, LOG_TIMESTAMP_FIELD, REQUEST_ID_FIELD
from feast.protos.feast.serving.ServingService_pb2 import FieldStatus


def prepare_logs(
    source_df: pd.DataFrame, feature_service: FeatureService, store: FeatureStore
) -> pd.DataFrame:
    num_rows = source_df.shape[0]

    logs_df = pd.DataFrame()
    logs_df[REQUEST_ID_FIELD] = [str(uuid.uuid4()) for _ in range(num_rows)]
    logs_df[LOG_TIMESTAMP_FIELD] = pd.Series(
        np.random.randint(0, 7 * 24 * 3600, num_rows)
    ).map(lambda secs: pd.Timestamp.utcnow() - datetime.timedelta(seconds=secs))
    logs_df[LOG_DATE_FIELD] = logs_df[LOG_TIMESTAMP_FIELD].dt.date

    for projection in feature_service.feature_view_projections:
        try:
            view = store.get_feature_view(projection.name)
        except FeatureViewNotFoundException:
            view = store.get_on_demand_feature_view(projection.name)
            for source in view.source_request_sources.values():
                for field in source.schema:
                    logs_df[field.name] = source_df[field.name]
        else:
            for entity_name in view.entities:
                entity = store.get_entity(entity_name)
                logs_df[entity.join_key] = source_df[entity.join_key]

        for feature in projection.features:
            source_field = (
                feature.name
                if feature.name in source_df.columns
                else f"{projection.name_to_use()}__{feature.name}"
            )
            destination_field = f"{projection.name_to_use()}__{feature.name}"
            logs_df[destination_field] = source_df[source_field]
            logs_df[f"{destination_field}__timestamp"] = source_df[
                "event_timestamp"
            ].dt.floor("s")
            if logs_df[f"{destination_field}__timestamp"].dt.tz:
                logs_df[f"{destination_field}__timestamp"] = logs_df[
                    f"{destination_field}__timestamp"
                ].dt.tz_convert(None)
            logs_df[f"{destination_field}__status"] = FieldStatus.PRESENT
            if isinstance(view, FeatureView) and view.ttl:
                logs_df[f"{destination_field}__status"] = logs_df[
                    f"{destination_field}__status"
                ].mask(
                    logs_df[f"{destination_field}__timestamp"]
                    < (datetime.datetime.utcnow() - view.ttl),
                    FieldStatus.OUTSIDE_MAX_AGE,
                )

    return logs_df


@contextlib.contextmanager
def to_logs_dataset(
    table: pyarrow.Table, pass_as_path: bool
) -> Iterator[Union[pyarrow.Table, Path]]:
    if not pass_as_path:
        yield table
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        pyarrow.parquet.write_to_dataset(table, root_path=temp_dir)
        yield Path(temp_dir)
