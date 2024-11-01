import logging
from typing import List, Set

import pandas as pd

from feast.feature_view import FeatureView
from feast.infra.passthrough_provider import PassthroughProvider
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView

logger = logging.getLogger(__name__)


class ExpediaProvider(PassthroughProvider):
    def __init__(self, config: RepoConfig):
        logger.info("Initializing Expedia provider...")

        if config.batch_engine.type != "spark.engine":
            logger.warning("Expedia provider recommends spark materialization engine")

        if config.offline_store.type != "spark":
            logger.warning(
                "Expedia provider recommends spark offline store as it only support SparkSource as Batch source"
            )

        super().__init__(config)

    def ingest_df(
        self,
        feature_view: FeatureView,
        df: pd.DataFrame,
    ):
        drop_list: List[str] = []
        fv_schema: Set[str] = set(map(lambda field: field.name, feature_view.schema))
        # Add timestamp field to the schema so we don't delete from dataframe
        if isinstance(feature_view, StreamFeatureView):
            fv_schema.add(feature_view.timestamp_field)
            if feature_view.source.created_timestamp_column:
                fv_schema.add(feature_view.source.created_timestamp_column)

        if isinstance(feature_view, FeatureView):
            if feature_view.stream_source is not None:
                fv_schema.add(feature_view.stream_source.timestamp_field)
                if feature_view.stream_source.created_timestamp_column:
                    fv_schema.add(feature_view.stream_source.created_timestamp_column)
            else:
                fv_schema.add(feature_view.batch_source.timestamp_field)
                if feature_view.batch_source.created_timestamp_column:
                    fv_schema.add(feature_view.batch_source.created_timestamp_column)

        for column in df.columns:
            if column not in fv_schema:
                drop_list.append(column)

        if len(drop_list) > 0:
            print(
                f"INFO!!! Dropping extra columns in the dataframe: {drop_list}. Avoid unnecessary columns in the dataframe."
            )

        super().ingest_df(feature_view, df.drop(drop_list, axis=1))
