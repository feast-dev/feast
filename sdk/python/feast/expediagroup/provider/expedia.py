import logging
from datetime import datetime
from typing import Callable, Union

from tqdm import tqdm

from feast import FeatureView, OnDemandFeatureView
from feast.infra.passthrough_provider import PassthroughProvider
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import RepoConfig

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

    # TODO: this temporarily overrides the passthrough materialization to use the legacy materialization
    # we need to evaluate the new spark materialization and see if it will work better for us
    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        feature_view: Union[FeatureView, OnDemandFeatureView],
        start_date: datetime,
        end_date: datetime,
        registry: BaseRegistry,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
        disable_event_timestamp: bool = False,
        **kwargs,
    ) -> None:
        logger.info(
            f"Materializing feature view {feature_view.name} from offline store"
        )
        super().materialize_single_feature_view(
            config,
            feature_view,
            start_date,
            end_date,
            registry,
            project,
            tqdm_builder,
            disable_event_timestamp,
            from_offline_store=True,
        )
