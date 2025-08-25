import logging

from feast.infra.passthrough_provider import PassthroughProvider
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
