from typing import Dict, Optional

from pydantic import StrictStr

from feast.repo_config import FeastConfigBaseModel


class SparkComputeConfig(FeastConfigBaseModel):
    type: StrictStr = "spark"
    """ Spark Compute type selector"""

    spark_conf: Optional[Dict[str, str]] = None
    """ Configuration overlay for the spark session """
    # sparksession is not serializable and we dont want to pass it around as an argument

    staging_location: Optional[StrictStr] = None
    """ Remote path for batch materialization jobs"""

    region: Optional[StrictStr] = None
    """ AWS Region if applicable for s3-based staging locations"""
