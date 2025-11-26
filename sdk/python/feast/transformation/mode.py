from enum import Enum


class TransformationMode(Enum):
    PYTHON = "python"
    PANDAS = "pandas"
    SPARK_SQL = "spark_sql"
    SPARK = "spark"
    RAY = "ray"
    SQL = "sql"
    SUBSTRAIT = "substrait"


class TransformationTiming(Enum):
    ON_READ = "on_read"        # Execute during get_online_features()
    ON_WRITE = "on_write"      # Execute during materialization, cache results
    BATCH = "batch"            # Scheduled batch processing
    STREAMING = "streaming"    # Real-time stream processing
