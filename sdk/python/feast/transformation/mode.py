from enum import Enum


class TransformationMode(Enum):
    PYTHON = "python"
    PANDAS = "pandas"
    SPARK_SQL = "spark_sql"
    SPARK = "spark"
    RAY = "ray"
    SQL = "sql"
    SUBSTRAIT = "substrait"


class TransformExecutionPattern(Enum):
    BATCH_ONLY = "batch_only"  # Pure batch: only in batch compute engine
    BATCH_ON_READ = "batch_on_read"  # Batch + feature server on read (lazy)
    BATCH_ON_WRITE = "batch_on_write"  # Batch + feature server on ingestion (eager)
