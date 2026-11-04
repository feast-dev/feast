from enum import Enum


class TransformationMode(Enum):
    PYTHON = "python"
    PANDAS = "pandas"
    SPARK_SQL = "spark_sql"
    SPARK = "spark"
    FLINK = "flink"
    RAY = "ray"
    SQL = "sql"
    TRINO_SQL = "trino_sql"
    SUBSTRAIT = "substrait"
