from enum import Enum


class TransformationMode(Enum):
    PYTHON = "python"
    PANDAS = "pandas"
    SPARK_SQL = "spark_sql"
    SPARK = "spark"
    RAY = "ray"
    SQL = "sql"
    SUBSTRAIT = "substrait"
