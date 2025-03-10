from enum import Enum


class TransformationMode(Enum):
    PYTHON = "python"
    PANDAS = "pandas"
    spark = "spark"
    SQL = "sql"
    SUBSTRAIT = "substrait"
