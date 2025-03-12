from enum import Enum


class TransformationMode(Enum):
    PYTHON = "python"
    PANDAS = "pandas"
    SPARK = "spark"
    SQL = "sql"
    SUBSTRAIT = "substrait"
