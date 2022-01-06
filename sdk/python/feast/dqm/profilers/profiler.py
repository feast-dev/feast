import abc
from typing import Any, List, Optional

import pandas as pd


class Profile:
    @abc.abstractmethod
    def validate(self, dataset: pd.DataFrame) -> "ValidationReport":
        """
        Run set of rules / expectations from current profile against given dataset.

        Return ValidationReport
        """
        ...

    @abc.abstractmethod
    def to_proto(self):
        ...

    @classmethod
    @abc.abstractmethod
    def from_proto(cls, proto) -> "Profile":
        ...


class Profiler:
    @abc.abstractmethod
    def analyze_dataset(self, dataset: pd.DataFrame) -> Profile:
        """
        Generate Profile object with dataset's characteristics (with rules / expectations)
        from given dataset (as pandas dataframe).
        """
        ...

    @abc.abstractmethod
    def to_proto(self):
        ...

    @classmethod
    @abc.abstractmethod
    def from_proto(cls, proto) -> "Profiler":
        ...


class ValidationReport:
    @property
    @abc.abstractmethod
    def is_success(self) -> bool:
        """
        Return whether validation was successful
        """
        ...

    @property
    @abc.abstractmethod
    def errors(self) -> List["ValidationError"]:
        """
        Return list of ValidationErrors if validation failed (is_success = false)
        """
        ...


class ValidationError:
    check_name: str
    column_name: str

    check_config: Optional[Any]

    missing_count: Optional[int]
    missing_percent: Optional[float]

    def __init__(
        self,
        check_name: str,
        column_name: str,
        check_config: Optional[Any] = None,
        missing_count: Optional[int] = None,
        missing_percent: Optional[float] = None,
    ):
        self.check_name = check_name
        self.column_name = column_name
        self.check_config = check_config
        self.missing_count = missing_count
        self.missing_percent = missing_percent

    def __repr__(self):
        return f"<ValidationError {self.check_name}:{self.column_name}>"
