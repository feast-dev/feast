from abc import ABC, abstractmethod
from datetime import timedelta


class DataFrameBackend(ABC):
    @abstractmethod
    def columns(self, df): ...

    @abstractmethod
    def from_arrow(self, table): ...

    @abstractmethod
    def join(self, left, right, on, how): ...

    @abstractmethod
    def groupby_agg(self, df, group_keys, agg_ops): ...

    @abstractmethod
    def filter(self, df, expr): ...

    @abstractmethod
    def to_arrow(self, df): ...

    @abstractmethod
    def to_timedelta_value(self, delta: timedelta): ...

    @abstractmethod
    def drop_duplicates(self, df, keys, sort_by, ascending: bool = False):
        pass
