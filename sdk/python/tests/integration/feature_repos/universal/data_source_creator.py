from abc import ABC, abstractmethod
from datetime import timedelta

import pandas as pd
from feast import driver_test_data, FeatureView

from feast.data_source import DataSource
from feast.repo_config import FeastConfigBaseModel


class DataSourceCreator(ABC):
    @abstractmethod
    def create_data_sources(
        self,
        name: str,
        df: pd.DataFrame,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
    ) -> DataSource:
        ...

    @abstractmethod
    def create_offline_store_config(self) -> FeastConfigBaseModel:
        ...

    @abstractmethod
    def teardown(self, name: str):
        ...

    @abstractmethod
    def get_prefixed_table_name(self, name: str, suffix: str) -> str:
        ...

    @staticmethod
    def generate_entities(start_time, order_count: int = 1000):
        end_date = start_time
        before_start_date = end_date - timedelta(days=365)
        start_date = end_date - timedelta(days=7)
        after_end_date = end_date + timedelta(days=365)
        customer_entities = list(range(1001, 1110))
        driver_entities = list(range(5001, 5110))
        orders_df = driver_test_data.create_orders_df(
            customers=customer_entities,
            drivers=driver_entities,
            start_date=before_start_date,
            end_date=after_end_date,
            order_count=order_count,
        )
        return customer_entities, driver_entities, end_date, orders_df, start_date

