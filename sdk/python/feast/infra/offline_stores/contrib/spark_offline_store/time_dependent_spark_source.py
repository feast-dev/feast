from __future__ import annotations, absolute_import
import logging
import os.path
import traceback
from typing import Dict, Optional, Any, List

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource, SparkOptions
from feast.errors import DataSourceNoNameException
from feast.infra.offline_stores.offline_utils import get_temp_entity_table_name
from feast.infra.offline_stores.contrib.spark_offline_store.defs import (
    MATERIALIZATION_START_DATE_KEY,
    MATERIALIZATION_END_DATE_KEY
)


logger = logging.getLogger(__name__)


def check_time_format(format_string):
    year_month_day_tokens = ['%Y', '%y', '%m', '%d']

    for token in year_month_day_tokens:
        format_string = format_string.replace(token, '')

    # Remove any other characters that do not specify date/time components
    for char in [' ', '-', ':', '/', '.']:
        format_string = format_string.replace(char, '')

    # True if date specifies something other than years, months, and days
    return len(format_string) > 0


class TimeDependentSparkSource(SparkSource):
    PATH_PREFIX_KEY = "path_prefix"
    TIME_FMT_STR_KEY = "time_fmt_str"
    PATH_SUFFIX_KEY = "path_suffix"

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        path_prefix: Optional[str] = None,
        time_fmt_str: Optional[str] = None,
        path_suffix: Optional[str] = "",
        file_format: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        timestamp_field: Optional[str] = None,
    ):
        # If no name, use the table as the default name.
        if name is None:
            raise DataSourceNoNameException()

        tags.update({
            self.PATH_PREFIX_KEY: path_prefix,
            self.TIME_FMT_STR_KEY: time_fmt_str,
            self.PATH_SUFFIX_KEY: path_suffix
        })

        self.path_prefix = path_prefix
        self.time_fmt_str = time_fmt_str
        self.path_suffix = path_suffix

        if check_time_format(self.time_fmt_str):
            raise ValueError(f"TimeDependentSparkSource: invalid time format string ({self.time_fmt_str}). "
                             f"Year-Month-Day formatting is only supported. If you need Hour, Minute, or "
                             f"higher frequency formatting, please reach out to ml-data.")

        super().__init__(
            name=name,
            path=path_prefix,
            file_format=file_format,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )


    @property
    def time_dependent_path(self) -> str:
        return os.path.join(self.path_prefix, self.time_fmt_str, self.path_suffix)

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_SPARK,
            data_source_class_type="feast.infra.offline_stores.contrib.spark_offline_store.spark_source.TimeDependentSparkSource",
            field_mapping=self.field_mapping,
            spark_options=self.spark_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        tags = dict(data_source.tags)
        path_prefix = str(tags[TimeDependentSparkSource.PATH_PREFIX_KEY])
        time_fmt_str = str(tags[TimeDependentSparkSource.TIME_FMT_STR_KEY])
        path_suffix = str(tags.get(TimeDependentSparkSource.PATH_SUFFIX_KEY, ""))

        assert data_source.HasField("spark_options")
        spark_options = SparkOptions.from_proto(data_source.spark_options)

        return TimeDependentSparkSource(
            name=data_source.name,
            path_prefix=path_prefix,
            time_fmt_str=time_fmt_str,
            path_suffix=path_suffix,
            file_format=spark_options.file_format,
            created_timestamp_column=data_source.created_timestamp_column,
            timestamp_field=data_source.timestamp_field,
            field_mapping=dict(data_source.field_mapping)
        )

    def get_paths_in_date_range(self, start_date: datetime, end_date: datetime) -> List[str]:
        current_date = start_date
        paths = []
        while current_date <= end_date:
            paths.append(current_date.strftime(self.time_dependent_path))
            current_date += timedelta(days=1)
        return paths

    def get_table_query_string(self, **kwargs) -> str:
        spark_session = SparkSession.getActiveSession()

        start_date: datetime = kwargs.get(MATERIALIZATION_START_DATE_KEY)
        end_date: datetime = kwargs.get(MATERIALIZATION_END_DATE_KEY)

        paths = []
        if start_date is None or end_date is None:
            logger.warning(f"TimeDependentSparkSource: one of `start_date` or `end_date` is not specified. If "
                           f"you are materializing, the prefix path will be used.")
            paths.append(self.path_prefix)
        else:
            paths = self.get_paths_in_date_range(start_date=start_date, end_date=end_date)

        if spark_session is None:
            raise AssertionError("Could not find an active spark session.")
        try:
            df = spark_session.read.format(self.file_format).load(*paths)
        except Exception:
            logger.exception(
                "Spark read of file source failed.\n" + traceback.format_exc()
            )
        tmp_table_name = get_temp_entity_table_name()
        df.createOrReplaceTempView(tmp_table_name)

        return f"`{tmp_table_name}`"
