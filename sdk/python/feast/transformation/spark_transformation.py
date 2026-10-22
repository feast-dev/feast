from typing import Any, Dict, Optional, Union, cast

import pandas as pd
import pyspark.sql

from feast.infra.compute_engines.spark.utils import get_or_create_new_spark_session
from feast.transformation.base import Transformation
from feast.transformation.mode import TransformationMode


class SparkTransformation(Transformation):
    r"""
    SparkTransformation can be used to define a transformation using a Spark UDF or SQL query.
    The current spark session will be used or a new one will be created if not available.
    E.g.:
    spark_transformation = SparkTransformation(
        mode=TransformationMode.SPARK,
        udf=remove_extra_spaces,
        udf_string="remove extra spaces",
    )
    OR
    spark_transformation = Transformation(
        mode=TransformationMode.SPARK_SQL,
        udf=remove_extra_spaces_sql,
        udf_string="remove extra spaces sql",
    )
    OR
    @transformation(mode=TransformationMode.SPARK)
    def remove_extra_spaces_udf(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(name=df['name'].str.replace('\s+', ' '))
    """

    def __new__(
        cls,
        mode: Union[TransformationMode, str],
        udf: Any,
        udf_string: str,
        spark_config: Dict[str, Any] = {},
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ) -> "SparkTransformation":
        """
        Creates a SparkTransformation
        Args:
            mode: (required) The mode of the transformation. Choose one from TransformationMode.SPARK or TransformationMode.SPARK_SQL.
            udf: (required) The user-defined transformation function.
            udf_string: (required) The string representation of the udf. The dill get source doesn't
            spark_config: (optional) The spark configuration to use for the transformation.
            name: (optional) The name of the transformation.
            tags: (optional) Metadata tags for the transformation.
            description: (optional) A description of the transformation.
            owner: (optional) The owner of the transformation.
        """
        instance = super(SparkTransformation, cls).__new__(
            cls,
            mode=mode,
            spark_config=spark_config,
            udf=udf,
            udf_string=udf_string,
            name=name,
            tags=tags,
            description=description,
            owner=owner,
        )
        return cast(SparkTransformation, instance)

    def __init__(
        self,
        mode: Union[TransformationMode, str],
        udf: Any,
        udf_string: str,
        spark_config: Dict[str, Any] = {},
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ):
        super().__init__(
            mode=mode,
            udf=udf,
            name=name,
            udf_string=udf_string,
            tags=tags,
            description=description,
            owner=owner,
        )
        self.spark_session = get_or_create_new_spark_session(spark_config)

    def transform(
        self,
        *inputs: Union[str, pd.DataFrame],
    ) -> pd.DataFrame:
        if self.mode == TransformationMode.SPARK_SQL:
            return self._transform_spark_sql(*inputs)
        else:
            return self._transform_spark_udf(*inputs)

    @staticmethod
    def _create_temp_view_for_dataframe(df: pyspark.sql.DataFrame, name: str):
        df_temp_view = f"feast_transformation_temp_view_{name}"
        df.createOrReplaceTempView(df_temp_view)
        return df_temp_view

    def _transform_spark_sql(
        self, *inputs: Union[pyspark.sql.DataFrame, str]
    ) -> pd.DataFrame:
        inputs_str = [
            self._create_temp_view_for_dataframe(v, f"index_{i}")
            if isinstance(v, pyspark.sql.DataFrame)
            else v
            for i, v in enumerate(inputs)
        ]
        return self.spark_session.sql(self.udf(*inputs_str))

    def _transform_spark_udf(self, *inputs: Any) -> pd.DataFrame:
        return self.udf(*inputs)

    def infer_features(self, *args, **kwargs) -> Any:
        pass
