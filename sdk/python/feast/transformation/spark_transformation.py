from typing import Any, Union, Dict, Optional, cast

import pandas as pd
import pyspark.sql

from feast.transformation.base import Transformation
from feast.transformation.mode import TransformationMode
from feast.infra.compute_engines.spark.utils import get_or_create_new_spark_session


class SparkTransformation(Transformation):

    def __new__(cls,
                mode: Union[TransformationMode, str],
                udf: Any,
                udf_string: str,
                spark_config: Dict[str, Any] = {},
                name: Optional[str] = None,
                tags: Optional[Dict[str, str]] = None,
                description: str = "",
                owner: str = "",
                *args,
                **kwargs) -> "SparkTransformation":
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

    def __init__(self,
                 mode: Union[TransformationMode, str],
                 udf: Any,
                 udf_string: str,
                 spark_config: Dict[str, Any] = {},
                 name: Optional[str] = None,
                 tags: Optional[Dict[str, str]] = None,
                 description: str = "",
                 owner: str = "",
                 *args,
                 **kwargs):
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

    def transform(self,
                  *inputs: Union[str, pd.DataFrame],
                  ) -> pd.DataFrame:
        if self.mode == TransformationMode.SPARK_SQL:
            return self._transform_spark_sql(*inputs)
        else:
            return self._transform_spark_udf(*inputs)

    @staticmethod
    def _create_temp_view_for_dataframe(df: pyspark.sql.DataFrame,
                                        name: str):
        df_temp_view = f"feast_transformation_temp_view_{name}"
        df.createOrReplaceTempView(df_temp_view)
        return df_temp_view

    def _transform_spark_sql(self,
                             *inputs: Union[pyspark.sql.DataFrame, str]
                             ) -> pd.DataFrame:
        inputs_str = [
            self._create_temp_view_for_dataframe(v, f"index_{i}")
            if isinstance(v, pyspark.sql.DataFrame) else v
            for i, v in enumerate(inputs)
        ]
        return self.spark_session.sql(self.udf(*inputs_str))

    def _transform_spark_udf(self,
                             *inputs: Any) -> pd.DataFrame:
        return self.udf(*inputs)

    def infer_features(self,
                       *args,
                       **kwargs) -> Any:
        pass
