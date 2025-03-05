from types import FunctionType
from typing import Any, Callable, Union

import dill
import pandas as pd
import pyarrow

from feast.field import Field, from_value_type
from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)
from feast.type_map import (
    python_type_to_feast_value_type,
)


class PandasTransformation:
    def __init__(self, udf: Union[FunctionType, Callable], udf_string: str = ""):
        """
        Creates an PandasTransformation object.

        Args:
            udf: The user defined transformation function, which must take pandas
                dataframes as inputs.
            udf_string: The source code version of the udf (for diffing and displaying in Web UI)
        """
        self.udf: Union[FunctionType, Callable] = udf
        self.udf_string = udf_string

    def transform_arrow(
        self, pa_table: pyarrow.Table, features: list[Field]
    ) -> pyarrow.Table:
        output_df_pandas = self.udf(pa_table.to_pandas())
        return pyarrow.Table.from_pandas(output_df_pandas)

    def transform(self, input_df: pd.DataFrame) -> pd.DataFrame:
        return self.udf(input_df)

    def infer_features(self, random_input: dict[str, list[Any]]) -> list[Field]:
        df = pd.DataFrame.from_dict(random_input)
        output_df: pd.DataFrame = self.transform(df)

        return [
            Field(
                name=f,
                dtype=from_value_type(
                    python_type_to_feast_value_type(f, type_name=str(dt))
                ),
            )
            for f, dt in zip(output_df.columns, output_df.dtypes)
        ]

    def __eq__(self, other):
        if not isinstance(other, PandasTransformation):
            raise TypeError(
                "Comparisons should only involve PandasTransformation class objects."
            )

        if (
            self.udf_string != other.udf_string
            or self.udf.__code__.co_code != other.udf.__code__.co_code
        ):
            return False

        return True

    def to_proto(self) -> UserDefinedFunctionProto:
        return UserDefinedFunctionProto(
            name=self.udf.__name__,
            body=dill.dumps(self.udf, recurse=True),
            body_text=self.udf_string,
        )

    @classmethod
    def from_proto(cls, user_defined_function_proto: UserDefinedFunctionProto):
        return PandasTransformation(
            udf=dill.loads(user_defined_function_proto.body),
            udf_string=user_defined_function_proto.body_text,
        )
