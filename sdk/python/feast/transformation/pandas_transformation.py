from types import FunctionType
from typing import Any, Dict, List

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
    def __init__(self, udf: FunctionType, udf_string: str = ""):
        """
        Creates an PandasTransformation object.

        Args:
            udf: The user defined transformation function, which must take pandas
                dataframes as inputs.
            udf_string: The source code version of the udf (for diffing and displaying in Web UI)
        """
        self.udf = udf
        self.udf_string = udf_string

    def transform_arrow(
        self, pa_table: pyarrow.Table, features: List[Field]
    ) -> pyarrow.Table:
        if not isinstance(pa_table, pyarrow.Table):
            raise TypeError(
                f"pa_table should be type pyarrow.Table but got {type(pa_table).__name__}"
            )
        output_df = self.udf.__call__(pa_table.to_pandas())
        output_df = pyarrow.Table.from_pandas(output_df)
        if not isinstance(output_df, pyarrow.Table):
            raise TypeError(
                f"output_df should be type pyarrow.Table but got {type(output_df).__name__}"
            )
        return output_df

    def transform(self, input_df: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(input_df, pd.DataFrame):
            raise TypeError(
                f"input_df should be type pd.DataFrame but got {type(input_df).__name__}"
            )
        output_df = self.udf.__call__(input_df)
        if not isinstance(output_df, pd.DataFrame):
            raise TypeError(
                f"output_df should be type pd.DataFrame but got {type(output_df).__name__}"
            )
        return output_df

    def infer_features(self, random_input: Dict[str, List[Any]]) -> List[Field]:
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
