from types import FunctionType

import dill
import pandas as pd

from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
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
