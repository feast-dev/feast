from types import FunctionType
from typing import Dict

import dill

from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunction as UserDefinedFunctionProto,
)


class OnDemandPythonTransformation:
    def __init__(self, udf: FunctionType, udf_string: str = ""):
        """
        Creates an OnDemandPythonTransformation object.

        Args:
            udf: The user defined transformation function, which must take pandas
                dataframes as inputs.
            udf_string: The source code version of the udf (for diffing and displaying in Web UI)
        """
        self.udf = udf
        self.udf_string = udf_string

    def transform(self, df: Dict) -> Dict:
        return self.udf.__call__(df)

    def __eq__(self, other):
        if not isinstance(other, OnDemandPythonTransformation):
            raise TypeError(
                "Comparisons should only involve OnDemandPythonTransformation class objects."
            )

        if not super().__eq__(other):
            return False

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
        return OnDemandPythonTransformation(
            udf=dill.loads(user_defined_function_proto.body),
            udf_string=user_defined_function_proto.body_text,
        )
