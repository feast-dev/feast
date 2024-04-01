from types import FunctionType
from typing import Dict

import dill

from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)


class PythonTransformation:
    def __init__(self, udf: FunctionType, udf_string: str = ""):
        """
        Creates an PythonTransformation object.
        Args:
            udf: The user defined transformation function, which must take pandas
                dataframes as inputs.
            udf_string: The source code version of the udf (for diffing and displaying in Web UI)
        """
        self.udf = udf
        self.udf_string = udf_string

    def transform(self, input_dict: Dict) -> Dict:
        if not isinstance(input_dict, Dict):
            raise TypeError(
                f"input_dict should be type Dict[str, Any] but got {type(input_dict).__name__}"
            )
        # Ensuring that the inputs are included as well
        output_dict = self.udf.__call__(input_dict)
        if not isinstance(output_dict, Dict):
            raise TypeError(
                f"output_dict should be type Dict[str, Any] but got {type(output_dict).__name__}"
            )
        return {**input_dict, **output_dict}

    def __eq__(self, other):
        if not isinstance(other, PythonTransformation):
            raise TypeError(
                "Comparisons should only involve PythonTransformation class objects."
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
        return PythonTransformation(
            udf=dill.loads(user_defined_function_proto.body),
            udf_string=user_defined_function_proto.body_text,
        )
