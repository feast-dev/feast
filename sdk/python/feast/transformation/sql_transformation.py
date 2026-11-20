from typing import Any

from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)
from feast.transformation.base import Transformation, udf_transformation_from_proto


class SQLTransformation(Transformation):
    def transform(self, inputs: Any) -> str:
        return self.udf(inputs)

    @classmethod
    def from_proto(
        cls, user_defined_function_proto: UserDefinedFunctionProto
    ) -> Transformation:
        return udf_transformation_from_proto(user_defined_function_proto)
