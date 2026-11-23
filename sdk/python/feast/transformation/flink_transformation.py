from __future__ import annotations

from typing import Any, Callable, Optional, cast

from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)
from feast.transformation.base import Transformation, udf_transformation_from_proto
from feast.transformation.mode import TransformationMode


class FlinkTransformation(Transformation):
    """Transformation wrapper for Flink compute-engine UDFs.

    The UDF is expected to accept PyFlink Table objects and return a PyFlink
    Table.
    """

    def __new__(
        cls,
        udf: Optional[Callable[..., Any]] = None,
        udf_string: Optional[str] = None,
        name: Optional[str] = None,
        tags: Optional[dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ) -> "FlinkTransformation":
        if udf is None and udf_string is None:
            return cast("FlinkTransformation", object.__new__(cls))
        if udf is None:
            raise ValueError("udf parameter cannot be None")
        if udf_string is None:
            raise ValueError("udf_string parameter cannot be None")
        return cast(
            "FlinkTransformation",
            super(FlinkTransformation, cls).__new__(
                cls,
                mode=TransformationMode.FLINK,
                udf=udf,
                name=name,
                udf_string=udf_string,
                tags=tags,
                description=description,
                owner=owner,
            ),
        )

    def __init__(
        self,
        udf: Optional[Callable[..., Any]] = None,
        udf_string: Optional[str] = None,
        name: Optional[str] = None,
        tags: Optional[dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ) -> None:
        if udf is None and udf_string is None:
            return
        if udf is None:
            raise ValueError("udf parameter cannot be None")
        if udf_string is None:
            raise ValueError("udf_string parameter cannot be None")
        super().__init__(
            mode=TransformationMode.FLINK,
            udf=udf,
            name=name,
            udf_string=udf_string,
            tags=tags,
            description=description,
            owner=owner,
        )

    def transform(self, *inputs: Any) -> Any:
        return self.udf(*inputs)

    def infer_features(self, *args, **kwargs) -> Any:
        pass

    @classmethod
    def from_proto(
        cls, user_defined_function_proto: UserDefinedFunctionProto
    ) -> Transformation:
        return udf_transformation_from_proto(user_defined_function_proto)
