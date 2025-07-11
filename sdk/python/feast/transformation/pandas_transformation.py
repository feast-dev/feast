import inspect
from typing import Any, Callable, Optional, cast, get_type_hints

import dill
import pandas as pd
import pyarrow

from feast.field import Field, from_value_type
from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)
from feast.transformation.base import Transformation
from feast.transformation.mode import TransformationMode
from feast.type_map import (
    python_type_to_feast_value_type,
)


class PandasTransformation(Transformation):
    def __new__(
        cls,
        udf: Optional[Callable[[Any], Any]] = None,
        udf_string: Optional[str] = None,
        name: Optional[str] = None,
        tags: Optional[dict[str, str]] = None,
        description: str = "",
        owner: str = "",
    ) -> "PandasTransformation":
        # Handle Ray deserialization where parameters may not be provided
        if udf is None and udf_string is None:
            # Create a bare instance for deserialization
            instance = object.__new__(cls)
            return cast("PandasTransformation", instance)

        # Ensure required parameters are not None before calling parent constructor
        if udf is None:
            raise ValueError("udf parameter cannot be None")
        if udf_string is None:
            raise ValueError("udf_string parameter cannot be None")

        return cast(
            "PandasTransformation",
            super(PandasTransformation, cls).__new__(
                cls,
                mode=TransformationMode.PANDAS,
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
        udf: Optional[Callable[[Any], Any]] = None,
        udf_string: Optional[str] = None,
        name: Optional[str] = None,
        tags: Optional[dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ):
        # Handle Ray deserialization where parameters may not be provided
        if udf is None and udf_string is None:
            # Early return for deserialization - don't initialize
            return

        # Ensure required parameters are not None before calling parent constructor
        if udf is None:
            raise ValueError("udf parameter cannot be None")
        if udf_string is None:
            raise ValueError("udf_string parameter cannot be None")

        return_annotation = get_type_hints(udf).get("return", inspect._empty)
        if return_annotation not in (inspect._empty, pd.DataFrame):
            raise TypeError(
                f"return signature for PandasTransformation should be pd.DataFrame, instead got {return_annotation}"
            )

        super().__init__(
            mode=TransformationMode.PANDAS,
            udf=udf,
            name=name,
            udf_string=udf_string,
            tags=tags,
            description=description,
            owner=owner,
        )

    def transform_arrow(
        self, pa_table: pyarrow.Table, features: list[Field]
    ) -> pyarrow.Table:
        output_df_pandas = self.udf(pa_table.to_pandas())
        return pyarrow.Table.from_pandas(output_df_pandas)

    def transform(self, inputs: pd.DataFrame) -> pd.DataFrame:
        return self.udf(inputs)

    def infer_features(
        self,
        random_input: dict[str, list[Any]],
        *args,
        **kwargs,
    ) -> list[Field]:
        df = pd.DataFrame.from_dict(random_input)
        output_df: pd.DataFrame = self.transform(df)

        fields = []
        for feature_name, feature_type in zip(output_df.columns, output_df.dtypes):
            feature_value = output_df[feature_name].tolist()
            if len(feature_value) <= 0:
                raise TypeError(
                    f"Failed to infer type for feature '{feature_name}' with value "
                    + f"'{feature_value}' since no items were returned by the UDF."
                )
            fields.append(
                Field(
                    name=feature_name,
                    dtype=from_value_type(
                        python_type_to_feast_value_type(
                            feature_name,
                            value=feature_value[0],
                            type_name=str(feature_type),
                        )
                    ),
                )
            )
        return fields

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

    @classmethod
    def from_proto(cls, user_defined_function_proto: UserDefinedFunctionProto):
        return PandasTransformation(
            udf=dill.loads(user_defined_function_proto.body),
            udf_string=user_defined_function_proto.body_text,
        )
