from types import FunctionType
from typing import Any, Optional

import dill
import pyarrow

from feast.field import Field, from_value_type
from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)
from feast.type_map import (
    python_type_to_feast_value_type,
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

    def transform_arrow(
        self,
        pa_table: pyarrow.Table,
        features: list[Field],
    ) -> pyarrow.Table:
        return pyarrow.Table.from_pydict(self.udf(pa_table.to_pydict()))

    def transform(self, input_dict: dict) -> dict:
        # Ensuring that the inputs are included as well
        output_dict = self.udf.__call__(input_dict)
        return {**input_dict, **output_dict}

    def transform_singleton(self, input_dict: dict) -> dict:
        # This flattens the list of elements to extract the first one
        # in the case of a singleton element, it takes the value directly
        # in the case of a list of lists, it takes the first list
        input_dict = {k: v[0] for k, v in input_dict.items()}
        output_dict = self.udf.__call__(input_dict)
        return {**input_dict, **output_dict}

    def infer_features(
        self, random_input: dict[str, Any], singleton: Optional[bool] = False
    ) -> list[Field]:
        output_dict: dict[str, Any] = self.transform(random_input)

        fields = []
        for feature_name, feature_value in output_dict.items():
            if isinstance(feature_value, list):
                if len(feature_value) <= 0:
                    raise TypeError(
                        f"Failed to infer type for feature '{feature_name}' with value "
                        + f"'{feature_value}' since no items were returned by the UDF."
                    )
                inferred_value = feature_value[0]
                if singleton and isinstance(inferred_value, list):
                    # If we have a nested list like [[0.5, 0.5, ...]]
                    if len(inferred_value) > 0:
                        # Get the actual element type from the inner list
                        inferred_type = type(inferred_value[0])
                    else:
                        raise TypeError(
                            f"Failed to infer type for nested feature '{feature_name}' - inner list is empty"
                        )
                else:
                    # For non-nested lists or when singleton is False
                    inferred_type = type(inferred_value)

            else:
                inferred_type = type(feature_value)
                inferred_value = feature_value

            fields.append(
                Field(
                    name=feature_name,
                    dtype=from_value_type(
                        python_type_to_feast_value_type(
                            feature_name,
                            value=inferred_value,
                            type_name=inferred_type.__name__ if inferred_type else None,
                        )
                    ),
                )
            )
        return fields

    def __eq__(self, other):
        if not isinstance(other, PythonTransformation):
            raise TypeError(
                "Comparisons should only involve PythonTransformation class objects."
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
        return PythonTransformation(
            udf=dill.loads(user_defined_function_proto.body),
            udf_string=user_defined_function_proto.body_text,
        )
