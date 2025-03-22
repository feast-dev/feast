import inspect
from typing import Any, Callable, Dict, Optional, cast, get_type_hints

import dill
import pandas as pd
import pyarrow
import pyarrow.substrait as substrait  # type: ignore # noqa

from feast.feature_view import FeatureView
from feast.field import Field, from_value_type
from feast.protos.feast.core.Transformation_pb2 import (
    SubstraitTransformationV2 as SubstraitTransformationProto,
)
from feast.transformation.base import Transformation
from feast.transformation.mode import TransformationMode
from feast.type_map import (
    feast_value_type_to_pandas_type,
    python_type_to_feast_value_type,
)


class SubstraitTransformation(Transformation):
    def __new__(
        cls,
        substrait_plan: bytes,
        udf: Callable[[Any], Any],
        name: Optional[str] = None,
        tags: Optional[dict[str, str]] = None,
        description: str = "",
        owner: str = "",
    ) -> "SubstraitTransformation":
        instance = super(SubstraitTransformation, cls).__new__(
            cls,
            mode=TransformationMode.SUBSTRAIT,
            udf=udf,
            name=name,
            udf_string="",
            tags=tags,
            description=description,
            owner=owner,
        )
        return cast(SubstraitTransformation, instance)

    def __init__(
        self,
        substrait_plan: bytes,
        udf: Callable[[Any], Any],
        name: Optional[str] = None,
        tags: Optional[dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ):
        """
        Creates an SubstraitTransformation object.

        Args:
            substrait_plan: The user-provided substrait plan.
            udf: The user-provided ibis function.
        """
        super().__init__(
            mode=TransformationMode.SUBSTRAIT,
            udf=udf,
            name=name,
            udf_string="",
            tags=tags,
            description=description,
            owner=owner,
        )
        self.substrait_plan = substrait_plan

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        def table_provider(names, schema: pyarrow.Schema):
            return pyarrow.Table.from_pandas(df[schema.names])

        table: pyarrow.Table = pyarrow.substrait.run_query(
            self.substrait_plan, table_provider=table_provider
        ).read_all()
        return table.to_pandas()

    def transform_singleton(self, input_df: pd.DataFrame) -> pd.DataFrame:
        raise ValueError(
            "SubstraitTransform does not support singleton transformations."
        )

    def transform_ibis(self, table):
        return self.udf(table)

    def transform_arrow(
        self, pa_table: pyarrow.Table, features: list[Field] = []
    ) -> pyarrow.Table:
        def table_provider(names, schema: pyarrow.Schema):
            return pa_table.select(schema.names)

        table: pyarrow.Table = pyarrow.substrait.run_query(
            self.substrait_plan, table_provider=table_provider
        ).read_all()

        if features:
            table = table.select([f.name for f in features])

        return table

    def infer_features(
        self, random_input: dict[str, list[Any]], singleton: Optional[bool]
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
            if feature_name not in random_input:
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
        if not isinstance(other, SubstraitTransformation):
            raise TypeError(
                "Comparisons should only involve SubstraitTransformation class objects."
            )

        return (
            self.substrait_plan == other.substrait_plan
            and self.udf.__code__.co_code == other.udf.__code__.co_code
        )

    def __deepcopy__(
        self, memo: Optional[Dict[int, Any]] = None
    ) -> "SubstraitTransformation":
        return SubstraitTransformation(substrait_plan=self.substrait_plan, udf=self.udf)

    def to_proto(self) -> SubstraitTransformationProto:
        return SubstraitTransformationProto(
            substrait_plan=self.substrait_plan,
            ibis_function=dill.dumps(self.udf, recurse=True),
        )

    @classmethod
    def from_proto(
        cls,
        substrait_transformation_proto: SubstraitTransformationProto,
    ):
        return SubstraitTransformation(
            substrait_plan=substrait_transformation_proto.substrait_plan,
            udf=dill.loads(substrait_transformation_proto.ibis_function),
        )

    @classmethod
    def from_ibis(cls, user_function, sources):
        from ibis.expr.types.relations import Table

        return_annotation = get_type_hints(user_function).get("return", inspect._empty)
        if return_annotation not in (inspect._empty, Table):
            raise TypeError(
                f"User function must return an ibis Table, got {return_annotation} for SubstraitTransformation"
            )

        import ibis
        import ibis.expr.datatypes as dt
        from ibis_substrait.compiler.core import SubstraitCompiler

        compiler = SubstraitCompiler()

        input_fields = []

        for s in sources:
            fields = s.projection.features if isinstance(s, FeatureView) else s.schema

            input_fields.extend(
                [
                    (
                        f.name,
                        dt.dtype(
                            feast_value_type_to_pandas_type(f.dtype.to_value_type())
                        ),
                    )
                    for f in fields
                ]
            )

        expr = user_function(ibis.table(input_fields, "t"))

        substrait_plan = compiler.compile(expr).SerializeToString()

        return SubstraitTransformation(
            substrait_plan=substrait_plan,
            udf=user_function,
        )
