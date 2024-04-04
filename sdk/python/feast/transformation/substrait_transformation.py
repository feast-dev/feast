from typing import Any, Dict, List

import pandas as pd
import pyarrow
import pyarrow.substrait as substrait  # type: ignore # noqa

from feast.feature_view import FeatureView
from feast.field import Field, from_value_type
from feast.protos.feast.core.Transformation_pb2 import (
    SubstraitTransformationV2 as SubstraitTransformationProto,
)
from feast.type_map import (
    feast_value_type_to_pandas_type,
    python_type_to_feast_value_type,
)


class SubstraitTransformation:
    def __init__(self, substrait_plan: bytes):
        """
        Creates an SubstraitTransformation object.

        Args:
            substrait_plan: The user-provided substrait plan.
        """
        self.substrait_plan = substrait_plan

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        def table_provider(names, schema: pyarrow.Schema):
            return pyarrow.Table.from_pandas(df[schema.names])

        table: pyarrow.Table = pyarrow.substrait.run_query(
            self.substrait_plan, table_provider=table_provider
        ).read_all()
        return table.to_pandas()

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
        if not isinstance(other, SubstraitTransformation):
            raise TypeError(
                "Comparisons should only involve SubstraitTransformation class objects."
            )

        if not super().__eq__(other):
            return False

        return self.substrait_plan == other.substrait_plan

    def to_proto(self) -> SubstraitTransformationProto:
        return SubstraitTransformationProto(substrait_plan=self.substrait_plan)

    @classmethod
    def from_proto(
        cls,
        substrait_transformation_proto: SubstraitTransformationProto,
    ):
        return SubstraitTransformation(
            substrait_plan=substrait_transformation_proto.substrait_plan
        )

    @classmethod
    def from_ibis(cls, user_function, sources):
        import ibis
        import ibis.expr.datatypes as dt
        from ibis_substrait.compiler.core import SubstraitCompiler

        compiler = SubstraitCompiler()

        input_fields = []

        for s in sources:
            fields = s.projection.features if isinstance(s, FeatureView) else s.features

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

        return SubstraitTransformation(
            substrait_plan=compiler.compile(expr).SerializeToString()
        )
