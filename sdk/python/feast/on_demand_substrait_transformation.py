import pandas as pd
import pyarrow
import pyarrow.substrait as substrait  # type: ignore # noqa

from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandSubstraitTransformation as OnDemandSubstraitTransformationProto,
)


class OnDemandSubstraitTransformation:
    def __init__(self, substrait_plan: bytes):
        """
        Creates an OnDemandSubstraitTransformation object.

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

    def __eq__(self, other):
        if not isinstance(other, OnDemandSubstraitTransformation):
            raise TypeError(
                "Comparisons should only involve OnDemandSubstraitTransformation class objects."
            )

        if not super().__eq__(other):
            return False

        return self.substrait_plan == other.substrait_plan

    def to_proto(self) -> OnDemandSubstraitTransformationProto:
        return OnDemandSubstraitTransformationProto(substrait_plan=self.substrait_plan)

    @classmethod
    def from_proto(
        cls,
        on_demand_substrait_transformation_proto: OnDemandSubstraitTransformationProto,
    ):
        return OnDemandSubstraitTransformation(
            substrait_plan=on_demand_substrait_transformation_proto.substrait_plan
        )
