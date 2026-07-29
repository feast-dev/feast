import dill
import pandas as pd

from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)
from feast.transformation.pandas_transformation import PandasTransformation
from feast.transformation.udf_rehydrate import rehydrate_udf_from_source, resolve_udf


def _sample_udf(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["doubled"] = df["x"] * 2
    return out


_SAMPLE_SRC = '''
def _sample_udf(df):
    import pandas as pd
    out = pd.DataFrame()
    out["doubled"] = df["x"] * 2
    return out
'''


def test_rehydrate_udf_from_source_prefers_named_function():
    fn = rehydrate_udf_from_source(_SAMPLE_SRC, preferred_name="_sample_udf")
    assert fn is not None
    result = fn(pd.DataFrame({"x": [1, 2]}))
    assert list(result["doubled"]) == [2, 4]


def test_resolve_udf_prefers_source_over_garbage_dill_body():
    # Garbage dill body would fail dill.loads; source must win.
    udf = resolve_udf(
        udf_string=_SAMPLE_SRC,
        body=b"not-valid-dill",
        preferred_name="_sample_udf",
    )
    result = udf(pd.DataFrame({"x": [3]}))
    assert result["doubled"].iloc[0] == 6


def test_resolve_udf_falls_back_to_dill_when_source_empty():
    body = dill.dumps(_sample_udf, recurse=True)
    udf = resolve_udf(udf_string="", body=body)
    result = udf(pd.DataFrame({"x": [4]}))
    assert result["doubled"].iloc[0] == 8


def test_rehydrate_strips_on_demand_decorator():
    src = '''@on_demand_feature_view(
    sources=[feature_view_1],
    schema=[Field(name="metric_sum", dtype=Float64)],
)
def metric_sum_odfv(inputs):
    import pandas as pd
    df = pd.DataFrame()
    df["metric_sum"] = inputs["metric_a"] + inputs["metric_b"]
    return df
'''
    fn = rehydrate_udf_from_source(src, preferred_name="metric_sum_odfv")
    assert fn is not None
    result = fn(pd.DataFrame({"metric_a": [1.0], "metric_b": [2.0]}))
    assert result["metric_sum"].iloc[0] == 3.0
