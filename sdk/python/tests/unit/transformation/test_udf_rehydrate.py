import time

import dill
import pandas as pd

from feast.transformation.udf_rehydrate import (
    _strip_leading_decorators,
    rehydrate_udf_from_source,
    resolve_udf,
)


def _sample_udf(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["doubled"] = df["x"] * 2
    return out


def _rehydrate_fallback_udf(value):
    return value + _rehydrate_fallback_constant


_rehydrate_fallback_constant = 40


_SAMPLE_SRC = """
def _sample_udf(df):
    import pandas as pd
    out = pd.DataFrame()
    out["doubled"] = df["x"] * 2
    return out
"""


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


def test_resolve_udf_falls_back_to_dill_for_missing_module_global():
    source = """
def _rehydrate_fallback_udf(value):
    return value + _rehydrate_fallback_constant
"""
    body = dill.dumps(_rehydrate_fallback_udf, recurse=True)

    udf = resolve_udf(
        udf_string=source,
        body=body,
        preferred_name="_rehydrate_fallback_udf",
    )

    assert udf(2) == 42


def test_rehydrate_udf_allows_attribute_names():
    source = """
def copy_frame(frame):
    return frame.copy()
"""

    udf = rehydrate_udf_from_source(source, preferred_name="copy_frame")

    assert udf is not None
    assert udf(pd.DataFrame({"x": [1]})).equals(pd.DataFrame({"x": [1]}))


def test_rehydrate_udf_checks_globals_in_nested_code():
    source = """
def make_callback(value):
    return lambda: missing_module_global + value
"""

    assert rehydrate_udf_from_source(source, preferred_name="make_callback") is None


def test_rehydrate_strips_on_demand_decorator():
    src = """@on_demand_feature_view(
    sources=[feature_view_1],
    schema=[Field(name="metric_sum", dtype=Float64)],
)
def metric_sum_odfv(inputs):
    import pandas as pd
    df = pd.DataFrame()
    df["metric_sum"] = inputs["metric_a"] + inputs["metric_b"]
    return df
"""
    fn = rehydrate_udf_from_source(src, preferred_name="metric_sum_odfv")
    assert fn is not None
    result = fn(pd.DataFrame({"metric_a": [1.0], "metric_b": [2.0]}))
    assert result["metric_sum"].iloc[0] == 3.0


def test_strip_leading_decorators_multiline_and_bare():
    src = """@decorator
@foo(
    a=1,
    b="x(y)",
)
def bar():
    return 1
"""
    stripped = _strip_leading_decorators(src)
    assert stripped.lstrip().startswith("def bar")
    assert "@" not in stripped.split("def", 1)[0]


def test_strip_leading_decorators_adversarial_at_spam_is_linear():
    # CodeQL concern: nested regex on many '@' / newlines. Must stay O(n).
    spam = "@\n" * 20_000
    started = time.perf_counter()
    stripped = _strip_leading_decorators(spam)
    elapsed = time.perf_counter() - started
    assert elapsed < 1.0
    # No def — strip may consume all decorators; result should not hang and
    # rehydrate should fail closed so dill fallback can run.
    assert rehydrate_udf_from_source(spam) is None
    assert isinstance(stripped, str)
