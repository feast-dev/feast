import pandas as pd

from feast.transformation.pandas_transformation import PandasTransformation
from feast.transformation.python_transformation import PythonTransformation
from feast.transformation.udf_rehydrate import resolve_udf


def pandas_udf(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["output1"] = features_df["feature1"]
    df["output2"] = features_df["feature2"]
    return df


def test_init_pandas_transformation():
    transformation = PandasTransformation(udf=pandas_udf, udf_string="udf1")
    features_df = pd.DataFrame.from_dict({"feature1": [1, 2], "feature2": [2, 3]})
    transformed_df = transformation.transform(features_df)
    assert transformed_df["output1"].values[0] == 1
    assert transformed_df["output2"].values[1] == 3


_UDF_SOURCE = """def pandas_udf(features_df):
    df = __import__("pandas").DataFrame()
    df["output1"] = features_df["feature1"]
    df["output2"] = features_df["feature2"]
    return df
"""


def test_pandas_transformation_eq_uses_udf_string_not_bytecode():
    """Repo vs source-rehydrated callables must compare equal when source matches."""
    rehydrated = resolve_udf(udf_string=_UDF_SOURCE, preferred_name="pandas_udf")
    assert pandas_udf.__code__.co_code != rehydrated.__code__.co_code

    left = PandasTransformation(udf=pandas_udf, udf_string=_UDF_SOURCE)
    right = PandasTransformation(udf=rehydrated, udf_string=_UDF_SOURCE)
    assert left == right

    different = PandasTransformation(udf=rehydrated, udf_string=_UDF_SOURCE + "\n")
    assert left != different


def test_python_transformation_eq_uses_udf_string_not_bytecode():
    rehydrated = resolve_udf(udf_string=_UDF_SOURCE, preferred_name="pandas_udf")
    left = PythonTransformation(udf=pandas_udf, udf_string=_UDF_SOURCE)
    right = PythonTransformation(udf=rehydrated, udf_string=_UDF_SOURCE)
    assert left == right
    assert left != PythonTransformation(udf=rehydrated, udf_string="other")


def test_ray_transformation_eq_uses_udf_string_not_bytecode():
    from feast.transformation.ray_transformation import RayTransformation

    rehydrated = resolve_udf(udf_string=_UDF_SOURCE, preferred_name="pandas_udf")
    left = RayTransformation(udf=pandas_udf, udf_string=_UDF_SOURCE)
    right = RayTransformation(udf=rehydrated, udf_string=_UDF_SOURCE)
    assert left == right
    assert left != RayTransformation(udf=rehydrated, udf_string="other")
