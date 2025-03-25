import pandas as pd

from feast.transformation.pandas_transformation import PandasTransformation


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
