from feast.transformation.sql_transformation import SQLTransformation


def sql_udf(inputs):
    return f"SELECT * FROM source WHERE id = {inputs['id']}"


def test_sql_transformation_transform():
    """SQLTransformation.transform delegates to the udf."""
    transformation = SQLTransformation(
        mode="sql",
        udf=sql_udf,
        udf_string="sql_udf",
    )
    result = transformation.transform({"id": 42})
    assert result == "SELECT * FROM source WHERE id = 42"
