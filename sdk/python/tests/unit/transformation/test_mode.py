from feast.transformation.mode import TransformationMode


class TestTransformationMode:
    def test_all_modes_defined(self):
        expected = {"PYTHON", "PANDAS", "SPARK_SQL", "SPARK", "RAY", "SQL", "SUBSTRAIT"}
        actual = {m.name for m in TransformationMode}
        assert actual == expected

    def test_mode_values(self):
        assert TransformationMode.PYTHON.value == "python"
        assert TransformationMode.PANDAS.value == "pandas"
        assert TransformationMode.SPARK_SQL.value == "spark_sql"
        assert TransformationMode.SPARK.value == "spark"
        assert TransformationMode.RAY.value == "ray"
        assert TransformationMode.SQL.value == "sql"
        assert TransformationMode.SUBSTRAIT.value == "substrait"

    def test_mode_from_value(self):
        assert TransformationMode("python") == TransformationMode.PYTHON
        assert TransformationMode("pandas") == TransformationMode.PANDAS
