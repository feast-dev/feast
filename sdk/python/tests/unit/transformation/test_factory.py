from unittest.mock import MagicMock, patch

import pytest

from feast.transformation.factory import (
    TRANSFORMATION_CLASS_FOR_TYPE,
    get_transformation_class_from_type,
)


class TestTransformationClassForType:
    def test_all_expected_types_registered(self):
        expected_types = {
            "python",
            "pandas",
            "substrait",
            "sql",
            "spark_sql",
            "spark",
            "ray",
        }
        assert set(TRANSFORMATION_CLASS_FOR_TYPE.keys()) == expected_types

    def test_spark_and_spark_sql_resolve_to_same_class(self):
        assert (
            TRANSFORMATION_CLASS_FOR_TYPE["spark"]
            == TRANSFORMATION_CLASS_FOR_TYPE["spark_sql"]
        )


class TestGetTransformationClassFromType:
    @patch("feast.transformation.factory.import_class")
    def test_known_type_resolves(self, mock_import):
        mock_cls = MagicMock()
        mock_import.return_value = mock_cls

        result = get_transformation_class_from_type("python")

        mock_import.assert_called_once_with(
            "feast.transformation.python_transformation",
            "PythonTransformation",
            "PythonTransformation",
        )
        assert result == mock_cls

    @patch("feast.transformation.factory.import_class")
    def test_pandas_type_resolves(self, mock_import):
        mock_cls = MagicMock()
        mock_import.return_value = mock_cls

        get_transformation_class_from_type("pandas")

        mock_import.assert_called_once_with(
            "feast.transformation.pandas_transformation",
            "PandasTransformation",
            "PandasTransformation",
        )

    @patch("feast.transformation.factory.import_class")
    def test_sql_type_resolves(self, mock_import):
        mock_cls = MagicMock()
        mock_import.return_value = mock_cls

        get_transformation_class_from_type("sql")

        mock_import.assert_called_once_with(
            "feast.transformation.sql_transformation",
            "SQLTransformation",
            "SQLTransformation",
        )

    def test_invalid_type_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid transformation type"):
            get_transformation_class_from_type("nonexistent")

    @patch("feast.transformation.factory.import_class")
    def test_fully_qualified_class_name_accepted(self, mock_import):
        """A string ending in 'Transformation' is treated as a fully qualified class path."""
        mock_cls = MagicMock()
        mock_import.return_value = mock_cls

        get_transformation_class_from_type("my.custom.module.CustomTransformation")

        mock_import.assert_called_once_with(
            "my.custom.module",
            "CustomTransformation",
            "CustomTransformation",
        )
