from typing import Dict, Type

from feast.transformation.base import Transformation
from feast.transformation.mode import TransformationMode
from feast.transformation.pandas_transformation import PandasTransformation
from feast.transformation.python_transformation import PythonTransformation
from feast.transformation.sql_transformation import SQLTransformation

TRANSFORMATION_CLASSES: Dict[str, Type[Transformation]] = {
    TransformationMode.PANDAS.value: PandasTransformation,
    TransformationMode.PYTHON.value: PythonTransformation,
    TransformationMode.SQL.value: SQLTransformation,
}
