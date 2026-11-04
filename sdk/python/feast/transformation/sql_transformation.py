from typing import Any

from feast.transformation.base import Transformation


class SQLTransformation(Transformation):
    def transform(self, inputs: Any) -> str:
        return self.udf(inputs)
