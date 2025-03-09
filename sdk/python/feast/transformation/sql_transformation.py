from typing import Any

from transformation.base import Transformation


class SQLTransformation(Transformation):

    def transform(self, inputs: Any) -> str:
        return self.udf(inputs)
