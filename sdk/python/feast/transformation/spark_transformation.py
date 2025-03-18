from typing import Any

from transformation.base import Transformation


class SparkTransformation(Transformation):
    def transform(self,
                  inputs: Any) -> Any:
        pass

    def infer_features(self,
                       *args,
                       **kwargs) -> Any:
        pass
