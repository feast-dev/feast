from typing import Any, Dict, Optional, Union, cast

from feast.transformation.base import Transformation
from feast.transformation.mode import TransformationMode


class TrinoTransformation(Transformation):
    """
    TrinoTransformation defines a feature transformation executed as Trino SQL.

    Example:
    ```python
    trino_transformation = TrinoTransformation(
        mode=TransformationMode.TRINO_SQL,
        udf="SELECT driver_id, conv_rate * 2.0 AS conv_rate, acc_rate * 2.0 AS acc_rate FROM {}",
        udf_string="SELECT driver_id, conv_rate * 2.0 AS conv_rate, acc_rate * 2.0 AS acc_rate FROM {}",
    )
    ```
    or with a callable returning a SQL string:
    ```python
    def double_rates_sql(table_name: str) -> str:
        return f"SELECT driver_id, conv_rate * 2.0 AS conv_rate, acc_rate * 2.0 AS acc_rate FROM {table_name}"

    trino_transformation = TrinoTransformation(
        mode=TransformationMode.TRINO_SQL,
        udf=double_rates_sql,
        udf_string="double_rates_sql",
    )
    ```
    """

    def __new__(
        cls,
        mode: Union[TransformationMode, str] = TransformationMode.TRINO_SQL,
        udf: Any = None,
        udf_string: str = "",
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ) -> "TrinoTransformation":
        if isinstance(mode, str):
            mode = TransformationMode(mode)

        resolved_name = name or getattr(udf, "__name__", None) or "trino_transformation"

        instance = super(TrinoTransformation, cls).__new__(
            cls,
            mode=mode,
            udf=udf,
            udf_string=udf_string,
            name=resolved_name,
            tags=tags,
            description=description,
            owner=owner,
        )
        return cast(TrinoTransformation, instance)

    def __init__(
        self,
        mode: Union[TransformationMode, str] = TransformationMode.TRINO_SQL,
        udf: Any = None,
        udf_string: str = "",
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ):
        if isinstance(mode, str):
            mode = TransformationMode(mode)

        resolved_name = name or getattr(udf, "__name__", None) or "trino_transformation"

        super().__init__(
            mode=mode,
            udf=udf,
            name=resolved_name,
            udf_string=udf_string,
            tags=tags,
            description=description,
            owner=owner,
        )

    def transform(self, *inputs: str) -> str:
        """
        Applies the SQL transformation by injecting the input table/CTE names
        into the SQL query template or calling the UDF.
        """
        if callable(self.udf):
            return self.udf(*inputs)
        elif isinstance(self.udf, str):
            if inputs and "{}" in self.udf:
                return self.udf.format(*inputs)
            return self.udf
        elif self.udf_string:
            if inputs and "{}" in self.udf_string:
                return self.udf_string.format(*inputs)
            return self.udf_string
        raise ValueError(f"Invalid TrinoTransformation definition: {self.name}")

    def infer_features(self, *args, **kwargs) -> Any:
        pass
