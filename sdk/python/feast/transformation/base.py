import functools
from abc import ABC
from typing import Any, Callable, Dict, Optional, Union

import dill
from transformation.mode import TransformationMode
from transformation.pandas_transformation import PandasTransformation
from transformation.python_transformation import PythonTransformation
from transformation.sql_transformation import SQLTransformation


class Transformation(ABC):
    def __new__(
        cls,
        mode: Union[TransformationMode, str],
        udf: Callable[[Any], Any],
        name: Optional[str] = None,
        udf_string: str = "",
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ):
        if cls is Transformation and mode is not None:
            # Normalize mode to string
            if isinstance(mode, TransformationMode):
                mode = mode.value  # Convert enum to string

            transformation_classes = {
                TransformationMode.PANDAS.value: PandasTransformation,
                TransformationMode.PYTHON.value: PythonTransformation,
                TransformationMode.SQL.value: SQLTransformation,
            }

            if mode.lower() in transformation_classes:
                return super().__new__(transformation_classes[mode.lower()])
            else:
                raise ValueError(
                    f"Invalid mode: {mode}. Choose from 'pandas', 'python', or 'sql'."
                )

        return super().__new__(cls)

    def __init__(
        self,
        mode: Union[TransformationMode, str],
        udf: Callable[[Any], Any],
        name: Optional[str] = None,
        udf_string: str = "",
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
    ):
        self.mode = mode if isinstance(mode, str) else mode.value
        self.udf = udf
        self.name = name
        self.udf_string = udf_string
        self.tags = tags or {}
        self.description = description
        self.owner = owner

    def transform(self, inputs: Any) -> Any:
        raise NotImplementedError

    def transform_arrow(self, inputs: Any, *args, **kwargs) -> Any:
        raise NotImplementedError


def transformation(
    mode: Union[TransformationMode, str],
    name: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    description: Optional[str] = "",
    owner: Optional[str] = "",
):
    def mainify(obj):
        # Needed to allow dill to properly serialize the udf. Otherwise, clients will need to have a file with the same
        # name as the original file defining the sfv.
        if obj.__module__ != "__main__":
            obj.__module__ = "__main__"

    def decorator(user_function):
        udf_string = dill.source.getsource(user_function)
        mainify(user_function)
        batch_feature_view_obj = Transformation(
            mode=mode,
            name=name or user_function.__name__,
            tags=tags,
            description=description,
            owner=owner,
            udf=user_function,
            udf_string=udf_string,
        )
        functools.update_wrapper(wrapper=batch_feature_view_obj, wrapped=user_function)
        return batch_feature_view_obj

    return decorator
