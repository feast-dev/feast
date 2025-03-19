import functools
from abc import ABC
from typing import Any, Callable, Dict, Optional, Union

import dill

from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)
from feast.transformation.factory import (
    TRANSFORMATION_CLASS_FOR_TYPE,
    get_transformation_class_from_type,
)
from feast.transformation.mode import TransformationMode


class Transformation(ABC):
    udf: Callable[[Any], Any]
    udf_string: str
    
    def __new__(
        cls,
        mode: Union[TransformationMode, str],
        udf: Callable[[Any], Any],
        udf_string: Optional[str] = "",
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        singleton: bool = False,
        *args,
        **kwargs,
    ) -> "Transformation":
        if cls is Transformation:
            if isinstance(mode, TransformationMode):
                mode = mode.value

            if mode.lower() in TRANSFORMATION_CLASS_FOR_TYPE:
                subclass = get_transformation_class_from_type(mode.lower())
                return super().__new__(subclass)

            raise ValueError(
                f"Invalid mode: {mode}. Choose one from TransformationMode."
            )

        return super().__new__(cls)

    def __init__(
        self,
        mode: Union[TransformationMode, str],
        udf: Callable[[Any], Any],
        udf_string: Optional[str] = "",
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        singleton: bool = False,
    ):
        self.mode = mode if isinstance(mode, str) else mode.value
        self.udf = udf
        if not udf_string:
            udf_string = dill.source.getsource(udf)
        self.udf_string = udf_string
        self.name = name
        self.tags = tags or {}
        self.description = description
        self.owner = owner
        self.singleton = singleton

    def to_proto(self) -> UserDefinedFunctionProto:
        return UserDefinedFunctionProto(
            name=self.udf.__name__,
            body=dill.dumps(self.udf, recurse=True),
            body_text=self.udf_string,
        )

    def transform(self, inputs: Any) -> Any:
        raise NotImplementedError

    def transform_arrow(self, *args, **kwargs) -> Any:
        pass

    def infer_features(self, *args, **kwargs) -> Any:
        raise NotImplementedError

    def transform_singleton(self, *args, **kwargs) -> Any:
        pass


def transformation(
    mode: Union[TransformationMode, str],
    name: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    description: Optional[str] = "",
    owner: Optional[str] = "",
    singleton: bool = False,
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
            singleton=singleton,
        )
        functools.update_wrapper(wrapper=batch_feature_view_obj, wrapped=user_function)
        return batch_feature_view_obj

    return decorator
