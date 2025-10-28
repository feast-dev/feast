import functools
from abc import ABC
from typing import Any, Callable, Dict, Optional, Union

import dill

from feast.protos.feast.core.Transformation_pb2 import (
    SubstraitTransformationV2 as SubstraitTransformationProto,
)
from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)
from feast.transformation.factory import (
    TRANSFORMATION_CLASS_FOR_TYPE,
    get_transformation_class_from_type,
)
from feast.transformation.mode import TransformationMode


class Transformation(ABC):
    """
    Base Transformation class. Can be used to define transformations that can be applied to FeatureViews.
    Also encapsulates the logic to serialize and deserialize the transformation to and from proto. This is
    important for the future transformation lifecycle management.
    E.g.:
    pandas_transformation = Transformation(
        mode=TransformationMode.PANDAS,
        udf=lambda df: df.assign(new_column=df['column1'] + df['column2']),
    )
    """

    udf: Callable[[Any], Any]
    udf_string: str

    def __new__(
        cls,
        mode: Union[TransformationMode, str],
        udf: Callable[[Any], Any],
        udf_string: str,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ) -> "Transformation":
        """
        Creates a Transformation object.
        Args:
            mode: (required) The mode of the transformation. Choose one from TransformationMode.
            udf: (required) The user-defined transformation function.
            udf_string: (required) The string representation of the udf. The dill get source doesn't
            work for all cases when extracting the source code from the udf. So it's better to pass
            the source code as a string.
            name: (optional) The name of the transformation.
            tags: (optional) Metadata tags for the transformation.
            description: (optional) A description of the transformation.
            owner: (optional) The owner of the transformation.
        """
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
        udf_string: str,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
    ):
        self.mode = mode
        self.udf = udf
        self.udf_string = udf_string
        self.name = name or udf.__name__
        self.tags = tags or {}
        self.description = description
        self.owner = owner

    def to_proto(self) -> Union[UserDefinedFunctionProto, SubstraitTransformationProto]:
        mode_str = (
            self.mode.value if isinstance(self.mode, TransformationMode) else self.mode
        )
        return UserDefinedFunctionProto(
            name=self.udf.__name__,
            body=dill.dumps(self.udf, recurse=True),
            body_text=self.udf_string,
            mode=mode_str,
        )

    def __deepcopy__(self, memo: Optional[Dict[int, Any]] = None) -> "Transformation":
        return Transformation(mode=self.mode, udf=self.udf, udf_string=self.udf_string)

    def transform(self, *inputs: Any) -> Any:
        raise NotImplementedError

    def transform_arrow(self, *args, **kwargs) -> Any:
        pass

    def transform_singleton(self, *args, **kwargs) -> Any:
        pass

    def infer_features(self, *args, **kwargs) -> Any:
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
        transformation_obj = Transformation(
            mode=mode,
            name=name or user_function.__name__,
            tags=tags,
            description=description,
            owner=owner,
            udf=user_function,
            udf_string=udf_string,
        )
        functools.update_wrapper(wrapper=transformation_obj, wrapped=user_function)
        return transformation_obj

    return decorator
