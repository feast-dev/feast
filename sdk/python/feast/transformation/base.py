import functools
from abc import ABC
from typing import Any, Callable, Dict, List, Optional, Union

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
from feast.transformation.mode import TransformationMode, TransformationTiming
from feast.entity import Entity
from feast.field import Field

# Online compatibility constants
ONLINE_COMPATIBLE_MODES = {"python", "pandas"}
BATCH_ONLY_MODES = {"sql", "spark_sql", "spark", "ray", "substrait"}


def is_online_compatible(mode: str) -> bool:
    """
    Check if a transformation mode can run online in Feature Server.

    Args:
        mode: The transformation mode string

    Returns:
        True if the mode can run in Feature Server, False if batch-only
    """
    return mode.lower() in ONLINE_COMPATIBLE_MODES


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
    mode: Union[TransformationMode, str],  # Support both enum and string
    when: Optional[str] = None,
    online: Optional[bool] = None,
    sources: Optional[List[Union["FeatureView", "FeatureViewProjection", "RequestSource"]]] = None,
    schema: Optional[List[Field]] = None,
    entities: Optional[List[Entity]] = None,
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
        # Validate mode (handle both enum and string)
        if isinstance(mode, TransformationMode):
            mode_str = mode.value
        else:
            mode_str = mode.lower()  # Normalize to lowercase
            try:
                mode_enum = TransformationMode(mode_str)
            except ValueError:
                valid_modes = [m.value for m in TransformationMode]
                raise ValueError(f"Invalid mode '{mode}'. Valid options: {valid_modes}")

        # Validate timing if provided
        timing_enum = None
        if when is not None:
            try:
                timing_enum = TransformationTiming(when.lower())
            except ValueError:
                valid_timings = [t.value for t in TransformationTiming]
                raise ValueError(f"Invalid timing '{when}'. Valid options: {valid_timings}")

        # Validate online compatibility
        if online and not is_online_compatible(mode_str):
            compatible_modes = list(ONLINE_COMPATIBLE_MODES)
            raise ValueError(
                f"Mode '{mode_str}' cannot run online in Feature Server. "
                f"Use {compatible_modes} for online transformations."
            )

        # Create transformation object
        udf_string = dill.source.getsource(user_function)
        mainify(user_function)
        transformation_obj = Transformation(
            mode=mode_str,
            name=name or user_function.__name__,
            tags=tags,
            description=description,
            owner=owner,
            udf=user_function,
            udf_string=udf_string,
        )

        # If FeatureView parameters are provided, create and return FeatureView
        if any(param is not None for param in [when, online, sources, schema, entities]):
            # Import FeatureView here to avoid circular imports
            from feast.feature_view import FeatureView

            # Validate required parameters when creating FeatureView
            if when is None:
                raise ValueError("'when' parameter is required when creating FeatureView")
            if online is None:
                raise ValueError("'online' parameter is required when creating FeatureView")
            if sources is None:
                raise ValueError("'sources' parameter is required when creating FeatureView")
            if schema is None:
                raise ValueError("'schema' parameter is required when creating FeatureView")

            # Handle source parameter correctly for FeatureView constructor
            if not sources:
                raise ValueError("At least one source must be provided for FeatureView")
            elif len(sources) == 1:
                # Single source - pass directly (works for DataSource or FeatureView)
                source_param = sources[0]
            else:
                # Multiple sources - pass as list (must be List[FeatureView])
                from feast.feature_view import FeatureView as FV
                for src in sources:
                    if not isinstance(src, (FV, type(src).__name__ == 'FeatureView')):
                        raise ValueError("Multiple sources must be FeatureViews, not DataSources")
                source_param = sources

            # Create FeatureView with transformation
            fv = FeatureView(
                name=name or user_function.__name__,
                source=source_param,
                entities=entities or [],
                schema=schema,
                feature_transformation=transformation_obj,
                when=when,
                online_enabled=online,
                description=description,
                tags=tags,
                owner=owner,
                mode=mode_str,
            )
            functools.update_wrapper(wrapper=fv, wrapped=user_function)
            return fv
        else:
            # Backward compatibility: return Transformation object
            functools.update_wrapper(wrapper=transformation_obj, wrapped=user_function)
            return transformation_obj

    return decorator
