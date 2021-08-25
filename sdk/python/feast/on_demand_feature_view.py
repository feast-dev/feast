import functools
import inspect
from collections import defaultdict
from types import FunctionType, MethodType, ModuleType
from typing import Dict, List

from dill.detect import freevars, globalvars

from feast.feature import Feature
from feast.feature_view import FeatureView
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureView as OnDemandFeatureViewProto,
)
from feast.protos.feast.core.OnDemandFeatureView_pb2 import OnDemandFeatureViewSpec
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    UserDefinedFunction as UserDefinedFunctionProto,
)
from feast.usage import log_exceptions
from feast.value_type import ValueType


class OnDemandFeatureView:
    """
    An OnDemandFeatureView defines on demand transformations on existing feature view values and request data.

    Args:
        name: Name of the group of features.
        features: Output schema of transformation with feature names
        inputs: The input feature views passed into the transform.
        udf: User defined transformation function that takes as input pandas dataframes
    """

    name: str
    features: List[Feature]
    inputs: Dict[str, FeatureView]
    udf: MethodType

    @log_exceptions
    def __init__(
        self,
        name: str,
        features: List[Feature],
        inputs: Dict[str, FeatureView],
        udf: MethodType,
    ):
        """
        Creates an OnDemandFeatureView object.
        """

        self.name = name
        self.features = features
        self.inputs = inputs
        self.udf = udf

    def to_proto(self) -> OnDemandFeatureViewProto:
        """
        Converts an on demand feature view object to its protobuf representation.

        Returns:
            A OnDemandFeatureViewProto protobuf.
        """
        spec = OnDemandFeatureViewSpec(
            name=self.name,
            features=[feature.to_proto() for feature in self.features],
            inputs={k: fv.to_proto() for k, fv in self.inputs.items()},
            user_defined_function=UserDefinedFunctionProto(
                name=self.udf.__name__,
                return_type="double",  # TODO: fix
                body=_getsource(self.udf),
            ),
        )

        return OnDemandFeatureViewProto(spec=spec)

    @classmethod
    def from_proto(cls, on_demand_feature_view_proto: OnDemandFeatureViewProto):
        """
        Creates an on demand feature view from a protobuf representation.

        Args:
            on_demand_feature_view_proto: A protobuf representation of an on-demand feature view.

        Returns:
            A OnDemandFeatureView object based on the on-demand feature view protobuf.
        """
        on_demand_feature_view_obj = cls(
            name=on_demand_feature_view_proto.spec.name,
            features=[
                Feature(
                    name=feature.name,
                    dtype=ValueType(feature.value_type),
                    labels=dict(feature.labels),
                )
                for feature in on_demand_feature_view_proto.spec.features
            ],
            inputs={
                feature_view_name: FeatureView.from_proto(feature_view_proto)
                for feature_view_name, feature_view_proto in on_demand_feature_view_proto.spec.inputs.items()
            },
            udf=udf_from_proto(on_demand_feature_view_proto.spec.user_defined_function),
        )

        return on_demand_feature_view_obj


def on_demand_feature_view(features: List[Feature], inputs: Dict[str, FeatureView]):
    """
    Declare an on-demand feature view

    :param features: Output schema with feature names
    :param inputs: The inputs passed into the transform.
    :return: An On Demand Feature View.
    """

    def decorator(user_function):
        on_demand_feature_view_obj = OnDemandFeatureView(
            name=user_function.__name__,
            inputs=inputs,
            features=features,
            udf=user_function,
        )
        functools.update_wrapper(
            wrapper=on_demand_feature_view_obj, wrapped=user_function
        )
        return on_demand_feature_view_obj

    return decorator


def udf_from_proto(serialized_transform: UserDefinedFunctionProto, scope=None):
    """
    deserialize into global scope with pandas by default. if a scope if provided, deserialize into provided scope
    """

    if scope is None:
        scope = __import__("__main__").__dict__
        import pandas as pd

        scope["pd"] = pd

    exec(serialized_transform.body, scope)

    # Return function pointer
    try:
        fn = eval(serialized_transform.name, scope)
        fn._code = serialized_transform.body
        return fn
    except Exception as e:
        raise ValueError("Invalid transform") from e


def _getsource(func):
    imports = defaultdict(set)
    modules = set()
    code_lines = []
    seen_args = {}

    def process_functiontype(name, obj, imports, modules, code_lines, seen_args):
        # if this is user-defined or otherwise unavailable to import at deserialization time
        # including anything without a module, with module of __main__, defined as @inlined method,
        # or anything with a qualified name containing characters invalid for an import path, like foo.<locals>.bar
        objs = globalvars(obj, recurse=False)
        objs.update(freevars(obj))
        default_objs = {}
        for param in inspect.signature(obj).parameters.values():
            if param.default != inspect.Parameter.empty:
                default_objs[param.name] = param.default
                # these defaults shadow over obj
                objs.pop(param.name, None)
        # need to sort the keys since globalvars ordering is non-deterministic
        for dependency, dep_obj in sorted(objs.items()):
            recurse(
                dependency,
                dep_obj,
                imports,
                modules,
                code_lines,
                seen_args,
                write_codelines=True,
            )
        for dependency, dep_obj in sorted(default_objs.items()):
            # we dont re-write defaults at top-level since the `def` lines should have the declarations
            recurse(
                dependency,
                dep_obj,
                imports,
                modules,
                code_lines,
                seen_args,
                write_codelines=False,
            )
        fdef = inspect.getsource(obj)
        fdef = fdef[fdef.find("def ") :]
        code_lines.append(fdef)

    def recurse(name, obj, imports, modules, code_lines, seen_args, write_codelines):
        def _add_codeline(line):
            if write_codelines:
                code_lines.append(line)

        # prevent processing same dependency object multiple times, even if
        # multiple dependent objects exist in the tree from the original
        # func
        seen_key = str(name) + str(obj)
        if seen_args.get(seen_key) is True:
            return
        seen_args[seen_key] = True

        # Confusingly classes are subtypes of 'type'; non-classes are not
        if isinstance(obj, type):
            if obj.__module__ == "__main__":
                raise Exception(
                    f"Cannot serialize class {obj.__name__} from module __main__"
                )
            imports[obj.__module__].add(obj.__name__)

        elif isinstance(obj, FunctionType):
            process_functiontype(name, obj, imports, modules, code_lines, seen_args)
        elif isinstance(obj, ModuleType):
            if f"{obj.__package__}.{name}" == obj.__name__:
                imports[obj.__package__].add(name)
            else:
                modules.add(obj.__name__)
        else:
            try:
                repr_str = f"{name}={repr(obj)}"
                exec(repr_str)
                _add_codeline(repr_str)
            except Exception:
                raise Exception(
                    f"Cannot evaluate object {obj} of type '{type(obj)}' for serialization"
                )

    recurse(
        func.__name__,
        func,
        imports,
        modules,
        code_lines,
        seen_args,
        write_codelines=True,
    )

    for module in sorted(imports):
        import_line = f"from {module} import "
        import_line += ", ".join(sorted(imports[module]))
        code_lines.insert(0, import_line)

    for module in sorted(modules):
        code_lines.insert(0, f"import {module}")

    return "\n".join(code_lines)
