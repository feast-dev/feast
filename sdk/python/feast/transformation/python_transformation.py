from types import FunctionType
from typing import Any, Dict, List

import dill
import pyarrow

from feast.field import Field, from_value_type
from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)
from feast.type_map import (
    python_type_to_feast_value_type,
)

from collections import defaultdict
import textwrap
import inspect
import ast

class DependencyTracker(ast.NodeVisitor):
    def __init__(self, input_dict_name, output_dict_name):
        self.input_dict_name = input_dict_name
        self.output_dict_name = output_dict_name
        self.current_key = None
        self.dependencies = defaultdict(set)
        self.temp_vars = {}

    def visit_Assign(self, node):
        target = node.targets[0]
        # Check if the target is a subscript (e.g., output["key"])
        if isinstance(target, ast.Subscript):
            if isinstance(target.value, ast.Name) and target.value.id == self.output_dict_name:
                # We are assigning to an output dictionary key
                key_name = target.slice.value
                self.current_key = key_name
                self.visit(node.value)
                self.current_key = None
            else:
                # Handle assignment to a temporary variable with a subscript
                var_name = target.value.id
                self.temp_vars[var_name] = node.value
                self.visit(node.value)
        elif isinstance(target, ast.Name):
            # Handle assignment to a simple variable
            var_name = target.id
            self.temp_vars[var_name] = node.value
            self.visit(node.value)
        else:
            # Other types of assignments (e.g., tuples, lists)
            self.generic_visit(node)

    def visit_Dict(self, node):
        # Handle dictionary initialization like output = {"key": ...}
        for key, value in zip(node.keys, node.values):
            if isinstance(key, ast.Constant):
                self.current_key = key.value
                self.visit(value)
                self.current_key = None

    def visit_BinOp(self, node):
        # Visit both sides of a binary operation
        self.visit(node.left)
        self.visit(node.right)

    def visit_Name(self, node):
        # Handle usage of variables
        if node.id in self.temp_vars:
            # If the variable is a temporary variable, expand its value
            temp_value = self.temp_vars[node.id]
            self.visit(temp_value)
        elif self.current_key and node.id == self.input_dict_name:
            # Record access to input dictionary keys
            pass

    def visit_Subscript(self, node):
        # Handle inputs["key"] access
        if isinstance(node.value, ast.Name) and node.value.id == self.input_dict_name:
            key_name = node.slice.value
            if self.current_key:
                self.dependencies[self.current_key].add(key_name)
        else:
            # Handle subscript on a temporary variable
            self.visit(node.value)

    def visit_Call(self, node):
        # Handle function calls, check for inputs["key"].some_func()
        self.visit(node.func)
        for arg in node.args:
            self.visit(arg)

    def get_dependencies(self):
        return dict(self.dependencies)

    def get_dependencies(self):
        return dict(self.dependencies)

class PythonTransformation:
    def __init__(self, udf: FunctionType, udf_string: str = ""):
        """
        Creates an PythonTransformation object.
        Args:
            udf: The user defined transformation function, which must take pandas
                dataframes as inputs.
            udf_string: The source code version of the udf (for diffing and displaying in Web UI)
        """
        self.udf = udf
        self.udf_string = udf_string

    def transform_arrow(
        self, pa_table: pyarrow.Table, features: List[Field]
    ) -> pyarrow.Table:
        raise Exception(
            'OnDemandFeatureView mode "python" not supported for offline processing.'
        )

    def transform(self, input_dict: Dict) -> Dict:
        if not isinstance(input_dict, Dict):
            raise TypeError(
                f"input_dict should be type Dict[str, Any] but got {type(input_dict).__name__}"
            )
        # Ensuring that the inputs are included as well
        output_dict = self.udf.__call__(input_dict)
        if not isinstance(output_dict, Dict):
            raise TypeError(
                f"output_dict should be type Dict[str, Any] but got {type(output_dict).__name__}"
            )
        return {**input_dict, **output_dict}

    def infer_features(self, random_input: Dict[str, List[Any]]) -> List[Field]:
        output_dict: Dict[str, List[Any]] = self.transform(random_input)

        return [
            Field(
                name=f,
                dtype=from_value_type(
                    python_type_to_feast_value_type(f, type_name=type(dt[0]).__name__)
                ),
            )
            for f, dt in output_dict.items()
        ]

    def __eq__(self, other):
        if not isinstance(other, PythonTransformation):
            raise TypeError(
                "Comparisons should only involve PythonTransformation class objects."
            )

        if (
            self.udf_string != other.udf_string
            or self.udf.__code__.co_code != other.udf.__code__.co_code
        ):
            return False

        return True

    def to_proto(self) -> UserDefinedFunctionProto:
        return UserDefinedFunctionProto(
            name=self.udf.__name__,
            body=dill.dumps(self.udf, recurse=True),
            body_text=self.udf_string,
        )

    @classmethod
    def from_proto(cls, user_defined_function_proto: UserDefinedFunctionProto):
        return PythonTransformation(
            udf=dill.loads(user_defined_function_proto.body),
            udf_string=user_defined_function_proto.body_text,
        )
    
    def infer_feature_dependencies(self) -> dict:
        try:
            key_usage = track_key_usage(self.udf)
            # print(f"Key usage: {key_usage}")
            return key_usage
        except Exception as e:
            print(f"Warning: An error occurred while tracking key usage: {str(e)}")
            return {}

def track_key_usage(func: FunctionType) -> dict:
    source = inspect.getsource(func)
    source = textwrap.dedent(source)
    tree = ast.parse(source)

    # Extract the function's input and output dictionary names
    signature = inspect.signature(func)
    input_dict_name = list(signature.parameters.keys())[0]

    # Determine the output dictionary name by finding the returned variable
    output_dict_name = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Return):
            if isinstance(node.value, ast.Name):
                output_dict_name = node.value.id
                break

    if not output_dict_name:
        raise ValueError("Could not determine the output dictionary name.")

    # Initialize the dependency tracker
    tracker = DependencyTracker(input_dict_name, output_dict_name)
    tracker.visit(tree)

    # Get and resolve dependencies
    dependencies = tracker.get_dependencies()
    resolved_dependencies = resolve_dependencies(dependencies)

    return resolved_dependencies

def resolve_dependencies(dependencies):
    def resolve(key):
        keys = dependencies[key]
        resolved_keys = set()
        for k in keys:
            if k in dependencies:
                resolved_keys.update(resolve(k))
            else:
                resolved_keys.add(k)
        return resolved_keys

    resolved = {key: resolve(key) for key in dependencies}
    return resolved