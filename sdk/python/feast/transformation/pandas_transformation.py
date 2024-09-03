from types import FunctionType
from typing import Any, Dict, List, Set, Union, Tuple

import dill
import pandas as pd
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
    def __init__(self, input_df_name, output_df_name):
        self.input_df_name = input_df_name
        self.output_df_name = output_df_name
        self.current_column = None
        self.dependencies = defaultdict(set)
        self.temp_vars = {}

    def visit_Assign(self, node):
        # Handle assignment like df["output_col"] = ...
        if isinstance(node.targets[0], ast.Subscript):
            target = node.targets[0]
            if isinstance(target.value, ast.Name) and target.value.id == self.output_df_name:
                # We are assigning to an output DataFrame column
                # print(f"Col name: {target.slice.value}")
                col_name = target.slice.value
                self.current_column = col_name
                self.visit(node.value)
                self.current_column = None
            else:
                # Handle assignment to a temporary variable
                var_name = target.value.id
                self.temp_vars[var_name] = node.value
                self.visit(node.value)
        else:
            # Handle assignment to a variable (temporary or otherwise)
            var_name = node.targets[0].id
            self.temp_vars[var_name] = node.value
            self.visit(node.value)

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
        elif self.current_column and node.id == self.input_df_name:
            # Record access to input DataFrame columns
            pass

    def visit_Subscript(self, node):
        # Handle df["col_name"] access
        if isinstance(node.value, ast.Name) and node.value.id == self.input_df_name:
            # print(f"Col name: {node.slice.value}")
            col_name = node.slice.value
            if self.current_column:
                self.dependencies[self.current_column].add(col_name)
        else:
            # Handle subscript on a temporary variable
            self.visit(node.value)

    def visit_Call(self, node):
        # Handle function calls, check for df["col_name"].some_func()
        self.visit(node.func)
        for arg in node.args:
            self.visit(arg)

    def get_dependencies(self):
        return dict(self.dependencies)
    
class PandasTransformation:
    def __init__(self, udf: FunctionType, udf_string: str = ""):
        """
        Creates an PandasTransformation object.

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
        if not isinstance(pa_table, pyarrow.Table):
            raise TypeError(
                f"pa_table should be type pyarrow.Table but got {type(pa_table).__name__}"
            )
        output_df = self.udf.__call__(pa_table.to_pandas())
        output_df = pyarrow.Table.from_pandas(output_df)
        if not isinstance(output_df, pyarrow.Table):
            raise TypeError(
                f"output_df should be type pyarrow.Table but got {type(output_df).__name__}"
            )
        return output_df

    def transform(self, input_df: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(input_df, pd.DataFrame):
            raise TypeError(
                f"input_df should be type pd.DataFrame but got {type(input_df).__name__}"
            )
        output_df = self.udf.__call__(input_df)
        if not isinstance(output_df, pd.DataFrame):
            raise TypeError(
                f"output_df should be type pd.DataFrame but got {type(output_df).__name__}"
            )
        return output_df

    def infer_features(self, random_input: Dict[str, List[Any]]) -> List[Field]:
        df = pd.DataFrame.from_dict(random_input)

        output_df: pd.DataFrame = self.transform(df)

        return [
            Field(
                name=f,
                dtype=from_value_type(
                    python_type_to_feast_value_type(f, type_name=str(dt))
                ),
            )
            for f, dt in zip(output_df.columns, output_df.dtypes)
        ]

    def __eq__(self, other):
        if not isinstance(other, PandasTransformation):
            raise TypeError(
                "Comparisons should only involve PandasTransformation class objects."
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
        return PandasTransformation(
            udf=dill.loads(user_defined_function_proto.body),
            udf_string=user_defined_function_proto.body_text,
        )
    
    def infer_feature_dependencies(self) -> dict:
        column_usage = track_column_usage(self.udf)
        print(f"Column usage: {column_usage}")
        return column_usage
    
def track_column_usage(func: FunctionType) -> dict:
    source = inspect.getsource(func)
    source = textwrap.dedent(source)
    tree = ast.parse(source)

    # Extract the function's input DataFrame name from the signature
    signature = inspect.signature(func)
    input_df_name = list(signature.parameters.keys())[0]

    # Determine the output DataFrame name by finding the returned variable
    output_df_name = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Return):
            if isinstance(node.value, ast.Name):
                output_df_name = node.value.id
                break

    if not output_df_name:
        raise ValueError("Could not determine the output DataFrame name.")

    # Initialize the dependency tracker
    tracker = DependencyTracker(input_df_name, output_df_name)
    tracker.visit(tree)

    # Get and resolve dependencies
    dependencies = tracker.get_dependencies()
    resolved_dependencies = resolve_dependencies(dependencies)

    return resolved_dependencies

def resolve_dependencies(dependencies):
    def resolve(col):
        cols = dependencies[col]
        resolved_cols = set()
        for c in cols:
            if c in dependencies:
                resolved_cols.update(resolve(c))
            else:
                resolved_cols.add(c)
        return resolved_cols

    resolved = {col: resolve(col) for col in dependencies}
    return resolved