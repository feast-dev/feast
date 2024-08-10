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
import ast

class TracingDataFrame(pd.DataFrame):
    _accessed_columns = defaultdict(set)
    _dependencies = defaultdict(list)
    _current_output_col = None

    @property
    def _constructor(self):
        return TracingDataFrame._construct_from_dict

    @classmethod
    def _construct_from_dict(cls, *args, **kwargs):
        df = pd.DataFrame(*args, **kwargs)
        df.__class__ = TracingDataFrame
        return df

    def __getitem__(self, key):
        if isinstance(key, str) and TracingDataFrame._current_output_col is not None:
            TracingDataFrame._accessed_columns[TracingDataFrame._current_output_col].add(key)
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        if isinstance(key, str):
            if TracingDataFrame._current_output_col is not None:
                TracingDataFrame._dependencies[key].extend(TracingDataFrame._accessed_columns[TracingDataFrame._current_output_col])
            TracingDataFrame.reset_accessed_columns()  # Reset after each setitem to capture new context
        super().__setitem__(key, value)

    @classmethod
    def set_current_output_col(cls, col_name):
        cls._current_output_col = col_name

    @classmethod
    def reset_accessed_columns(cls):
        cls._accessed_columns = defaultdict(set)

    @classmethod
    def reset_dependencies(cls):
        cls._dependencies = defaultdict(list)

    @classmethod
    def get_dependencies(cls):
        return dict(cls._dependencies)

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

    def infer_feature_dependencies(self) -> Dict[str, Set[str]]:
        """
        Infers the dependencies of each column in the transformation.

        Returns:
            A dictionary where each key is an output column name and the value is a set
            of input column names that the output column depends on.
        """
        # Parse the UDF code
        tree = ast.parse(self.udf_string)
        #print(f"ast tree: {ast.dump(tree, indent=2)}")
        
        # Initialize dependency dictionary
        dependencies = {}
        
        # Helper function to extract column names used in an expression
        def get_column_names(node):
            if isinstance(node, ast.Name):
                return {node.id}
            elif isinstance(node, ast.BinOp):
                left = get_column_names(node.left)
                right = get_column_names(node.right)
                #print(f"Left: {left}, Right: {right}")
                return left.union(right)
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                return get_column_names(node.args[0])
            elif isinstance(node, ast.Subscript):
                #print(f"Id: {node.slice.value}, Dataframe: {node.value.id}")
                return {(("id", node.slice.value), ("df", node.value.id))}
            return set()
        
        # Helper function to process a statement in the AST
        def process_node(node):
            if isinstance(node, ast.Assign):
                #print("Found an assignment!")
                targets = [t.slice.value for t in node.targets if isinstance(t, ast.Subscript)]
                if len(targets) > 0:
                    output_column = targets[0]
                    dependencies[output_column] = get_column_names(node.value)
        
        # Find parameter name
        input = tree.body[0].args.args[0].arg
        #print(f"Input parameter: {input}")

        # Process each node in the AST
        for node in tree.body[0].body:
            process_node(node)
        
        #print(f"Dependencies before simplification: {dependencies}")
        
        # Recursive function to resolve full dependencies
        # TODO: Test this better
        # Process dependencies to create a simpler map
        def simplify_dependencies(deps):
            simple_deps = {}
            for key, value in deps.items():
                simple_deps[key] = set()
                for dep in value:
                    if dep[1][1] == input:
                        simple_deps[key].add(dep[0][1])
                    else:
                        if dep[0][1] in deps:
                            simple_deps[key].add(dep[0][1])
            return simple_deps

        # Simplified initial dependencies
        simplified_dependencies = simplify_dependencies(dependencies)

        # Helper function to recursively resolve all dependencies
        def resolve_dependencies(col, resolved):
            if col in resolved:
                return resolved[col]

            if col not in simplified_dependencies:
                return set()

            direct_deps = simplified_dependencies[col]
            all_deps = set(direct_deps)
            for dep in direct_deps:
                all_deps.update(resolve_dependencies(dep, resolved))

            resolved[col] = all_deps
            return all_deps

        # Final resolved dependencies
        final_dependencies = {}
        for col in simplified_dependencies:
            final_dependencies[col] = resolve_dependencies(col, {})

        #print(f"Dependencies after simplification: {final_dependencies}")

        return final_dependencies