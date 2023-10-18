import logging
from pathlib import Path
from typing import Dict, List

from bowler import Query
from fissix.fixer_util import touch_import
from fissix.pgen2 import token
from fissix.pygram import python_symbols
from fissix.pytree import Node

from feast.repo_operations import get_repo_files

SOURCES = {
    "FileSource",
    "BigQuerySource",
    "RedshiftSource",
    "SnowflakeSource",
    "KafkaSource",
    "KinesisSource",
}


class RepoUpgrader:
    def __init__(self, repo_path: str, write: bool):
        self.repo_path = repo_path
        self.write = write
        self.repo_files: List[str] = [
            str(p) for p in get_repo_files(Path(self.repo_path))
        ]
        logging.getLogger("RefactoringTool").setLevel(logging.WARNING)

    def upgrade(self):
        self.remove_date_partition_column()
        self.rename_features_to_schema()

    def rename_inputs_to_sources(self):
        def _change_argument_transform(node, capture, filename) -> None:
            children = node.children
            self.rename_arguments_in_children(children, {"inputs": "sources"})

        PATTERN = """
            decorator<
                any *
                "on_demand_feature_view"
                any *
            >
        """

        Query(self.repo_files).select(PATTERN).modify(
            _change_argument_transform
        ).execute(write=self.write, interactive=False)

    def rename_features_to_schema(self):
        Query(str(self.repo_path)).select_class("Feature").modify(
            self.import_remover("Feature")
        ).execute(interactive=False, write=self.write)

        def _rename_class_name(
            node: Node, capture: Dict[str, Node], filename: str
        ) -> None:
            self.rename_class_call(node, "Field")
            touch_import("feast", "Field", node)

        Query(self.repo_files).select_class("Feature").is_call().modify(
            _rename_class_name
        ).execute(write=self.write, interactive=False)

    def remove_date_partition_column(self):
        def _remove_date_partition_column(
            node: Node, capture: Dict[str, Node], filename: str
        ) -> None:
            self.remove_argument_transform(node, "date_partition_column")

        for s in SOURCES:
            Query(self.repo_files).select_class(s).is_call().modify(
                _remove_date_partition_column
            ).execute(write=self.write, interactive=False)

    @staticmethod
    def rename_arguments_in_children(
        children: List[Node], renames: Dict[str, str]
    ) -> None:
        """
        Renames the arguments in the children list of a node by searching for the
        argument list or trailing list and renaming all keys in `renames` dict to
        corresponding value.
        """
        for child in children:
            if not isinstance(child, Node):
                continue
            if (
                child.type == python_symbols.arglist
                or child.type == python_symbols.trailer
            ):
                if not child.children:
                    continue
                for _, child in enumerate(child.children):
                    if not isinstance(child, Node):
                        continue
                    else:
                        if child.type == python_symbols.argument:
                            if child.children[0].value in renames:
                                child.children[0].value = renames[
                                    child.children[0].value
                                ]

    @staticmethod
    def rename_class_call(node: Node, new_class_name: str):
        """
        Rename the class being instantiated.
        f = Feature(
            name="driver_id",
            join_key="driver_id",
        )
        into
        f = Field(
            name="driver_id",
        )
        This method assumes that node represents a class call that already has an arglist.
        """
        if len(node.children) < 2 or len(node.children[1].children) < 2:
            raise ValueError(f"Expected a class call with an arglist but got {node}.")
        node.children[0].value = new_class_name

    @staticmethod
    def remove_argument_transform(node: Node, argument: str):
        """
        Removes the specified argument.
        For example, if the argument is "join_key", this method transforms
        driver = Entity(
            name="driver_id",
            join_key="driver_id",
        )
        into
        driver = Entity(
            name="driver_id",
        )
        This method assumes that node represents a class call that already has an arglist.
        """
        if len(node.children) < 2 or len(node.children[1].children) < 2:
            raise ValueError(f"Expected a class call with an arglist but got {node}.")
        class_args = node.children[1].children[1].children
        for i, class_arg in enumerate(class_args):
            if (
                class_arg.type == python_symbols.argument
                and class_arg.children[0].value == argument
            ):
                class_args.pop(i)
                if i < len(class_args) and class_args[i].type == token.COMMA:
                    class_args.pop(i)
                if i < len(class_args) and class_args[i].type == token.NEWLINE:
                    class_args.pop(i)

    @staticmethod
    def import_remover(class_name):
        def remove_import_transformer(node, capture, filename):
            if "class_import" in capture and capture["class_name"].value == class_name:
                if capture["class_import"].type == python_symbols.import_from:
                    import_from_stmt = node.children
                    imported_classes = import_from_stmt[3]

                    if len(imported_classes.children) > 1:
                        # something of the form `from feast import A, ValueType`
                        for i, class_leaf in enumerate(imported_classes.children):
                            if class_leaf.value == class_name:
                                imported_classes.children.pop(i)
                                if i == len(imported_classes.children):
                                    imported_classes.children.pop(i - 1)
                                else:
                                    imported_classes.children.pop(i)
                    else:
                        # something of the form `from feast import ValueType`
                        node.parent.children.remove(node)

        return remove_import_transformer
