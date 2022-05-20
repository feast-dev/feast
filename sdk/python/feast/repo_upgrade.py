import logging
from pathlib import Path
from typing import Dict, List

from bowler import Query
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
