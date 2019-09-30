from feast.types import FeatureRow_pb2 as FeatureRowProto
from feast.core import FeatureSet_pb2 as FeatureSetProto
import sqlite3
from typing import Dict, List
from feast.entity import Entity
from feast.value_type import ValueType
from feast.feature_set import FeatureSet, Feature

from feast.types import (
    FeatureRow_pb2 as FeatureRowProto,
    Field_pb2 as FieldProto,
    Value_pb2 as ValueProto,
)
from google.protobuf.timestamp_pb2 import Timestamp


class Database:
    pass


class SQLiteDatabase(Database):
    def __init__(self):
        self._conn = sqlite3.connect(":memory:")
        self._c = self._conn.cursor()

    def register_feature_set(self, feature_set: FeatureSetProto.FeatureSetSpec):
        query = build_sqlite_create_table_query(feature_set)
        print(query)
        self._c.execute(query)
        self._c.execute("SELECT name FROM sqlite_master WHERE type='table';")

        available_table = self._c.fetchall()
        print(available_table)

    def upsert_feature_row(
        self,
        feature_set: FeatureSetProto.FeatureSetSpec,
        feature_row: FeatureRowProto.FeatureRow,
    ):
        values = (feature_row.event_timestamp,)
        for entity in list(feature_set.entities):
            values = values + (get_feature_row_value_by_name(feature_row, entity.name),)
        values = values + (feature_row.SerializeToString(),)
        self._c.execute(build_sqlite_insert_feature_row_query(feature_set), values)


def build_sqlite_create_table_query(feature_set: FeatureSetProto.FeatureSetSpec):
    query = (
        """
        CREATE TABLE IF NOT EXISTS {} (
        {}
        PRIMARY KEY ({})
        );
        """
    ).format(
        get_table_name(feature_set),
        " ".join([column + " text NOT NULL," for column in get_columns(feature_set)]),
        ", ".join(
            get_columns(feature_set)[1:]
        ),  # exclude event_timestamp column for online stores
    )
    # Hyphens become three underscores
    query = query.replace("-", "___")
    return query


def build_sqlite_insert_feature_row_query(feature_set: FeatureSetProto.FeatureSetSpec):
    return """
              INSERT OR REPLACE INTO {} ({})
              VALUES(?,?,?,?,?,?)
            """.format(
        get_table_name(feature_set), ",".join(get_columns(feature_set))
    )


def get_columns(feature_set: FeatureSetProto.FeatureSetSpec) -> List[str]:
    return (
        ["event_timestamp"]
        + [field.name for field in list(feature_set.entities)]
        + ["value"]
    )


def get_feature_row_value_by_name(feature_row, name):
    values = [field.value for field in list(feature_row.fields) if field.name == name]
    if len(values) != 1:
        raise Exception(
            "Invalid number of features with name {} in feature row {}".format(
                name, feature_row.name
            )
        )
    return values[0]


def get_table_name(feature_set: FeatureSetProto.FeatureSetSpec) -> str:
    if not feature_set.name and not feature_set.version:
        raise ValueError("Feature set name or version is missing")
    return (feature_set.name + "_" + str(feature_set.version)).replace("-", "___")
