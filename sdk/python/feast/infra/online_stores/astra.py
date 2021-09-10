from abc import ABC
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Sequence

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast import Entity, FeatureTable, FeatureView
from feast.infra.utils.online_store_utils import _table_id, _to_naive_utc
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.infra.online_stores.online_store import OnlineStore

from pydantic import StrictStr
from pydantic.typing import Literal


class AstraConfig(FeastConfigBaseModel):
    """Online store config for Astra online store"""

    type: Literal["astra"] = "astra"
    """Online store type selector"""

    secure_connect_bundle: StrictStr
    client_id: StrictStr
    secret_key: StrictStr
    keyspace: StrictStr


class AstraDBOnlineStore(OnlineStore, ABC):
    """
    Online feature store for Astra Cassandra Database
    """

    def __init__(self, config: RepoConfig):
        self.config = config
        self.online_config = config.online_store
        """Verify if the online store is the instance of AstraConfig Class"""
        assert isinstance(self.online_config, AstraConfig)
        self.session = self._initialize_astra_session(self.online_config)

    @staticmethod
    def _initialize_astra_session(config: AstraConfig):
        cloud_config = {
                'secure_connect_bundle': config.secure_connect_bundle
        }
        auth_provider = PlainTextAuthProvider(config.client_id, config.secret_key)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        return session

    def online_write_batch(
        self,
            config: RepoConfig,
            table: Union[FeatureTable, FeatureView],
            data: List[
                Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
            ],
            progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        A function that will take features and store in specified data
        :param config:
        :param table:
        :param data:
        :param progress:
        :return:
        """
        project = config.project
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(entity_key)
            timestamp = _to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = _to_naive_utc(created_ts)
            for feature_name, val in values.items():
                table_name = _table_id(project, table)
                keyspace = self.online_config.keyspace

                # Now create Insert command
                insert_cql = _create_cql_insert_record(keyspace,
                                                       table_name,
                                                       column_names=["entity_key",
                                                                     "feature_name",
                                                                     "value",
                                                                     "event_ts",
                                                                     "created_ts"],
                                                       values=[entity_key_bin,
                                                               feature_name,
                                                               val.SerializeToString(),
                                                               timestamp,
                                                               created_ts])
                self.session.execute(insert_cql)

    def online_read(
            self,
            config: RepoConfig,
            table: Union[FeatureTable, FeatureView],
            entity_keys: List[EntityKeyProto],
            requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        project = config.project
        table_name = _table_id(project, table)
        keyspace = self.online_config.keyspace

        all_rows = []
        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(entity_key)
            cql_query = _create_select_cql(keyspace, table_name,
                                           column_to_select=["feature_name",
                                                             "value",
                                                             "event_ts"],
                                           conditions_eq={"entity_key": entity_key_bin})

            prepared_statement = self.session.prepare(cql_query)

            all_rows += self.session.execute(
                prepared_statement.bind([entity_key_bin])
                                 ).all()

        # Now find the result

        for row in all_rows:
            res = {}
            feature_name = row.feature_name
            value = row.value
            ts = row.event_ts
            val = ValueProto()
            val.ParseFromString(value)
            res[feature_name] = val
            res_ts = ts
            result.append((res_ts, res))
        if not result:
            result.append((None, None))
        return result

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        project = config.project
        key_space = self.online_config.keyspace
        for table in tables_to_keep:
            table_name = _table_id(project, table)

            # Create table if not exist
            cql_create_table = _create_cql_table(key_space, table_name,
                                                 primary_key=["entity_key", "feature_name"],
                                                 columns=[
                                                     "entity_key",
                                                     "feature_name",
                                                     "value",
                                                     "event_ts",
                                                     "created_ts"
                                                 ],
                                                 column_types=[
                                                     "BLOB",
                                                     "TEXT",
                                                     "BLOB",
                                                     "timestamp",
                                                     "timestamp"
                                                 ]
                                                 )
            self.session.execute(cql_create_table)

            # Now create Index
            cql_index = _create_index_cql(key_space, table_name+"_ek", table_name, "entity_key")
            self.session.execute(cql_index)

        for table in tables_to_delete:
            table_name = _table_id(project, table)
            delete_cql = _create_delete_table_cql(key_space, table_name)
            self.session.execute(delete_cql)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        online_config = config.online_store
        assert isinstance(online_config, AstraConfig)
        key_space = online_config.keyspace
        for table in tables:
            table_name = _table_id(config.project, table)
            cql_delete = _create_delete_table_cql(key_space, table_name)
            self.session.execute(cql_delete)


def _create_cql_table(key_space: str,
                      table_name: str,
                      primary_key: List,
                      columns: List[str],
                      column_types: List[str]
                      ) -> str:
    """
    in this function we will create a CQL to create a table
    """
    assert len(columns) > 0, "Columns can not be empty in a Table. "

    assert len(columns) == len(column_types), "Length of columns and type of columns length missmatch."

    # If we have same amount of columns and their types
    cql_create_table = "CREATE TABLE  IF NOT EXISTS " + key_space + "." + table_name
    cql_create_table += " ("
    for col, typ in zip(columns, column_types):
        if col in primary_key:
            cql_create_table += col + " " + typ + ", "
        else:
            cql_create_table += col + " " + typ + " , "
    cql_create_table = cql_create_table[:-2]
    cql_create_table += ", PRIMARY KEY (" + ", ".join(primary_key) + ") "
    cql_create_table += ");"
    return cql_create_table


def _create_cql_insert_record(key_space: str,
                              table_name: str,
                              column_names: List[str],
                              values: List[str]
                              ) -> str:
    """
    This is general function to insert a record in table
    """
    assert len(column_names) > 0, "Columns are empty."

    assert len(column_names) == len(values), " Values size and column names size are not equal."

    cql_insert_record = "INSERT INTO " + key_space + "." + table_name + " ("
    cql_insert_record += ", ".join(column_names) + ") VALUES ("
    for value in values:
        if type(value) == str and value != "now()":
            value = "'" + value + "'"
        cql_insert_record += str(value) + " , "
    cql_insert_record = cql_insert_record[:-2]
    cql_insert_record += ");"
    return cql_insert_record


def _create_cql_update_query(key_space: str, table_name: str,
                             set_columns_value_dict: dict,
                             primary_key_values: dict) -> str:
    """ This function will create an update CQL query"""
    cql_update = "UPDATE " + key_space + "." + table_name + " SET "
    for key in set_columns_value_dict:
        cql_update += key + " = '" + set_columns_value_dict[key] + "', "
    cql_update = cql_update[:-2] + " WHERE "
    # Now where conditions of primary key or composite key
    for key in primary_key_values:
        if primary_key_values[key]["type"] == "str":
            cql_update += key + " = '" + primary_key_values[key]["value"] + "', "
        else:
            cql_update += key + " = " + primary_key_values[key]["value"] + ", "
    cql_update = cql_update[:-2]
    cql_update += ";"
    return cql_update


def _create_select_cql(key_space: str, table_name: str,
                       column_to_select: List,
                       conditions_eq: dict) -> str:
    """
    This function will create a general level Select query for Cassendera
    """
    select_cql = "SELECT "
    if column_to_select:
        select_cql += ", ".join(column_to_select)
    else:
        select_cql += "* "
    select_cql += " FROM " + key_space + "." + table_name
    select_cql += " WHERE "
    for key in conditions_eq:
        select_cql += (key + " = ?" + " AND ")
    select_cql = select_cql[:-4]
    select_cql += " ALLOW FILTERING;"

    return select_cql


def _create_index_cql(key_space: str,
                      index_name: str,
                      table_name: str,
                      index_on: str
                      ) -> str:
    cql_index = "CREATE INDEX " + index_name + " ON "
    cql_index += key_space + "." + table_name + " ("
    cql_index += index_on + ");"
    return cql_index


def _create_delete_table_cql(key_space: str, table_name: str) -> str:
    cql_delete = "DROP TABLE " + key_space + "." + table_name + ";"
    return cql_delete
