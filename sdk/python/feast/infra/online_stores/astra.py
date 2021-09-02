from abc import ABC
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast import FeatureTable, FeatureView
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
                table_name = _table_id
                keyspace = self.online_config.keyspace

                pass
        pass


def _create_cql_table(key_space: str,
                      table_name: str,
                      primary_key: str,
                      columns: List,
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
        if col == primary_key:
            cql_create_table += col + " " + typ + ", "
        else:
            cql_create_table += col + " " + typ + " , "
    cql_create_table = cql_create_table[:-2]
    cql_create_table += ", PRIMARY KEY (" + primary_key + ") "
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
