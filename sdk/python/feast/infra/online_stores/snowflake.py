import itertools
import os
from binascii import hexlify
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

import pandas as pd
from pydantic import ConfigDict, Field, StrictStr

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.utils.snowflake.snowflake_utils import (
    GetSnowflakeConnection,
    execute_snowflake_statement,
    get_snowflake_online_store_path,
    write_pandas_binary,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.utils import to_naive_utc


class SnowflakeOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for Snowflake"""

    type: Literal["snowflake.online"] = "snowflake.online"
    """ Online store type selector """

    config_path: Optional[str] = os.path.expanduser("~/.snowsql/config")
    """ Snowflake snowsql config path -- absolute path required (Cant use ~)"""

    connection_name: Optional[str] = None
    """ Snowflake connector connection name -- typically defined in ~/.snowflake/connections.toml """

    account: Optional[str] = None
    """ Snowflake deployment identifier -- drop .snowflakecomputing.com """

    user: Optional[str] = None
    """ Snowflake user name """

    password: Optional[str] = None
    """ Snowflake password """

    role: Optional[str] = None
    """ Snowflake role name """

    warehouse: Optional[str] = None
    """ Snowflake warehouse name """

    authenticator: Optional[str] = None
    """ Snowflake authenticator name """

    private_key: Optional[str] = None
    """ Snowflake private key file path"""

    private_key_content: Optional[bytes] = None
    """ Snowflake private key stored as bytes"""

    private_key_passphrase: Optional[str] = None
    """ Snowflake private key file passphrase"""

    database: StrictStr
    """ Snowflake database name """

    schema_: Optional[str] = Field("PUBLIC", alias="schema")
    """ Snowflake schema name """
    model_config = ConfigDict(populate_by_name=True, extra="allow")


class SnowflakeOnlineStore(OnlineStore):
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        assert isinstance(config.online_store, SnowflakeOnlineStoreConfig)

        dfs = [None] * len(data)
        for i, (entity_key, values, timestamp, created_ts) in enumerate(data):
            df = pd.DataFrame(
                columns=[
                    "entity_feature_key",
                    "entity_key",
                    "feature_name",
                    "value",
                    "event_ts",
                    "created_ts",
                ],
                index=range(0, len(values)),
            )

            timestamp = to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)

            for j, (feature_name, val) in enumerate(values.items()):
                df.loc[j, "entity_feature_key"] = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                ) + bytes(feature_name, encoding="utf-8")
                df.loc[j, "entity_key"] = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                df.loc[j, "feature_name"] = feature_name
                df.loc[j, "value"] = val.SerializeToString()
                df.loc[j, "event_ts"] = timestamp
                df.loc[j, "created_ts"] = created_ts

            dfs[i] = df

        if dfs:
            agg_df = pd.concat(dfs)

            # This combines both the data upload plus the overwrite in the same transaction
            online_path = get_snowflake_online_store_path(config, table)
            with GetSnowflakeConnection(config.online_store, autocommit=False) as conn:
                write_pandas_binary(
                    conn,
                    agg_df,
                    table_name=f"[online-transient] {config.project}_{table.name}",
                    database=f"{config.online_store.database}",
                    schema=f"{config.online_store.schema_}",
                )  # special function for writing binary to snowflake

                query = f"""
                    INSERT OVERWRITE INTO {online_path}."[online-transient] {config.project}_{table.name}"
                        SELECT
                            "entity_feature_key",
                            "entity_key",
                            "feature_name",
                            "value",
                            "event_ts",
                            "created_ts"
                        FROM
                          (SELECT
                              *,
                              ROW_NUMBER() OVER(PARTITION BY "entity_key","feature_name" ORDER BY "event_ts" DESC, "created_ts" DESC) AS "_feast_row"
                          FROM
                              {online_path}."[online-transient] {config.project}_{table.name}")
                        WHERE
                            "_feast_row" = 1;
                """
                execute_snowflake_statement(conn, query)
                conn.commit()
            if progress:
                progress(len(data))

        return None

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        assert isinstance(config.online_store, SnowflakeOnlineStoreConfig)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        requested_features = requested_features if requested_features else []

        entity_fetch_str = ",".join(
            [
                (
                    "TO_BINARY("
                    + hexlify(
                        serialize_entity_key(
                            combo[0],
                            entity_key_serialization_version=config.entity_key_serialization_version,
                        )
                        + bytes(combo[1], encoding="utf-8")
                    ).__str__()[1:]
                    + ")"
                )
                for combo in itertools.product(entity_keys, requested_features)
            ]
        )

        online_path = get_snowflake_online_store_path(config, table)
        with GetSnowflakeConnection(config.online_store) as conn:
            query = f"""
                SELECT
                    "entity_key", "feature_name", "value", "event_ts"
                FROM
                    {online_path}."[online-transient] {config.project}_{table.name}"
                WHERE
                    "entity_feature_key" IN ({entity_fetch_str})
            """
            df = execute_snowflake_statement(conn, query).fetch_pandas_all()

        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            res = {}
            res_ts = None
            for index, row in df[df["entity_key"] == entity_key_bin].iterrows():
                val = ValueProto()
                val.ParseFromString(row["value"])
                res[row["feature_name"]] = val
                res_ts = row["event_ts"].to_pydatetime()

            if not res:
                result.append((None, None))
            else:
                result.append((res_ts, res))
        return result

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        assert isinstance(config.online_store, SnowflakeOnlineStoreConfig)

        with GetSnowflakeConnection(config.online_store) as conn:
            for table in tables_to_keep:
                online_path = get_snowflake_online_store_path(config, table)
                query = f"""
                    CREATE TRANSIENT TABLE IF NOT EXISTS {online_path}."[online-transient] {config.project}_{table.name}" (
                        "entity_feature_key" BINARY,
                        "entity_key" BINARY,
                        "feature_name" VARCHAR,
                        "value" BINARY,
                        "event_ts" TIMESTAMP,
                        "created_ts" TIMESTAMP
                    )
                """
                execute_snowflake_statement(conn, query)

            for table in tables_to_delete:
                online_path = get_snowflake_online_store_path(config, table)
                query = f'DROP TABLE IF EXISTS {online_path}."[online-transient] {config.project}_{table.name}"'
                execute_snowflake_statement(conn, query)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        assert isinstance(config.online_store, SnowflakeOnlineStoreConfig)

        with GetSnowflakeConnection(config.online_store) as conn:
            for table in tables:
                online_path = get_snowflake_online_store_path(config, table)
                query = f'DROP TABLE IF EXISTS {online_path}."[online-transient] {config.project}_{table.name}"'
                execute_snowflake_statement(conn, query)
