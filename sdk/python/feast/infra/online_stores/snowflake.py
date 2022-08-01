import itertools
import os
from binascii import hexlify
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import pytz
from pydantic import Field
from pydantic.schema import Literal

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.utils.snowflake_utils import get_snowflake_conn, write_pandas_binary
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage


class SnowflakeOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for Snowflake"""

    type: Literal["snowflake.online"] = "snowflake.online"
    """ Online store type selector"""

    config_path: Optional[str] = (
        Path(os.environ["HOME"]) / ".snowsql/config"
    ).__str__()
    """ Snowflake config path -- absolute path required (Can't use ~)"""

    account: Optional[str] = None
    """ Snowflake deployment identifier -- drop .snowflakecomputing.com"""

    user: Optional[str] = None
    """ Snowflake user name """

    password: Optional[str] = None
    """ Snowflake password """

    role: Optional[str] = None
    """ Snowflake role name"""

    warehouse: Optional[str] = None
    """ Snowflake warehouse name """

    database: Optional[str] = None
    """ Snowflake database name """

    schema_: Optional[str] = Field("PUBLIC", alias="schema")
    """ Snowflake schema name """

    class Config:
        allow_population_by_field_name = True


class SnowflakeOnlineStore(OnlineStore):
    @log_exceptions_and_usage(online_store="snowflake")
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

            timestamp = _to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = _to_naive_utc(created_ts)

            entity_key_serialization_version = (
                config.entity_key_serialization_version
                if config.entity_key_serialization_version
                else 2
            )
            for j, (feature_name, val) in enumerate(values.items()):
                df.loc[j, "entity_feature_key"] = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version,
                ) + bytes(feature_name, encoding="utf-8")
                df.loc[j, "entity_key"] = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version,
                )
                df.loc[j, "feature_name"] = feature_name
                df.loc[j, "value"] = val.SerializeToString()
                df.loc[j, "event_ts"] = timestamp
                df.loc[j, "created_ts"] = created_ts

            dfs[i] = df

        if dfs:
            agg_df = pd.concat(dfs)

            # This combines both the data upload plus the overwrite in the same transaction
            with get_snowflake_conn(config.online_store, autocommit=False) as conn:
                write_pandas_binary(
                    conn, agg_df, f"[online-transient] {config.project}_{table.name}"
                )  # special function for writing binary to snowflake

                query = f"""
                    INSERT OVERWRITE INTO "{config.online_store.database}"."{config.online_store.schema_}"."[online-transient] {config.project}_{table.name}"
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
                              "{config.online_store.database}"."{config.online_store.schema_}"."[online-transient] {config.project}_{table.name}")
                        WHERE
                            "_feast_row" = 1;
                """

                conn.cursor().execute(query)

            if progress:
                progress(len(data))

        return None

    @log_exceptions_and_usage(online_store="snowflake")
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: List[str],
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        assert isinstance(config.online_store, SnowflakeOnlineStoreConfig)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        entity_key_serialization_version = (
            config.entity_key_serialization_version
            if config.entity_key_serialization_version
            else 2
        )

        entity_fetch_str = ",".join(
            [
                (
                    "TO_BINARY("
                    + hexlify(
                        serialize_entity_key(combo[0], entity_key_serialization_version)
                        + bytes(combo[1], encoding="utf-8")
                    ).__str__()[1:]
                    + ")"
                )
                for combo in itertools.product(entity_keys, requested_features)
            ]
        )

        with get_snowflake_conn(config.online_store) as conn:

            df = (
                conn.cursor()
                .execute(
                    f"""
                SELECT
                    "entity_key", "feature_name", "value", "event_ts"
                FROM
                    "{config.online_store.database}"."{config.online_store.schema_}"."[online-transient] {config.project}_{table.name}"
                WHERE
                    "entity_feature_key" IN ({entity_fetch_str})
            """,
                )
                .fetch_pandas_all()
            )

        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version,
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

    @log_exceptions_and_usage(online_store="snowflake")
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

        with get_snowflake_conn(config.online_store) as conn:

            for table in tables_to_keep:

                conn.cursor().execute(
                    f"""CREATE TRANSIENT TABLE IF NOT EXISTS "{config.online_store.database}"."{config.online_store.schema_}"."[online-transient] {config.project}_{table.name}" (
                        "entity_feature_key" BINARY,
                        "entity_key" BINARY,
                        "feature_name" VARCHAR,
                        "value" BINARY,
                        "event_ts" TIMESTAMP,
                        "created_ts" TIMESTAMP
                        )"""
                )

            for table in tables_to_delete:

                conn.cursor().execute(
                    f'DROP TABLE IF EXISTS "{config.online_store.database}"."{config.online_store.schema_}"."[online-transient] {config.project}_{table.name}"'
                )

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        assert isinstance(config.online_store, SnowflakeOnlineStoreConfig)

        with get_snowflake_conn(config.online_store) as conn:

            for table in tables:
                query = f'DROP TABLE IF EXISTS "{config.online_store.database}"."{config.online_store.schema_}"."[online-transient] {config.project}_{table.name}"'
                conn.cursor().execute(query)


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
