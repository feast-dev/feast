import json
import logging
import uuid
from datetime import date, datetime, timezone
from datetime import time as dt_time
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd
import pyarrow
from pydantic import Field, FilePath, SecretStr, StrictBool, StrictStr, model_validator
from trino.auth import (
    BasicAuthentication,
    CertificateAuthentication,
    JWTAuthentication,
    KerberosAuthentication,
    OAuth2Authentication,
)

from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.contrib.trino_offline_store.connectors.upload import (
    upload_pandas_dataframe_to_trino,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import Trino
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import (
    SavedDatasetTrinoStorage,
    TrinoSource,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import get_timestamp_filter_sql
from feast.infra.registry.base_registry import BaseRegistry
from feast.monitoring.monitoring_utils import (
    MON_TABLE_FEATURE,
    MON_TABLE_FEATURE_SERVICE,
    MON_TABLE_FEATURE_VIEW,
    MON_TABLE_JOB,
    empty_categorical_metric,
    empty_numeric_metric,
    monitoring_table_meta,
    normalize_monitoring_row,
    opt_float,
)
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage

logger = logging.getLogger(__name__)


class BasicAuthModel(FeastConfigBaseModel):
    username: StrictStr
    password: StrictStr


class KerberosAuthModel(FeastConfigBaseModel):
    config: Optional[FilePath] = Field(default=None, alias="config-file")
    service_name: Optional[StrictStr] = Field(default=None, alias="service-name")
    mutual_authentication: StrictBool = Field(
        default=False, alias="mutual-authentication"
    )
    force_preemptive: StrictBool = Field(default=False, alias="force-preemptive")
    hostname_override: Optional[StrictStr] = Field(
        default=None, alias="hostname-override"
    )
    sanitize_mutual_error_response: StrictBool = Field(
        default=True, alias="sanitize-mutual-error-response"
    )
    principal: Optional[StrictStr]
    delegate: StrictBool = False
    ca_bundle: Optional[FilePath] = Field(default=None, alias="ca-bundle-file")


class JWTAuthModel(FeastConfigBaseModel):
    token: SecretStr


class CertificateAuthModel(FeastConfigBaseModel):
    cert: Optional[FilePath] = Field(default=None, alias="cert-file")
    key: Optional[FilePath] = Field(default=None, alias="key-file")


CLASSES_BY_AUTH_TYPE = {
    "kerberos": {
        "auth_model": KerberosAuthModel,
        "trino_auth": KerberosAuthentication,
    },
    "basic": {
        "auth_model": BasicAuthModel,
        "trino_auth": BasicAuthentication,
    },
    "jwt": {
        "auth_model": JWTAuthModel,
        "trino_auth": JWTAuthentication,
    },
    "oauth2": {
        "auth_model": None,
        "trino_auth": OAuth2Authentication,
    },
    "certificate": {
        "auth_model": CertificateAuthModel,
        "trino_auth": CertificateAuthentication,
    },
}


class AuthConfig(FeastConfigBaseModel):
    type: Literal["kerberos", "basic", "jwt", "oauth2", "certificate"]
    config: Optional[Dict[StrictStr, Any]]

    @model_validator(mode="after")
    def config_only_nullable_for_oauth2(self):
        auth_type = self.type
        auth_config = self.config
        if auth_type != "oauth2" and auth_config is None:
            raise ValueError(f"config cannot be null for auth type '{auth_type}'")

        return self

    def to_trino_auth(self):
        auth_type = self.type
        trino_auth_cls = CLASSES_BY_AUTH_TYPE[auth_type]["trino_auth"]

        if auth_type == "oauth2":
            return trino_auth_cls()

        model_cls = CLASSES_BY_AUTH_TYPE[auth_type]["auth_model"]
        model = model_cls(**self.config)
        return trino_auth_cls(**model.model_dump())


class TrinoOfflineStoreConfig(FeastConfigBaseModel):
    """Online store config for Trino"""

    type: StrictStr = "trino"
    """ Offline store type selector """

    host: StrictStr
    """ Host of the Trino cluster """

    port: int
    """ Port of the Trino cluster """

    catalog: StrictStr
    """ Catalog of the Trino cluster """

    user: StrictStr
    """ User of the Trino cluster """

    source: Optional[StrictStr] = "trino-python-client"
    """ ID of the feast's Trino Python client, useful for debugging """

    http_scheme: Literal["http", "https"] = Field(default="http", alias="http-scheme")
    """ HTTP scheme that should be used while establishing a connection to the Trino cluster """

    verify: StrictBool = Field(default=True, alias="ssl-verify")
    """ Whether the SSL certificate emited by the Trino cluster should be verified or not """

    extra_credential: Optional[StrictStr] = Field(
        default=None, alias="x-trino-extra-credential-header"
    )
    """ Specifies the HTTP header X-Trino-Extra-Credential, e.g. user1=pwd1, user2=pwd2 """

    connector: Dict[str, str]
    """
    Trino connector to use as well as potential extra parameters.
    Needs to contain at least the path, for example
    {"type": "bigquery"}
    or
    {"type": "hive", "file_format": "parquet"}
    """

    dataset: StrictStr = "feast"
    """ (optional) Trino Dataset name for temporary tables """

    auth: Optional[AuthConfig] = None
    """
    (optional) Authentication mechanism to use when connecting to Trino. Supported options are:
        - kerberos
        - basic
        - jwt
        - oauth2
        - certificate
    """


class TrinoRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: str,
        client: Trino,
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
        temp_table: Optional[str] = None,
    ):
        self._query = query
        self._client = client
        self._config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata
        self._temp_table = temp_table
        self._cleaned_up = False

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _drop_temp_table(self) -> None:
        if self._cleaned_up or not self._temp_table:
            return
        self._cleaned_up = True
        try:
            self._client.execute_query(f"DROP TABLE IF EXISTS {self._temp_table}")
        except Exception:
            logger.exception(
                "Failed to drop temporary entity table %s",
                self._temp_table,
            )

    def __del__(self) -> None:
        self._drop_temp_table()

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """Return dataset as Pandas DataFrame synchronously including on demand transforms"""
        try:
            results = self._client.execute_query(query_text=self._query)
            self.pyarrow_schema = results.pyarrow_schema
            return results.to_dataframe()
        finally:
            self._drop_temp_table()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        """Return payrrow dataset as synchronously including on demand transforms"""
        return pyarrow.Table.from_pandas(self._to_df_internal(timeout=timeout))

    def to_sql(self) -> str:
        """Returns the SQL query that will be executed in Trino to build the historical feature table"""
        return self._query

    def to_trino(
        self,
        destination_table: Optional[str] = None,
        timeout: int = 1800,
        retry_cadence: int = 10,
    ) -> Optional[str]:
        """
        Triggers the execution of a historical feature retrieval query and exports the results to a Trino table.
        Runs for a maximum amount of time specified by the timeout parameter (defaulting to 30 minutes).
        Args:
            timeout: An optional number of seconds for setting the time limit of the QueryJob.
            retry_cadence: An optional number of seconds for setting how long the job should checked for completion.
        Returns:
            Returns the destination table name.
        """
        if not destination_table:
            today = date.today().strftime("%Y%m%d")
            rand_id = str(uuid.uuid4())[:7]
            destination_table = f"{self._client.catalog}.{self._config.offline_store.dataset}.historical_{today}_{rand_id}"

        # TODO: Implement the timeout logic
        try:
            create_query = f"CREATE TABLE {destination_table} AS ({self._query})"
            self._client.execute_query(query_text=create_query)
        finally:
            self._drop_temp_table()
        return destination_table

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        """
        Run the retrieval and persist the results in the same offline store used for read.
        """
        if not isinstance(storage, SavedDatasetTrinoStorage):
            raise ValueError(
                f"The storage object is not a `SavedDatasetTrinoStorage` but is instead a {type(storage)}"
            )
        self.to_trino(destination_table=storage.trino_options.table)

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """
        Return metadata information about retrieval.
        Should be available even before materializing the dataset itself.
        """
        return self._metadata


class TrinoOfflineStore(OfflineStore):
    supports_filter_by_created_timestamp = True

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> TrinoRetrievalJob:
        assert isinstance(config.offline_store, TrinoOfflineStoreConfig)
        assert isinstance(data_source, TrinoSource)

        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(join_key_columns + feature_name_columns + timestamps)
        client = _get_trino_client(config=config)

        query = f"""
            SELECT
                {field_string}
                {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE {timestamp_field} BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
            )
            WHERE _feast_row = 1
            """

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return TrinoRetrievalJob(
            query=query,
            client=client,
            config=config,
            full_feature_names=False,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
        filter_by_created_timestamp: bool = False,
    ) -> TrinoRetrievalJob:
        assert isinstance(config.offline_store, TrinoOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, TrinoSource)

        client = _get_trino_client(config=config)

        table_reference = _get_table_reference_for_new_entity(
            catalog=config.offline_store.catalog,
            dataset_name=config.offline_store.dataset,
        )

        entity_schema = _upload_entity_df_and_get_entity_schema(
            client=client,
            table_name=table_reference,
            entity_df=entity_df,
            connector=config.offline_store.connector,
        )

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(
                entity_schema=entity_schema
            )
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df=entity_df,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
            client=client,
        )

        expected_join_keys = offline_utils.get_expected_join_keys(
            project=project, feature_views=feature_views, registry=registry
        )

        offline_utils.assert_expected_columns_in_entity_df(
            entity_schema=entity_schema,
            join_keys=expected_join_keys,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
        )

        # Build a query context containing all information required to template the Trino SQL query
        query_context = offline_utils.get_feature_view_query_context(
            feature_refs=feature_refs,
            feature_views=feature_views,
            registry=registry,
            project=project,
            entity_df_timestamp_range=entity_df_event_timestamp_range,
        )

        # Generate the Trino SQL query from the query context
        entity_table_ref = table_reference
        if type(entity_df) is str:
            entity_table_ref = f"({entity_df})"
        query = offline_utils.build_point_in_time_query(
            query_context,
            left_table_query_string=entity_table_ref,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
            entity_df_columns=entity_schema.keys(),
            query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
            full_feature_names=full_feature_names,
            filter_by_created_timestamp=filter_by_created_timestamp,
        )

        return TrinoRetrievalJob(
            query=query,
            temp_table=table_reference if isinstance(entity_df, pd.DataFrame) else None,
            client=client,
            config=config,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(set(entity_schema.keys()) - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, TrinoOfflineStoreConfig)
        assert isinstance(data_source, TrinoSource)
        from_expression = data_source.get_table_query_string()

        client = _get_trino_client(config=config)

        timestamp_fields = [timestamp_field]
        if created_timestamp_column:
            timestamp_fields.append(created_timestamp_column)
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamp_fields
        )

        timestamp_filter = get_timestamp_filter_sql(
            start_date,
            end_date,
            timestamp_field,
            quote_fields=False,
            cast_style="timestamp",
            date_time_separator=" ",
        )
        query = f"""
            SELECT {field_string}
            FROM ( {from_expression} )
            WHERE {timestamp_filter}
        """
        return TrinoRetrievalJob(
            query=query,
            client=client,
            config=config,
            full_feature_names=False,
        )

    @staticmethod
    def compute_monitoring_metrics(
        config: RepoConfig,
        data_source: DataSource,
        feature_columns: List[Tuple[str, str]],
        timestamp_field: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        histogram_bins: int = 20,
        top_n: int = 10,
    ) -> List[Dict[str, Any]]:
        assert isinstance(config.offline_store, TrinoOfflineStoreConfig)
        assert isinstance(data_source, TrinoSource)

        client = _get_trino_client(config=config)
        from_expression = data_source.get_table_query_string()
        ts_filter = get_timestamp_filter_sql(
            start_date,
            end_date,
            timestamp_field,
            tz=timezone.utc,
            cast_style="timestamp",
            date_time_separator=" ",
            quote_fields=False,
        )
        ts_clause = ts_filter if ts_filter else "1=1"

        numeric_features = [n for n, t in feature_columns if t == "numeric"]
        categorical_features = [n for n, t in feature_columns if t == "categorical"]
        results: List[Dict[str, Any]] = []

        if numeric_features:
            results.extend(
                _trino_sql_numeric_stats(
                    client,
                    from_expression,
                    numeric_features,
                    ts_clause,
                    histogram_bins,
                )
            )

        for col_name in categorical_features:
            results.append(
                _trino_sql_categorical_stats(
                    client,
                    from_expression,
                    col_name,
                    ts_clause,
                    top_n,
                )
            )

        return results

    @staticmethod
    def get_monitoring_max_timestamp(
        config: RepoConfig,
        data_source: DataSource,
        timestamp_field: str,
    ) -> Optional[datetime]:
        assert isinstance(config.offline_store, TrinoOfflineStoreConfig)
        assert isinstance(data_source, TrinoSource)

        client = _get_trino_client(config=config)
        from_expression = data_source.get_table_query_string()
        q_ts = f'"{timestamp_field}"'
        sql = f"SELECT MAX({q_ts}) AS max_ts FROM {from_expression} AS _src"
        results = client.execute_query(sql)
        rows = results.data
        if not rows or rows[0] is None or rows[0][0] is None:
            return None
        val = rows[0][0]
        if isinstance(val, datetime):
            return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        if isinstance(val, date):
            return datetime.combine(val, dt_time.min, tzinfo=timezone.utc)
        return pd.to_datetime(val, utc=True).to_pydatetime()

    @staticmethod
    def ensure_monitoring_tables(config: RepoConfig) -> None:
        assert isinstance(config.offline_store, TrinoOfflineStoreConfig)
        client = _get_trino_client(config=config)
        catalog = config.offline_store.catalog
        dataset = config.offline_store.dataset

        if dataset:
            try:
                client.execute_query(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{dataset}")
            except Exception:
                logging.exception(f"Failed to create schema {catalog}.{dataset}")
                pass

        with_clause = _trino_table_with_clause(config)
        for ddl_template, tbl_name in zip(
            _TRINO_MONITORING_DDL_STATEMENTS,
            [
                MON_TABLE_FEATURE,
                MON_TABLE_FEATURE_VIEW,
                MON_TABLE_FEATURE_SERVICE,
                MON_TABLE_JOB,
            ],
        ):
            full_table = _trino_monitoring_table_name(config, tbl_name)
            stmt = ddl_template.format(
                table=full_table,
                with_clause=with_clause,
            )
            client.execute_query(stmt)

        for tbl in (
            MON_TABLE_FEATURE,
            MON_TABLE_FEATURE_VIEW,
            MON_TABLE_FEATURE_SERVICE,
        ):
            full_table = _trino_monitoring_table_name(config, tbl)
            try:
                client.execute_query(
                    f"ALTER TABLE {full_table} ADD COLUMN max_event_timestamp TIMESTAMP"
                )
            except Exception:
                # Column already exists on newly created tables or dialect difference
                pass

    @staticmethod
    def save_monitoring_metrics(
        config: RepoConfig,
        metric_type: str,
        metrics: List[Dict[str, Any]],
    ) -> None:
        if not metrics:
            return
        assert isinstance(config.offline_store, TrinoOfflineStoreConfig)
        table, columns, pk_columns = monitoring_table_meta(metric_type)
        full_table_name = _trino_monitoring_table_name(config, table)
        pdf_new = pd.DataFrame([{c: m.get(c) for c in columns} for m in metrics])
        pdf_new = _trino_normalize_histogram_column(pdf_new)

        client = _get_trino_client(config=config)
        try:
            results = client.execute_query(f"SELECT * FROM {full_table_name}")
            pdf_old = results.to_dataframe()
            pdf_merged = _trino_pandas_upsert(pdf_old, pdf_new, pk_columns)
        except Exception:
            pdf_merged = pdf_new

        try:
            client.execute_query(f"DROP TABLE IF EXISTS {full_table_name}")
        except Exception:
            pass

        upload_pandas_dataframe_to_trino(
            client=client,
            df=pdf_merged,
            table=full_table_name,
            connector_args=config.offline_store.connector,
        )

    @staticmethod
    def query_monitoring_metrics(
        config: RepoConfig,
        project: str,
        metric_type: str,
        filters: Optional[Dict[str, Any]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        assert isinstance(config.offline_store, TrinoOfflineStoreConfig)
        table, columns, _ = monitoring_table_meta(metric_type)
        full_table_name = _trino_monitoring_table_name(config, table)
        client = _get_trino_client(config=config)

        conditions: List[str] = []
        if project:
            conditions.append(f'"project_id" = {_trino_sql_literal(project)}')
        if filters:
            for key, value in filters.items():
                if value is not None:
                    conditions.append(f'"{key}" = {_trino_sql_literal(value)}')
        if start_date is not None:
            conditions.append(
                f"\"metric_date\" >= DATE '{start_date.strftime('%Y-%m-%d')}'"
            )
        if end_date is not None:
            conditions.append(
                f"\"metric_date\" <= DATE '{end_date.strftime('%Y-%m-%d')}'"
            )

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        order_col = '"metric_date"' if metric_type != "job" else '"job_id"'
        cols_str = ", ".join(f'"{c}"' for c in columns)
        query = f"SELECT {cols_str} FROM {full_table_name} {where_clause} ORDER BY {order_col}"

        try:
            results = client.execute_query(query)
            df = results.to_dataframe()
            if df.empty:
                return []
            return [normalize_monitoring_row(row.to_dict()) for _, row in df.iterrows()]
        except Exception:
            return []

    @staticmethod
    def clear_monitoring_baseline(
        config: RepoConfig,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        data_source_type: Optional[str] = None,
    ) -> None:
        assert isinstance(config.offline_store, TrinoOfflineStoreConfig)
        client = _get_trino_client(config=config)
        full_table_name = _trino_monitoring_table_name(config, MON_TABLE_FEATURE)

        try:
            results = client.execute_query(f"SELECT * FROM {full_table_name}")
            pdf = results.to_dataframe()
        except Exception:
            return

        if pdf.empty:
            return

        mask = (pdf["project_id"] == project) & (pdf["is_baseline"] == True)  # noqa: E712
        if feature_view_name is not None:
            mask &= pdf["feature_view_name"] == feature_view_name
        if feature_name is not None:
            mask &= pdf["feature_name"] == feature_name
        if data_source_type is not None:
            mask &= pdf["data_source_type"] == data_source_type

        if not mask.any():
            return

        pdf.loc[mask, "is_baseline"] = False
        try:
            client.execute_query(f"DROP TABLE IF EXISTS {full_table_name}")
        except Exception:
            pass

        upload_pandas_dataframe_to_trino(
            client=client,
            df=pdf,
            table=full_table_name,
            connector_args=config.offline_store.connector,
        )


def _trino_monitoring_table_name(config: RepoConfig, table: str) -> str:
    catalog = config.offline_store.catalog
    dataset = config.offline_store.dataset
    if dataset:
        return f"{catalog}.{dataset}.{table}"
    return f"{catalog}.{table}"


def _trino_table_with_clause(config: RepoConfig) -> str:
    connector_args = config.offline_store.connector or {}
    connector_type = connector_args.get("type", "")
    if connector_type in {"hive", "iceberg"}:
        file_format = connector_args.get("file_format", "parquet")
        return f"WITH (format = '{file_format}')"
    return ""


def _trino_normalize_histogram_column(pdf: pd.DataFrame) -> pd.DataFrame:
    if "histogram" not in pdf.columns:
        return pdf
    out = pdf.copy()

    def _ser(x: Any) -> Any:
        if x is None:
            return None
        if isinstance(x, str):
            return x
        return json.dumps(x)

    out["histogram"] = out["histogram"].map(_ser)
    return out


def _trino_pandas_upsert(
    pdf_old: pd.DataFrame,
    pdf_new: pd.DataFrame,
    pk_columns: List[str],
) -> pd.DataFrame:
    if pdf_old.empty:
        return pdf_new
    pk_cols_present = [
        c for c in pk_columns if c in pdf_old.columns and c in pdf_new.columns
    ]
    if not pk_cols_present:
        return pd.concat([pdf_old, pdf_new], ignore_index=True)
    old_idx = pdf_old.set_index(pk_cols_present)
    new_idx = pdf_new.set_index(pk_cols_present)
    kept = old_idx.loc[~old_idx.index.isin(new_idx.index)]
    kept_df = kept.reset_index()
    return pd.concat([kept_df, pdf_new], ignore_index=True)


def _trino_sql_literal(val: Any) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, (bool, np.bool_)):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float, np.integer, np.floating)):
        return str(val)
    if isinstance(val, (datetime, pd.Timestamp)):
        return f"TIMESTAMP '{val.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    if isinstance(val, date):
        return f"DATE '{val.strftime('%Y-%m-%d')}'"
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"


def _trino_sql_numeric_stats(
    client: Trino,
    from_expression: str,
    feature_names: List[str],
    ts_clause: str,
    histogram_bins: int,
) -> List[Dict[str, Any]]:
    select_parts = ["COUNT(*)"]
    for col in feature_names:
        q = f'"{col}"'
        c = f"CAST({q} AS DOUBLE)"
        select_parts.extend(
            [
                f"COUNT({q})",
                f"AVG({c})",
                f"STDDEV_SAMP({c})",
                f"MIN({c})",
                f"MAX({c})",
                f"APPROX_PERCENTILE({c}, 0.50)",
                f"APPROX_PERCENTILE({c}, 0.75)",
                f"APPROX_PERCENTILE({c}, 0.90)",
                f"APPROX_PERCENTILE({c}, 0.95)",
                f"APPROX_PERCENTILE({c}, 0.99)",
            ]
        )

    query = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {from_expression} AS _src WHERE {ts_clause}"
    )
    results = client.execute_query(query)
    rows = results.data
    if not rows or rows[0] is None or rows[0][0] is None:
        return [empty_numeric_metric(n) for n in feature_names]

    row = rows[0]
    row_count = int(row[0] or 0)
    metric_results: List[Dict[str, Any]] = []

    for i, col in enumerate(feature_names):
        base = 1 + i * 10
        non_null = int(row[base] or 0)
        null_count = row_count - non_null

        min_val = opt_float(row[base + 3])
        max_val = opt_float(row[base + 4])

        result: Dict[str, Any] = {
            "feature_name": col,
            "feature_type": "numeric",
            "row_count": row_count,
            "null_count": null_count,
            "null_rate": null_count / row_count if row_count > 0 else 0.0,
            "mean": opt_float(row[base + 1]),
            "stddev": opt_float(row[base + 2]),
            "min_val": min_val,
            "max_val": max_val,
            "p50": opt_float(row[base + 5]),
            "p75": opt_float(row[base + 6]),
            "p90": opt_float(row[base + 7]),
            "p95": opt_float(row[base + 8]),
            "p99": opt_float(row[base + 9]),
            "histogram": None,
        }

        if min_val is not None and max_val is not None and non_null > 0:
            result["histogram"] = _trino_sql_numeric_histogram(
                client,
                from_expression,
                col,
                ts_clause,
                histogram_bins,
                min_val,
                max_val,
            )

        metric_results.append(result)

    return metric_results


def _trino_sql_numeric_histogram(
    client: Trino,
    from_expression: str,
    col_name: str,
    ts_clause: str,
    bins: int,
    min_val: float,
    max_val: float,
) -> Dict[str, Any]:
    q_col = f'"{col_name}"'

    if min_val == max_val:
        sql = (
            f"SELECT COUNT(*) FROM {from_expression} AS _src "
            f"WHERE {q_col} IS NOT NULL AND {ts_clause}"
        )
        res = client.execute_query(sql)
        cnt = int(res.data[0][0] or 0) if res.data and res.data[0] else 0
        return {"bins": [min_val, max_val], "counts": [cnt], "bin_width": 0.0}

    bin_width = (max_val - min_val) / bins
    cast_col = f"CAST({q_col} AS DOUBLE)"
    inner = (
        f"CASE WHEN {min_val} = {max_val} THEN CAST(1 AS BIGINT) "
        f"ELSE LEAST(GREATEST(CAST(FLOOR(({cast_col} - {min_val}) / {bin_width}) + 1 AS BIGINT), CAST(1 AS BIGINT)), CAST({bins} AS BIGINT)) "
        f"END AS bucket"
    )

    query = (
        f"SELECT bucket, COUNT(*) AS cnt FROM ("
        f"  SELECT {inner} "
        f"  FROM {from_expression} AS _src "
        f"  WHERE {q_col} IS NOT NULL AND {ts_clause}"
        f") AS _b WHERE bucket IS NOT NULL "
        f"GROUP BY bucket ORDER BY bucket"
    )
    res = client.execute_query(query)
    hrows = res.data or []
    counts = [0] * bins
    for hr in hrows:
        bucket = int(hr[0] or 0)
        cnt = int(hr[1] or 0)
        if 1 <= bucket <= bins:
            counts[bucket - 1] = cnt

    bin_edges = [min_val + i * bin_width for i in range(bins + 1)]
    return {
        "bins": [float(b) for b in bin_edges],
        "counts": counts,
        "bin_width": float(bin_width),
    }


def _trino_sql_categorical_stats(
    client: Trino,
    from_expression: str,
    col_name: str,
    ts_clause: str,
    top_n: int,
) -> Dict[str, Any]:
    q_col = f'"{col_name}"'

    query = (
        f"WITH filtered AS ("
        f"  SELECT * FROM {from_expression} AS _src WHERE {ts_clause}"
        f") "
        f"SELECT "
        f"  (SELECT COUNT(*) FROM filtered) AS row_count, "
        f"  (SELECT COUNT(*) - COUNT({q_col}) FROM filtered) AS null_count, "
        f"  (SELECT COUNT(DISTINCT {q_col}) FROM filtered "
        f"   WHERE {q_col} IS NOT NULL) AS unique_count, "
        f"  CAST({q_col} AS VARCHAR) AS value, COUNT(*) AS cnt "
        f"FROM filtered WHERE {q_col} IS NOT NULL "
        f"GROUP BY {q_col} ORDER BY cnt DESC LIMIT {int(top_n)}"
    )

    res = client.execute_query(query)
    rows = res.data or []
    if not rows:
        return empty_categorical_metric(col_name)

    row_count = int(rows[0][0] or 0)
    null_count = int(rows[0][1] or 0)
    unique_count = int(rows[0][2] or 0)

    top_entries = [{"value": r[3], "count": int(r[4] or 0)} for r in rows]
    top_total = sum(e["count"] for e in top_entries)
    other_count = (row_count - null_count) - top_total

    return {
        "feature_name": col_name,
        "feature_type": "categorical",
        "row_count": row_count,
        "null_count": null_count,
        "null_rate": null_count / row_count if row_count > 0 else 0.0,
        "mean": None,
        "stddev": None,
        "min_val": None,
        "max_val": None,
        "p50": None,
        "p75": None,
        "p90": None,
        "p95": None,
        "p99": None,
        "histogram": {
            "values": top_entries,
            "other_count": max(other_count, 0),
            "unique_count": unique_count,
        },
    }


_TRINO_MONITORING_DDL_STATEMENTS = [
    """
CREATE TABLE IF NOT EXISTS {table} (
    project_id        VARCHAR,
    feature_view_name VARCHAR,
    feature_name      VARCHAR,
    metric_date       DATE,
    granularity       VARCHAR,
    data_source_type  VARCHAR,
    computed_at       TIMESTAMP,
    max_event_timestamp TIMESTAMP,
    is_baseline       BOOLEAN,
    feature_type      VARCHAR,
    row_count         BIGINT,
    null_count        BIGINT,
    null_rate         DOUBLE,
    mean              DOUBLE,
    stddev            DOUBLE,
    min_val           DOUBLE,
    max_val           DOUBLE,
    p50               DOUBLE,
    p75               DOUBLE,
    p90               DOUBLE,
    p95               DOUBLE,
    p99               DOUBLE,
    histogram         VARCHAR
) {with_clause}
""",
    """
CREATE TABLE IF NOT EXISTS {table} (
    project_id        VARCHAR,
    feature_view_name VARCHAR,
    metric_date       DATE,
    granularity       VARCHAR,
    data_source_type  VARCHAR,
    computed_at       TIMESTAMP,
    max_event_timestamp TIMESTAMP,
    is_baseline       BOOLEAN,
    total_row_count   BIGINT,
    total_features    INTEGER,
    features_with_nulls INTEGER,
    avg_null_rate     DOUBLE,
    max_null_rate     DOUBLE
) {with_clause}
""",
    """
CREATE TABLE IF NOT EXISTS {table} (
    project_id           VARCHAR,
    feature_service_name VARCHAR,
    metric_date          DATE,
    granularity          VARCHAR,
    data_source_type     VARCHAR,
    computed_at          TIMESTAMP,
    max_event_timestamp  TIMESTAMP,
    is_baseline          BOOLEAN,
    total_feature_views  INTEGER,
    total_features       INTEGER,
    avg_null_rate        DOUBLE,
    max_null_rate        DOUBLE
) {with_clause}
""",
    """
CREATE TABLE IF NOT EXISTS {table} (
    job_id            VARCHAR,
    project_id        VARCHAR,
    feature_view_name VARCHAR,
    job_type          VARCHAR,
    status            VARCHAR,
    parameters        VARCHAR,
    metric_date       DATE,
    started_at        TIMESTAMP,
    completed_at      TIMESTAMP,
    error_message     VARCHAR,
    result_summary    VARCHAR
) {with_clause}
""",
]


def _get_table_reference_for_new_entity(
    catalog: str,
    dataset_name: str,
) -> str:
    """Gets the table_id for the new entity to be uploaded."""
    table_name = offline_utils.get_temp_entity_table_name()
    return f"{catalog}.{dataset_name}.{table_name}"


def _upload_entity_df_and_get_entity_schema(
    client: Trino,
    table_name: str,
    entity_df: Union[pd.DataFrame, str],
    connector: Dict[str, str],
) -> Dict[str, np.dtype]:
    """Uploads a Pandas entity dataframe into a Trino table and returns the resulting table"""
    if type(entity_df) is str:
        results = client.execute_query(f"SELECT * FROM ({entity_df}) LIMIT 1")

        limited_entity_df = pd.DataFrame(
            data=results.data, columns=results.columns_names
        )
        for col_name, col_type in results.schema.items():
            if col_type == "timestamp":
                limited_entity_df[col_name] = pd.to_datetime(
                    limited_entity_df[col_name]
                )
        entity_schema = dict(zip(limited_entity_df.columns, limited_entity_df.dtypes))

        return entity_schema
    elif isinstance(entity_df, pd.DataFrame):
        upload_pandas_dataframe_to_trino(
            client=client, df=entity_df, table=table_name, connector_args=connector
        )
        entity_schema = dict(zip(entity_df.columns, entity_df.dtypes))
        return entity_schema
    else:
        raise InvalidEntityType(type(entity_df))


def _get_trino_client(config: RepoConfig) -> Trino:
    auth = None
    if config.offline_store.auth is not None:
        auth = config.offline_store.auth.to_trino_auth()

    return Trino(
        host=config.offline_store.host,
        port=config.offline_store.port,
        user=config.offline_store.user,
        catalog=config.offline_store.catalog,
        source=config.offline_store.source,
        http_scheme=config.offline_store.http_scheme,
        verify=config.offline_store.verify,
        extra_credential=config.offline_store.extra_credential,
        auth=auth,
    )


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    client: Trino,
) -> Tuple[datetime, datetime]:
    if type(entity_df) is str:
        results = client.execute_query(
            f"SELECT MIN({entity_df_event_timestamp_col}) AS min, MAX({entity_df_event_timestamp_col}) AS max "
            f"FROM ({entity_df})"
        )

        entity_df_event_timestamp_range = (
            pd.to_datetime(results.data[0][0]).to_pydatetime(),
            pd.to_datetime(results.data[0][1]).to_pydatetime(),
        )
    elif isinstance(entity_df, pd.DataFrame):
        entity_df_event_timestamp = entity_df.loc[
            :, entity_df_event_timestamp_col
        ].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(
                entity_df_event_timestamp, utc=True
            )
        entity_df_event_timestamp_range = (
            entity_df_event_timestamp.min().to_pydatetime(),
            entity_df_event_timestamp.max().to_pydatetime(),
        )
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,CONCAT(
                {% for entity in featureview.entities %}
                    CAST({{entity}} AS VARCHAR),
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS VARCHAR)
            ) AS {{featureview.name}}__entity_row_unique_id
            {% else %}
            ,CAST({{entity_df_event_timestamp_col}} AS VARCHAR) AS {{featureview.name}}__entity_row_unique_id
            {% endif %}
        {% endfor %}
    FROM {{ left_table_query_string }}
),
{% for featureview in featureviews %}
{{ featureview.name }}__entity_dataframe AS (
    SELECT
        {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY
        {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
),
/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.
 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `timestamp_field`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `timestamp_field`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously
 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/
{{ featureview.name }}__subquery AS (
    SELECT
        {{ featureview.timestamp_field }} as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM (
        {{ featureview.table_subquery }}
    ) AS {{ featureview.name }}__subquery
    WHERE {{ featureview.timestamp_field }} <= from_iso8601_timestamp('{{ featureview.max_event_timestamp }}')
    {% if featureview.ttl == 0 %}{% else %}
    AND {{ featureview.timestamp_field }} >= from_iso8601_timestamp('{{ featureview.min_event_timestamp }}')
    {% endif %}
),
{{ featureview.name }}__base AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe.{{featureview.name}}__entity_row_unique_id
    FROM {{ featureview.name }}__subquery AS subquery
    INNER JOIN {{ featureview.name }}__entity_dataframe AS entity_dataframe
    ON TRUE
        AND subquery.event_timestamp <= entity_dataframe.entity_timestamp
        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= entity_dataframe.entity_timestamp - interval '{{ featureview.ttl }}' second
        {% endif %}
        {% if filter_by_created_timestamp and featureview.created_timestamp_column %}
        AND subquery.created_timestamp <= entity_dataframe.entity_timestamp
        {% endif %}
        {% for entity in featureview.entities %}
        AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
        {% endfor %}
),
/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
{{ featureview.name }}__dedup AS (
    SELECT
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM {{ featureview.name }}__base
    GROUP BY {{featureview.name}}__entity_row_unique_id, event_timestamp
),
{% endif %}
/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
{{ featureview.name }}__latest AS (
    SELECT
        event_timestamp,
        {% if featureview.created_timestamp_column %}created_timestamp,{% endif %}
        {{featureview.name}}__entity_row_unique_id
    FROM
    (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY {{featureview.name}}__entity_row_unique_id
                ORDER BY event_timestamp DESC{% if featureview.created_timestamp_column %},created_timestamp DESC{% endif %}
            ) AS row_number
        FROM {{ featureview.name }}__base
        {% if featureview.created_timestamp_column %}
            INNER JOIN {{ featureview.name }}__dedup
            USING ({{featureview.name}}__entity_row_unique_id, event_timestamp, created_timestamp)
        {% endif %}
    )
    WHERE row_number = 1
),
/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*, {{featureview.name}}__entity_row_unique_id
    FROM {{ featureview.name }}__base as base
    INNER JOIN {{ featureview.name }}__latest
    USING(
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}
{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */
SELECT {{ final_output_feature_names | join(', ')}}
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) USING ({{featureview.name}}__entity_row_unique_id)
{% endfor %}
"""
