import ibis
from pydantic import StrictStr

from feast.infra.offline_stores.ibis import IbisOfflineStore
from feast.repo_config import FeastConfigBaseModel


class DuckDBOfflineStoreConfig(FeastConfigBaseModel):
    type: StrictStr = "duckdb"
    # """ Offline store type selector"""


class DuckDBOfflineStore(IbisOfflineStore):
    @staticmethod
    def setup_ibis_backend():
        # there's no need to call setup as duckdb is default ibis backend
        ibis.set_backend("duckdb")
