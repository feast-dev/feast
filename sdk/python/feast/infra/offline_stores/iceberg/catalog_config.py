import logging
from typing import Optional

from pydantic import StrictBool, StrictStr

from feast.repo_config import FeastConfigBaseModel

logger = logging.getLogger(__name__)


class IcebergCatalogConfig(FeastConfigBaseModel):
    """Configuration for connecting to an Iceberg REST Catalog.

    This config is generic and works with any Iceberg REST catalog
    implementation: Databricks Unity Catalog, Apache Polaris, Nessie,
    Tabular, etc. Authentication is via an environment variable
    (``token_env_var``) and the Iceberg REST ``v1/oauth/tokens``
    endpoint where supported.

    The three-level namespace (``warehouse.namespace.table``) maps to
    the Iceberg REST Catalog's ``prefix`` (warehouse/preﬁx),
    ``namespace`` (schema), and ``table`` concepts.
    """

    type: StrictStr = "rest"
    """Catalog backend type.  One of ``"rest"`` (generic REST catalog),
    ``"databricks"``, ``"polaris"``, ``"nessie"``.
    See ``pyiceberg.catalog.load_catalog()`` for the full list."""

    endpoint: StrictStr
    """Iceberg REST Catalog endpoint URL.

    For Databricks Unity Catalog this is typically:
    ``https://<workspace>.databricks.net/api/2.1/unity-catalog/iceberg``

    For Apache Polaris this is the Polaris REST endpoint."""

    warehouse: StrictStr
    """Warehouse (sometimes called ``prefix`` or ``catalog`` in the
    Iceberg REST protocol).  For Unity Catalog this is the catalog name,
    e.g. ``prod_ml``."""

    namespace: StrictStr
    """Default namespace (schema) within the warehouse, e.g. ``features``."""

    token_env_var: StrictStr = "ICEBERG_REST_TOKEN"
    """Environment variable name that holds the bearer token / PAT for
    the Iceberg REST Catalog."""

    credential_vending: StrictBool = True
    """Whether to use the Iceberg REST ``v1/oauth/tokens`` credential
    vending flow when configuring the underlying file IO for engines."""

    warehouse_location: Optional[StrictStr] = None
    """Optional storage location for warehouse data (``s3://...``,
    ``abfss://...``, ``gs://...``).  If not set, the catalog may
    derive it from the endpoint response."""

    rest_signing: StrictBool = False
    """Enable REST sign‑and‑push for write operations when the REST
    catalog supports the ``v1/configure`` signing endpoint."""
