from datetime import datetime

import pytz
from typing import Union

from feast import FeatureTable
from feast.feature_view import FeatureView


def _table_id(project: str, table: Union[FeatureTable, FeatureView]) -> str:
    return f"{project}_{table.name}"


def _to_naive_utc(ts: datetime):
    return ts.astimezone(pytz.utc).replace(tzinfo=None)