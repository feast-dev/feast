import os
from typing import Callable, Dict, Iterable, Optional, Tuple

import pandas as pd

from feast.errors import MaxcomputeCredentialsError, MaxcomputeQueryError
from feast.type_map import pa_to_redshift_value_type

try:
    import odps
except ImportError as e:
    from odps.errors import NoSuchObject as NoSuchObject

    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aliyun", str(e))


def get_maxcompute_client(
    ak: str, sk: str, project: str, region: str, endpoint: Optional[str] = ""
):
    try:
        o = odps.ODPS(
            access_id=ak, secret_access_key=sk, project=project, endpoint=endpoint
        )
    except:
        raise FeastProviderLoginError(
            "Aliyun error: "
            + str(e)
            + "\nIt may be necessary to set a default Aliyun project by running "
            '"gcloud config set project your-project"'
        )
    return o
