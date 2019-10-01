import pytest
import pandas as pd
from datetime import datetime
import pytz


GOOD = pd.DataFrame(
    {
        "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)],
        "entity_id": [1001, 1002, 1004],
        "feature_1": [0.2, 0.4, 0.5],
        "feature_2": ["string1", "string2", "string3"],
        "feature_3": [1, 2, 5],
    }
)

BAD_NO_ENTITY = pd.DataFrame(
    {
        "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)],
        "feature_1": [0.2, 0.4, 0.5],
        "feature_2": [0.3, 0.3, 0.34],
        "feature_3": [1, 2, 5],
    }
)

BAD_NO_FEATURES = pd.DataFrame(
    {
        "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)],
        "entity_id": [1001, 1002, 1004],
    }
)

BAD_NO_DATETIME = pd.DataFrame(
    {
        "entity_id": [1001, 1002, 1004],
        "feature_1": [0.2, 0.4, 0.5],
        "feature_2": [0.3, 0.3, 0.34],
        "feature_3": [1, 2, 5],
    }
)

BAD_INCORRECT_DATETIME_TYPE = pd.DataFrame(
    {
        "datetime": [1.23, 3.23, 1.23],
        "entity_id": [1001, 1002, 1004],
        "feature_1": [0.2, 0.4, 0.5],
        "feature_2": [0.3, 0.3, 0.34],
        "feature_3": [1, 2, 5],
    }
)
