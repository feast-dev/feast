import datetime

import numpy as np
import pandas as pd
from feast import Entity, Feature, FeatureSet, ValueType
from pytz import utc

"""

Fraud features: customer counts for different windows of time (15M throughout day):
- FR1-7: int

"""
FRAUD_COUNTS_FEATURE_SET = FeatureSet(
    "fraud_count_features",
    entities=[Entity("customer_id", ValueType.INT64)],
    features=[
        Feature("window_count1", ValueType.INT64),
        Feature("window_count2", ValueType.INT64),
        Feature("window_count3", ValueType.INT64),
        Feature("window_count4", ValueType.INT64),
        Feature("window_count5", ValueType.INT64),
        Feature("window_count6", ValueType.INT64),
        Feature("window_count7", ValueType.INT64),
    ],
)


def create_fraud_counts_df(initial_customer_id=1, n=1000, dt=None):
    if dt is None:
        dt = datetime.datetime.now(datetime.timezone.utc)
    return pd.DataFrame(
        {
            "datetime": dt,
            "customer_id": list(range(initial_customer_id, initial_customer_id + n)),
            "window_count1": list(np.random.random_integers(10, size=n)),
            "window_count2": list(np.random.random_integers(20, size=n)),
            "window_count3": list(np.random.random_integers(50, size=n)),
            "window_count4": list(np.random.random_integers(100, size=n)),
            "window_count5": list(np.random.random_integers(1000, size=n)),
            "window_count6": list(np.random.random_integers(2000, size=n)),
            "window_count7": list(np.random.random_integers(5000, size=n)),
        }
    )
