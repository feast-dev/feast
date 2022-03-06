from datetime import timedelta

from feast import (
    Entity,
    Feature,
    FeatureService,
    FeatureView,
    FileSource,
    ValueType,
)
from feast.data_source import RequestDataSource
from feast.request_feature_view import RequestFeatureView
from feast.on_demand_feature_view import on_demand_feature_view
import pandas as pd

zipcode = Entity(
    name="zipcode",
    value_type=ValueType.INT64,
    description="A zipcode",
    labels={"owner": "danny@tecton.ai", "team": "hack week",},
)

zipcode_source = FileSource(
    name="zipcode",
    path="data/zipcode_table.parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)

zipcode_features = FeatureView(
    name="zipcode_features",
    entities=["zipcode"],
    ttl=timedelta(days=3650),
    features=[
        Feature(name="city", dtype=ValueType.STRING),
        Feature(name="state", dtype=ValueType.STRING),
        Feature(name="location_type", dtype=ValueType.STRING),
        Feature(name="tax_returns_filed", dtype=ValueType.INT64),
        Feature(name="population", dtype=ValueType.INT64),
        Feature(name="total_wages", dtype=ValueType.INT64),
    ],
    batch_source=zipcode_source,
    tags={
        "date_added": "2022-02-7",
        "experiments": "experiment-A,experiment-B,experiment-C",
        "access_group": "feast-team@tecton.ai",
    },
    online=True,
)

zipcode_features = FeatureView(
    name="zipcode_features",
    entities=["zipcode"],
    ttl=timedelta(days=3650),
    features=[
        Feature(name="city", dtype=ValueType.STRING),
        Feature(name="state", dtype=ValueType.STRING),
        Feature(name="location_type", dtype=ValueType.STRING),
        Feature(name="tax_returns_filed", dtype=ValueType.INT64),
        Feature(name="population", dtype=ValueType.INT64),
        Feature(name="total_wages", dtype=ValueType.INT64),
    ],
    batch_source=zipcode_source,
    tags={
        "date_added": "2022-02-7",
        "experiments": "experiment-A,experiment-B,experiment-C",
        "access_group": "feast-team@tecton.ai",
    },
    online=True,
)

zipcode_money_features = FeatureView(
    name="zipcode_money_features",
    entities=["zipcode"],
    ttl=timedelta(days=3650),
    features=[
        Feature(name="tax_returns_filed", dtype=ValueType.INT64),
        Feature(name="total_wages", dtype=ValueType.INT64),
    ],
    batch_source=zipcode_source,
    tags={
        "date_added": "2022-02-7",
        "experiments": "experiment-A,experiment-B,experiment-C",
        "access_group": "feast-team@tecton.ai",
    },
    online=True,
)

dob_ssn = Entity(
    name="dob_ssn",
    value_type=ValueType.STRING,
    description="Date of birth and last four digits of social security number",
    labels={"owner": "tony@tecton.ai", "team": "hack week",},
)

credit_history_source = FileSource(
    name="credit_history",
    path="data/credit_history.parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)

credit_history = FeatureView(
    name="credit_history",
    entities=["dob_ssn"],
    ttl=timedelta(days=9000),
    features=[
        Feature(name="credit_card_due", dtype=ValueType.INT64),
        Feature(name="mortgage_due", dtype=ValueType.INT64),
        Feature(name="student_loan_due", dtype=ValueType.INT64),
        Feature(name="vehicle_loan_due", dtype=ValueType.INT64),
        Feature(name="hard_pulls", dtype=ValueType.INT64),
        Feature(name="missed_payments_2y", dtype=ValueType.INT64),
        Feature(name="missed_payments_1y", dtype=ValueType.INT64),
        Feature(name="missed_payments_6m", dtype=ValueType.INT64),
        Feature(name="bankruptcies", dtype=ValueType.INT64),
    ],
    batch_source=credit_history_source,
    tags={
        "date_added": "2022-02-6",
        "experiments": "experiment-A",
        "access_group": "feast-team@tecton.ai",
    },
    online=True,
)

# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestDataSource(
    name="transaction", schema={"transaction_amt": ValueType.INT64},
)

# Define an on demand feature view which can generate new features based on
# existing feature views and RequestDataSource features
@on_demand_feature_view(
    inputs={"credit_history": credit_history, "transaction": input_request,},
    features=[
        Feature(name="transaction_gt_last_credit_card_due", dtype=ValueType.BOOL),
    ],
)
def transaction_gt_last_credit_card_due(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["transaction_gt_last_credit_card_due"] = (
        inputs["transaction_amt"] > inputs["credit_card_due"]
    )
    return df


# Define request feature view
transaction_request_fv = RequestFeatureView(
    name="transaction_request_fv", request_data_source=input_request,
)

model_v1 = FeatureService(
    name="credit_score_v1",
    features=[
        credit_history[["mortgage_due", "credit_card_due", "missed_payments_1y"]],
        zipcode_features,
    ],
    tags={"owner": "tony@tecton.ai", "stage": "staging"},
    description="Credit scoring model",
)

model_v2 = FeatureService(
    name="credit_score_v2",
    features=[
        credit_history[["mortgage_due", "credit_card_due", "missed_payments_1y"]],
        zipcode_features,
        transaction_request_fv,
    ],
    tags={"owner": "tony@tecton.ai", "stage": "prod"},
    description="Credit scoring model",
)

model_v3 = FeatureService(
    name="credit_score_v3",
    features=[
        credit_history[["mortgage_due", "credit_card_due", "missed_payments_1y"]],
        zipcode_features,
        transaction_gt_last_credit_card_due,
    ],
    tags={"owner": "tony@tecton.ai", "stage": "dev"},
    description="Credit scoring model",
)

zipcode_model = FeatureService(
    name="zipcode_model",
    features=[zipcode_features,],
    tags={"owner": "amanda@tecton.ai", "stage": "dev"},
    description="Location model",
)

zipcode_model_v2 = FeatureService(
    name="zipcode_model_v2",
    features=[zipcode_money_features,],
    tags={"owner": "amanda@tecton.ai", "stage": "dev"},
    description="Location model",
)
