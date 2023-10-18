from datetime import timedelta

import pandas as pd

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.data_source import RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Bool, Int64, String

zipcode = Entity(
    name="zipcode",
    description="A zipcode",
    tags={
        "owner": "danny@tecton.ai",
        "team": "hack week",
    },
)

zipcode_source = FileSource(
    name="zipcode",
    path="data/zipcode_table.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

zipcode_features = FeatureView(
    name="zipcode_features",
    entities=[zipcode],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="city", dtype=String),
        Field(name="state", dtype=String),
        Field(name="location_type", dtype=String),
        Field(name="tax_returns_filed", dtype=Int64),
        Field(name="population", dtype=Int64),
        Field(name="total_wages", dtype=Int64),
        Field(name="zipcode", dtype=Int64),
    ],
    source=zipcode_source,
    tags={
        "date_added": "2022-02-7",
        "experiments": "experiment-A,experiment-B,experiment-C",
        "access_group": "feast-team@tecton.ai",
    },
    online=True,
)

zipcode_features = FeatureView(
    name="zipcode_features",
    entities=[zipcode],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="city", dtype=String),
        Field(name="state", dtype=String),
        Field(name="location_type", dtype=String),
        Field(name="tax_returns_filed", dtype=Int64),
        Field(name="population", dtype=Int64),
        Field(name="total_wages", dtype=Int64),
        Field(name="zipcode", dtype=Int64),
    ],
    source=zipcode_source,
    tags={
        "date_added": "2022-02-7",
        "experiments": "experiment-A,experiment-B,experiment-C",
        "access_group": "feast-team@tecton.ai",
    },
    online=True,
)

zipcode_money_features = FeatureView(
    name="zipcode_money_features",
    entities=[zipcode],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="tax_returns_filed", dtype=Int64),
        Field(name="total_wages", dtype=Int64),
        Field(name="zipcode", dtype=Int64),
    ],
    source=zipcode_source,
    tags={
        "date_added": "2022-02-7",
        "experiments": "experiment-A,experiment-B,experiment-C",
        "access_group": "feast-team@tecton.ai",
    },
    online=True,
)

dob_ssn = Entity(
    name="dob_ssn",
    description="Date of birth and last four digits of social security number",
    tags={
        "owner": "tony@tecton.ai",
        "team": "hack week",
    },
)

credit_history_source = FileSource(
    name="credit_history",
    path="data/credit_history.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

credit_history = FeatureView(
    name="credit_history",
    entities=[dob_ssn],
    ttl=timedelta(days=9000),
    schema=[
        Field(name="credit_card_due", dtype=Int64),
        Field(name="mortgage_due", dtype=Int64),
        Field(name="student_loan_due", dtype=Int64),
        Field(name="vehicle_loan_due", dtype=Int64),
        Field(name="hard_pulls", dtype=Int64),
        Field(name="missed_payments_2y", dtype=Int64),
        Field(name="missed_payments_1y", dtype=Int64),
        Field(name="missed_payments_6m", dtype=Int64),
        Field(name="bankruptcies", dtype=Int64),
        Field(name="dob_ssn", dtype=String),
    ],
    source=credit_history_source,
    tags={
        "date_added": "2022-02-6",
        "experiments": "experiment-A",
        "access_group": "feast-team@tecton.ai",
    },
    online=True,
)

# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestSource(
    name="transaction",
    schema=[
        Field(name="transaction_amt", dtype=Int64),
    ],
)

# Define an on demand feature view which can generate new features based on
# existing feature views and RequestSource features
@on_demand_feature_view(
    sources=[credit_history, input_request],
    schema=[
        Field(name="transaction_gt_last_credit_card_due", dtype=Bool),
    ],
)
def transaction_gt_last_credit_card_due(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["transaction_gt_last_credit_card_due"] = (
        inputs["transaction_amt"] > inputs["credit_card_due"]
    )
    return df


model_v1 = FeatureService(
    name="credit_score_v1",
    features=[
        credit_history[["credit_card_due", "missed_payments_1y"]],
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
    features=[
        zipcode_features,
    ],
    tags={"owner": "amanda@tecton.ai", "stage": "dev"},
    description="Location model",
)

zipcode_model_v2 = FeatureService(
    name="zipcode_model_v2",
    features=[
        zipcode_money_features,
    ],
    tags={"owner": "amanda@tecton.ai", "stage": "dev"},
    description="Location model",
)
