from datetime import timedelta

import pandas as pd
import numpy as np

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.data_source import RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.permissions.action import AuthzedAction, READ
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.types import Bool, Int64, String, Float32, Array

zipcode = Entity(
    name="zipcode",
    description="A zipcode",
    tags={
        "owner": "danny@feast.ai",
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
        "access_group": "feast-team@feast.ai",
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
        "access_group": "feast-team@feast.ai",
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
        "access_group": "feast-team@feast.ai",
    },
    online=True,
)

dob_ssn = Entity(
    name="dob_ssn",
    description="Date of birth and last four digits of social security number",
    tags={
        "owner": "tony@feast.ai",
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
        "access_group": "feast-team@feast.ai",
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
    tags={"owner": "tony@feast.ai", "stage": "staging"},
    description="Credit scoring model",
)

model_v2 = FeatureService(
    name="credit_score_v2",
    features=[
        credit_history[["mortgage_due", "credit_card_due", "missed_payments_1y"]],
        zipcode_features,
    ],
    tags={"owner": "tony@feast.ai", "stage": "prod"},
    description="Credit scoring model",
)

model_v3 = FeatureService(
    name="credit_score_v3",
    features=[
        credit_history[["mortgage_due", "credit_card_due", "missed_payments_1y"]],
        zipcode_features,
        transaction_gt_last_credit_card_due,
    ],
    tags={"owner": "tony@feast.ai", "stage": "dev"},
    description="Credit scoring model",
)

zipcode_model = FeatureService(
    name="zipcode_model",
    features=[
        zipcode_features,
    ],
    tags={"owner": "amanda@feast.ai", "stage": "dev"},
    description="Location model",
)

zipcode_model_v2 = FeatureService(
    name="zipcode_model_v2",
    features=[
        zipcode_money_features,
    ],
    tags={"owner": "amanda@feast.ai", "stage": "dev"},
    description="Location model",
)

zipcode_features_permission = Permission(
    name="zipcode-features-reader",
    types=[FeatureView],
    name_patterns=["zipcode_features"],
    policy=RoleBasedPolicy(roles=["analyst", "data_scientist"]),
    actions=[AuthzedAction.DESCRIBE, *READ],
)

zipcode_source_permission = Permission(
    name="zipcode-source-writer",
    types=[FileSource],
    name_patterns=["zipcode"],
    policy=RoleBasedPolicy(roles=["admin", "data_engineer"]),
    actions=[AuthzedAction.CREATE, AuthzedAction.UPDATE, AuthzedAction.WRITE_OFFLINE],
)

model_v1_permission = Permission(
    name="credit-score-v1-reader",
    types=[FeatureService],
    name_patterns=["credit_score_v1"],
    policy=RoleBasedPolicy(roles=["model_user", "data_scientist"]),
    actions=[AuthzedAction.DESCRIBE, AuthzedAction.READ_ONLINE],
)

risky_features_permission = Permission(
    name="risky-features-reader",
    types=[FeatureView, FeatureService],
    required_tags={"stage": "prod"},
    policy=RoleBasedPolicy(roles=["trusted_analyst"]),
    actions=[AuthzedAction.READ_OFFLINE],
)

document = Entity(
    name="document_id",
    description="Document identifier for RAG system",
    tags={
        "owner": "nlp_team@feast.ai",
        "team": "rag",
    },
)

document_source = FileSource(
    name="document_embeddings",
    path="data/document_embeddings.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

document_metadata_source = FileSource(
    name="document_metadata",
    path="data/document_metadata.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

document_embeddings_view = FeatureView(
    name="document_embeddings",
    entities=[document],
    ttl=timedelta(days=365),
    schema=[
        Field(name="embedding", dtype=Array(Float32, 768)),
        Field(name="document_id", dtype=String),
    ],
    source=document_source,
    tags={
        "date_added": "2025-05-04",
        "model": "sentence-transformer",
        "access_group": "nlp-team@feast.ai",
        "stage": "prod",
    },
    online=True,
)

document_metadata_view = FeatureView(
    name="document_metadata",
    entities=[document],
    ttl=timedelta(days=365),
    schema=[
        Field(name="title", dtype=String),
        Field(name="content", dtype=String),
        Field(name="source", dtype=String),
        Field(name="author", dtype=String),
        Field(name="publish_date", dtype=String),
        Field(name="document_id", dtype=String),
    ],
    source=document_metadata_source,
    tags={
        "date_added": "2025-05-04",
        "access_group": "nlp-team@feast.ai",
        "stage": "prod",
    },
    online=True,
)

# Define a request data source for query embeddings
query_request = RequestSource(
    name="query",
    schema=[
        Field(name="query_embedding", dtype=Array(Float32, 768)),
    ],
)

# Define an on-demand feature view for similarity calculation
@on_demand_feature_view(
    sources=[document_embeddings_view, query_request],
    schema=[
        Field(name="similarity_score", dtype=Float32),
    ],
)
def document_similarity(inputs: pd.DataFrame) -> pd.DataFrame:
    """Calculate cosine similarity between query and document embeddings."""
    df = pd.DataFrame()
    df["similarity_score"] = 0.95  # Placeholder value
    return df

rag_model = FeatureService(
    name="rag_retriever",
    features=[
        document_embeddings_view,
        document_metadata_view,
        document_similarity,
    ],
    tags={"owner": "nlp_team@feast.ai", "stage": "prod"},
    description="Retrieval Augmented Generation model",
)

document_embeddings_permission = Permission(
    name="document-embeddings-reader",
    types=[FeatureView],
    name_patterns=["document_embeddings"],
    policy=RoleBasedPolicy(roles=["ml_engineer", "data_scientist"]),
    actions=[AuthzedAction.DESCRIBE, *READ],
)

document_metadata_permission = Permission(
    name="document-metadata-reader",
    types=[FeatureView],
    name_patterns=["document_metadata"],
    policy=RoleBasedPolicy(roles=["ml_engineer", "content_manager"]),
    actions=[AuthzedAction.DESCRIBE, *READ],
)

rag_model_permission = Permission(
    name="rag-model-user",
    types=[FeatureService],
    name_patterns=["rag_retriever"],
    policy=RoleBasedPolicy(roles=["ml_engineer", "app_developer"]),
    actions=[AuthzedAction.DESCRIBE, AuthzedAction.READ_ONLINE],
)
