from datetime import timedelta

from feast import (
    BigQuerySource,
    Entity,
    Feature,
    FeatureView,
    ValueType,
    FeatureService,
)

from .avg_conv_rate import avg_conv_rate_source

driver = Entity(
    name="driver_id",
    join_key="driver_id",
    value_type=ValueType.INT64,
    description="A driver",
    labels={"owner": "amanda@tecton.ai", "team": "hack week",},
)

driver_stats_source = BigQuerySource(
    name="driver_stats_with_string",
    table_ref="kf-feast.danny_test_dataset.driver_stats_with_string",
    event_timestamp_column="event_timestamp",
)

driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=timedelta(weeks=52),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    batch_source=driver_stats_source,
    tags={
        "team": "driver_performance",
        "date_added": "2022-02-10",
        "experiments": "experiment-B,experiment-C",
        "access_group": "hack-team@tecton.ai",
    },
)

avg_conv_rate_fv = FeatureView(
    name="avg_conv_rate",
    entities=["driver_id"],
    ttl=timedelta(weeks=52),
    features=[Feature(name="average_conv_rate_over_last_5", dtype=ValueType.FLOAT),],
    batch_source=avg_conv_rate_source,
    tags={
        "team": "driver_performance",
        "date_added": "2022-02-10",
        "experiments": "experiment-A,experiment-C",
        "access_group": "hack-team-2@tecton.ai",
    },
)

model_v0 = FeatureService(
    name="driver_ranking_v0",
    features=[avg_conv_rate_fv,],
    tags={"owner": "danny@tecton.ai", "stage": "dev"},
    description="Driver ranking model",
)

model_v1 = FeatureService(
    name="driver_ranking_v1",
    features=[driver_stats_fv[["acc_rate"]], avg_conv_rate_fv,],
    tags={"owner": "amanda@tecton.ai", "stage": "staging"},
    description="Driver ranking model",
)

model_v2 = FeatureService(
    name="driver_ranking_v2",
    features=[driver_stats_fv, avg_conv_rate_fv,],
    tags={"owner": "amanda@tecton.ai", "stage": "prod"},
    description="Driver ranking model",
)
