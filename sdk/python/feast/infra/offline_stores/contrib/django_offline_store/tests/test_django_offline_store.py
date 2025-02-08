from datetime import datetime, timedelta

import pandas as pd
from django.db import models
from django.test import TestCase

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.contrib.django_offline_store.django_offline_store import (
    DjangoOfflineStore,
    DjangoOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.django_offline_store.django_source import (
    DjangoSource,
)
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64
from feast.value_type import ValueType


class DriverModel(models.Model):
    """Test Django model for driver features."""

    driver_id = models.IntegerField(primary_key=True)
    event_timestamp = models.DateTimeField()
    created = models.DateTimeField(auto_now_add=True)
    conv_rate = models.FloatField()
    acc_rate = models.FloatField()
    avg_daily_trips = models.IntegerField()

    class Meta:
        app_label = "test_app"
        db_table = "driver_stats"


class TestDjangoOfflineStore(TestCase):
    """Test cases for DjangoOfflineStore class."""

    def setUp(self):
        """Set up test cases."""
        self.model = DriverModel
        self.config = RepoConfig(
            registry="registry",
            project="project",
            provider="local",
            offline_store=DjangoOfflineStoreConfig(),
        )

        # Create test data
        self.start_date = datetime.now() - timedelta(days=7)
        self.end_date = datetime.now()
        self.driver_ids = [1, 2, 3]

        # Create test entities and feature views
        self.entity = Entity(
            name="driver_id",
            value_type=ValueType.INT64,
            description="driver id",
        )

        self.driver_stats_source = DjangoSource(
            model=self.model,
            timestamp_field="event_timestamp",
        )

        self.driver_stats_fv = FeatureView(
            name="driver_stats",
            entities=[self.entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="conv_rate", dtype=Float32),
                Field(name="acc_rate", dtype=Float32),
                Field(name="avg_daily_trips", dtype=Int64),
            ],
            source=self.driver_stats_source,
        )

    def test_pull_latest_from_table(self):
        """Test pulling latest features from table."""
        retrieval_job = DjangoOfflineStore.pull_latest_from_table_or_query(
            config=self.config,
            data_source=self.driver_stats_source,
            join_key_columns=["driver_id"],
            feature_name_columns=["conv_rate", "acc_rate", "avg_daily_trips"],
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
            start_date=self.start_date,
            end_date=self.end_date,
        )

        df = retrieval_job.to_df()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(
            {"driver_id", "conv_rate", "acc_rate", "avg_daily_trips"}.issubset(
                df.columns
            )
        )

    def test_get_historical_features(self):
        """Test getting historical features."""
        # Create entity DataFrame
        entity_df = pd.DataFrame.from_dict(
            {
                "driver_id": self.driver_ids,
                "event_timestamp": [self.end_date] * len(self.driver_ids),
            }
        )

        # Get historical features
        retrieval_job = DjangoOfflineStore.get_historical_features(
            config=self.config,
            feature_views=[self.driver_stats_fv],
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:acc_rate",
                "driver_stats:avg_daily_trips",
            ],
            entity_df=entity_df,
            registry=None,
            project="project",
        )

        df = retrieval_job.to_df()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(
            {
                "driver_id",
                "driver_stats__conv_rate",
                "driver_stats__acc_rate",
                "driver_stats__avg_daily_trips",
            }.issubset(df.columns)
        )
