from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd
from django.test import TestCase, override_settings
from feast import Entity, FeatureStore, FeatureView, Field, RepoConfig
from feast.infra.offline_stores.contrib.django_offline_store.django_source import DjangoSource
from feast.types import Float32, Int64
from feast.value_type import ValueType

from .test_app.models import Driver, Order

# Configure Django settings for testing
test_settings = {
    'DATABASES': {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',
        }
    },
    'INSTALLED_APPS': [
        'feast.infra.offline_stores.contrib.django_offline_store.tests.test_app',
    ],
}

@override_settings(**test_settings)
class TestDjangoIntegration(TestCase):
    """Integration tests for Django offline store."""

    def setUp(self):
        """Set up test data and feature store."""
        # Create test data
        self.driver = Driver.objects.create(
            driver_id=1,
            event_timestamp=datetime.now(),
            conv_rate=0.85,
            acc_rate=0.92,
            avg_daily_trips=15,
        )
        self.order = Order.objects.create(
            order_id=1,
            driver=self.driver,
            event_timestamp=datetime.now(),
            amount=Decimal('25.50'),
            status='completed',
        )

        # Create feature store
        self.store = FeatureStore(
            config=RepoConfig(
                registry="registry",
                project="project",
                provider="local",
                offline_store={
                    "type": "django",
                },
            )
        )

        # Create entities
        self.driver_entity = Entity(
            name="driver_id",
            value_type=ValueType.INT64,
            description="driver id",
        )

        # Create feature views
        self.driver_stats_source = DjangoSource(
            model=Driver,
            timestamp_field="event_timestamp",
        )

        self.driver_stats_fv = FeatureView(
            name="driver_stats",
            entities=[self.driver_entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="conv_rate", dtype=Float32),
                Field(name="acc_rate", dtype=Float32),
                Field(name="avg_daily_trips", dtype=Int64),
            ],
            source=self.driver_stats_source,
        )

    def test_sync_feature_retrieval(self):
        """Test synchronous feature retrieval."""
        # Apply feature view
        self.store.apply([self.driver_entity, self.driver_stats_fv])

        # Create entity DataFrame
        entity_df = pd.DataFrame.from_dict({
            "driver_id": [1],
            "event_timestamp": [datetime.now()],
        })

        # Get historical features
        feature_data = self.store.get_historical_features(
            entity_df=entity_df,
            features=[
                "driver_stats:conv_rate",
                "driver_stats:acc_rate",
                "driver_stats:avg_daily_trips",
            ],
        ).to_df()

        # Verify feature values
        self.assertEqual(feature_data.shape[0], 1)
        self.assertEqual(feature_data["driver_stats__conv_rate"].iloc[0], 0.85)
        self.assertEqual(feature_data["driver_stats__acc_rate"].iloc[0], 0.92)
        self.assertEqual(feature_data["driver_stats__avg_daily_trips"].iloc[0], 15)

    def test_multiple_feature_retrieval(self):
        """Test retrieving multiple features."""
        # Apply feature view
        self.store.apply([self.driver_entity, self.driver_stats_fv])

        # Create entity DataFrame with multiple drivers
        entity_df = pd.DataFrame.from_dict({
            "driver_id": [1, 1],
            "event_timestamp": [
                datetime.now(),
                datetime.now() - timedelta(days=1),
            ],
        })

        # Get historical features
        feature_data = self.store.get_historical_features(
            entity_df=entity_df,
            features=[
                "driver_stats:conv_rate",
                "driver_stats:acc_rate",
                "driver_stats:avg_daily_trips",
            ],
        ).to_df()

        # Verify feature values
        self.assertEqual(feature_data.shape[0], 2)
        self.assertEqual(feature_data["driver_stats__conv_rate"].iloc[0], 0.85)
        self.assertEqual(feature_data["driver_stats__acc_rate"].iloc[0], 0.92)
        self.assertEqual(feature_data["driver_stats__avg_daily_trips"].iloc[0], 15)
