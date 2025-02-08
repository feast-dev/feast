from django.db import models
from django.test import TestCase

from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.django_offline_store.django_source import (
    DjangoSource,
    SavedDatasetDjangoStorage,
)
from feast.repo_config import RepoConfig


class TestModel(models.Model):
    """Test Django model for feature source testing."""
    id = models.AutoField(primary_key=True)
    event_timestamp = models.DateTimeField()
    created = models.DateTimeField(auto_now_add=True)
    value = models.FloatField()

    class Meta:
        app_label = 'test_app'
        db_table = 'test_table'


class TestDjangoSource(TestCase):
    """Test cases for DjangoSource class."""

    def setUp(self):
        """Set up test cases."""
        self.model = TestModel
        self.config = RepoConfig(
            registry="registry",
            project="project",
            provider="local",
        )

    def test_init_with_model(self):
        """Test initialization with Django model."""
        source = DjangoSource(
            model=self.model,
            timestamp_field="event_timestamp",
        )
        self.assertEqual(source.timestamp_field, "event_timestamp")
        self.assertEqual(source._django_options._model, self.model)
        self.assertEqual(source.name, self.model._meta.db_table)

    def test_init_with_field_mapping(self):
        """Test initialization with field mapping."""
        field_mapping = {"value": "feature_value"}
        source = DjangoSource(
            model=self.model,
            field_mapping=field_mapping,
        )
        self.assertEqual(source.field_mapping, field_mapping)

    def test_get_table_column_names_and_types(self):
        """Test getting column names and types."""
        source = DjangoSource(model=self.model)
        columns = dict(source.get_table_column_names_and_types(self.config))
        
        self.assertIn("id", columns)
        self.assertIn("event_timestamp", columns)
        self.assertIn("created", columns)
        self.assertIn("value", columns)
        
        self.assertEqual(columns["id"], "AutoField")
        self.assertEqual(columns["event_timestamp"], "DateTimeField")
        self.assertEqual(columns["value"], "FloatField")

    def test_get_table_query_string(self):
        """Test getting table query string."""
        source = DjangoSource(model=self.model)
        self.assertEqual(source.get_table_query_string(), "test_table")

    def test_validate_timestamp_field(self):
        """Test timestamp field validation."""
        source = DjangoSource(
            model=self.model,
            timestamp_field="event_timestamp",
        )
        source.validate(self.config)  # Should not raise

        source = DjangoSource(
            model=self.model,
            timestamp_field="nonexistent_field",
        )
        with self.assertRaises(AssertionError):
            source.validate(self.config)

    def test_saved_dataset_storage(self):
        """Test saved dataset storage functionality."""
        storage = SavedDatasetDjangoStorage(model=self.model)
        
        # Test conversion to data source
        source = storage.to_data_source()
        self.assertIsInstance(source, DataSource)
        self.assertEqual(source.name, self.model._meta.db_table)

        # Test proto serialization
        proto = storage.to_proto()
        restored = SavedDatasetDjangoStorage.from_proto(proto)
        self.assertEqual(
            restored.django_options._model._meta.db_table,
            self.model._meta.db_table
        )
