from django.db import models
from django.test import TestCase

from feast.infra.offline_stores.contrib.django_offline_store.django_source import DjangoSource
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


class ComplexModel(models.Model):
    """Test model with various field types for schema inference testing."""
    id = models.AutoField(primary_key=True)
    char_field = models.CharField(max_length=100)
    text_field = models.TextField()
    integer_field = models.IntegerField()
    float_field = models.FloatField()
    decimal_field = models.DecimalField(max_digits=10, decimal_places=2)
    boolean_field = models.BooleanField()
    date_field = models.DateField()
    datetime_field = models.DateTimeField()
    timestamp_field = models.DateTimeField(auto_now_add=True)
    json_field = models.JSONField()
    binary_field = models.BinaryField()

    class Meta:
        app_label = 'test_app'
        db_table = 'complex_model'


class TestSchemaInference(TestCase):
    """Test cases for schema inference from Django models."""

    def setUp(self):
        """Set up test cases."""
        self.model = ComplexModel
        self.config = RepoConfig(
            registry="registry",
            project="project",
            provider="local",
        )

    def test_field_type_inference(self):
        """Test field type inference from Django model fields."""
        source = DjangoSource(model=self.model)
        columns = dict(source.get_table_column_names_and_types(self.config))

        # Verify field types are correctly inferred
        self.assertEqual(columns["id"], "AutoField")
        self.assertEqual(columns["char_field"], "CharField")
        self.assertEqual(columns["text_field"], "TextField")
        self.assertEqual(columns["integer_field"], "IntegerField")
        self.assertEqual(columns["float_field"], "FloatField")
        self.assertEqual(columns["decimal_field"], "DecimalField")
        self.assertEqual(columns["boolean_field"], "BooleanField")
        self.assertEqual(columns["date_field"], "DateField")
        self.assertEqual(columns["datetime_field"], "DateTimeField")
        self.assertEqual(columns["timestamp_field"], "DateTimeField")
        self.assertEqual(columns["json_field"], "JSONField")
        self.assertEqual(columns["binary_field"], "BinaryField")

    def test_feast_value_type_mapping(self):
        """Test mapping Django field types to Feast value types."""
        type_map = DjangoSource.source_datatype_to_feast_value_type()

        # Test mapping of Django field types to Feast value types
        self.assertEqual(type_map("IntegerField"), ValueType.INT64)
        self.assertEqual(type_map("FloatField"), ValueType.FLOAT)
        self.assertEqual(type_map("DecimalField"), ValueType.FLOAT)
        self.assertEqual(type_map("BooleanField"), ValueType.BOOL)
        self.assertEqual(type_map("CharField"), ValueType.STRING)
        self.assertEqual(type_map("TextField"), ValueType.STRING)
        self.assertEqual(type_map("DateTimeField"), ValueType.UNIX_TIMESTAMP)
        self.assertEqual(type_map("DateField"), ValueType.UNIX_TIMESTAMP)
        self.assertEqual(type_map("JSONField"), ValueType.STRING)
        self.assertEqual(type_map("BinaryField"), ValueType.BYTES)

    def test_model_field_validation(self):
        """Test validation of model fields."""
        source = DjangoSource(
            model=self.model,
            timestamp_field="timestamp_field",
        )
        source.validate(self.config)  # Should not raise

        source = DjangoSource(
            model=self.model,
            timestamp_field="nonexistent_field",
        )
        with self.assertRaises(AssertionError):
            source.validate(self.config)

    def test_table_query_string(self):
        """Test getting table query string."""
        source = DjangoSource(model=self.model)
        self.assertEqual(source.get_table_query_string(), "complex_model")
