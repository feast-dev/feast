"""
Tests for skip_feature_view_validation parameter in FeatureStore.apply() and FeatureStore.plan()

This feature allows users to skip Feature View validation when the validation system
is being overly strict. This is particularly important for:
- Feature transformations that go through validation (e.g., _construct_random_input in ODFVs)
- Cases where the type/validation system is being too restrictive

Users should be encouraged to report issues on GitHub when they need to use this flag.

Also tests skip_validation parameter in push() and related methods to handle
On-Demand Feature Views with UDFs that reference modules not available in the
current environment.
"""

import inspect

import dill
import pandas as pd

from feast.feature_store import FeatureStore
from feast.on_demand_feature_view import PandasTransformation, PythonTransformation
from feast.protos.feast.core.Transformation_pb2 import UserDefinedFunctionV2 as UserDefinedFunctionProto


def test_apply_has_skip_feature_view_validation_parameter():
    """Test that FeatureStore.apply() method has skip_feature_view_validation parameter"""
    # Get the signature of the apply method
    sig = inspect.signature(FeatureStore.apply)

    # Check that skip_feature_view_validation parameter exists
    assert "skip_feature_view_validation" in sig.parameters

    # Check that it has a default value of False
    param = sig.parameters["skip_feature_view_validation"]
    assert param.default is False

    # Check that it's a boolean type hint (if type hints are present)
    if param.annotation != inspect.Parameter.empty:
        assert param.annotation == bool


def test_plan_has_skip_feature_view_validation_parameter():
    """Test that FeatureStore.plan() method has skip_feature_view_validation parameter"""
    # Get the signature of the plan method
    sig = inspect.signature(FeatureStore.plan)

    # Check that skip_feature_view_validation parameter exists
    assert "skip_feature_view_validation" in sig.parameters

    # Check that it has a default value of False
    param = sig.parameters["skip_feature_view_validation"]
    assert param.default is False

    # Check that it's a boolean type hint (if type hints are present)
    if param.annotation != inspect.Parameter.empty:
        assert param.annotation == bool


def test_push_has_skip_validation_parameter():
    """Test that FeatureStore.push() method has skip_validation parameter"""
    # Get the signature of the push method
    sig = inspect.signature(FeatureStore.push)

    # Check that skip_validation parameter exists
    assert "skip_validation" in sig.parameters

    # Check that it has a default value of False
    param = sig.parameters["skip_validation"]
    assert param.default is False

    # Check that it's a boolean type hint (if type hints are present)
    if param.annotation != inspect.Parameter.empty:
        assert param.annotation == bool


def test_push_async_has_skip_validation_parameter():
    """Test that FeatureStore.push_async() method has skip_validation parameter"""
    # Get the signature of the push_async method
    sig = inspect.signature(FeatureStore.push_async)

    # Check that skip_validation parameter exists
    assert "skip_validation" in sig.parameters

    # Check that it has a default value of False
    param = sig.parameters["skip_validation"]
    assert param.default is False

    # Check that it's a boolean type hint (if type hints are present)
    if param.annotation != inspect.Parameter.empty:
        assert param.annotation == bool


def test_pandas_transformation_from_proto_with_skip_udf():
    """Test that PandasTransformation.from_proto works with skip_udf=True."""
    
    # Create a UDF that would reference a non-existent module
    def udf_with_missing_module(df: pd.DataFrame) -> pd.DataFrame:
        # This would normally fail if a module is missing during deserialization
        import nonexistent_module  # noqa: F401
        return df
    
    # Serialize the UDF
    serialized_udf = dill.dumps(udf_with_missing_module)
    udf_string = "import nonexistent_module\ndef udf(df): return df"
    
    # Create proto
    udf_proto = UserDefinedFunctionProto(
        name="test_udf",
        body=serialized_udf,
        body_text=udf_string,
    )
    
    # Test that skip_udf=True doesn't try to deserialize the UDF
    # This would normally fail with ModuleNotFoundError
    transformation = PandasTransformation.from_proto(udf_proto, skip_udf=True)
    
    # Should get a dummy transformation with identity function
    assert transformation is not None
    assert transformation.udf_string == udf_string
    
    # The dummy UDF should be callable and act as identity
    test_df = pd.DataFrame({"col1": [1, 2, 3]})
    result = transformation.udf(test_df)
    assert result.equals(test_df)


def test_python_transformation_from_proto_with_skip_udf():
    """Test that PythonTransformation.from_proto works with skip_udf=True."""
    
    # Create a UDF that would reference a non-existent module
    def udf_with_missing_module(features_dict):
        # This would normally fail if a module is missing during deserialization
        import nonexistent_module  # noqa: F401
        return features_dict
    
    # Serialize the UDF
    serialized_udf = dill.dumps(udf_with_missing_module)
    udf_string = "import nonexistent_module\ndef udf(d): return d"
    
    # Create proto
    udf_proto = UserDefinedFunctionProto(
        name="test_udf",
        body=serialized_udf,
        body_text=udf_string,
    )
    
    # Test that skip_udf=True doesn't try to deserialize the UDF
    # This would normally fail with ModuleNotFoundError
    transformation = PythonTransformation.from_proto(udf_proto, skip_udf=True)
    
    # Should get a dummy transformation with identity function
    assert transformation is not None
    assert transformation.udf_string == udf_string
    
    # The dummy UDF should be callable and act as identity
    test_dict = {"col1": 1}
    result = transformation.udf(test_dict)
    assert result == test_dict


def test_skip_feature_view_validation_use_case_documentation():
    """
    Documentation test: This test documents the key use case for skip_feature_view_validation.

    The skip_feature_view_validation flag is particularly important for On-Demand Feature Views (ODFVs)
    that use feature transformations. During the apply() process, ODFVs call infer_features()
    which internally uses _construct_random_input() to validate the transformation.

    Sometimes this validation can be overly strict or fail for complex transformations.
    In such cases, users can use skip_feature_view_validation=True to bypass this check.

    Example use case from the issue:
    - User has an ODFV with a complex transformation
    - The _construct_random_input validation fails or is too restrictive
    - User can now call: fs.apply([odfv], skip_feature_view_validation=True)
    - The ODFV is registered without going through the validation

    Note: Users should be encouraged to report such cases on GitHub so the Feast team
    can improve the validation system.
    """
    pass  # This is a documentation test


def test_skip_validation_use_case_documentation():
    """
    Documentation test: This test documents the key use case for skip_validation in push().

    The skip_validation flag in push() addresses the ModuleNotFoundError issue when:
    1. An OnDemandFeatureView with a UDF is defined in an environment with specific modules
    2. The UDF references functions, classes, or constants from those modules (e.g., 'training')
    3. feast.apply() is run to save the definition to the remote registry
    4. store.push() is called from a different environment without those modules

    Without skip_validation:
    - push() calls list_all_feature_views() which deserializes ODFVs
    - Deserialization uses dill.loads() which fails if referenced modules are missing
    - Results in: ModuleNotFoundError: No module named 'training'

    With skip_validation=True:
    - push() calls list_all_feature_views(skip_validation=True)
    - ODFVs are loaded with dummy UDFs (identity functions)
    - No deserialization of the actual UDF happens
    - push() can proceed successfully

    Example usage:
        store.push("my_push_source", df, skip_validation=True)

    This is particularly useful in production environments where:
    - Data ingestion services don't need the training/modeling code
    - The UDF logic isn't needed during push operations
    - Different teams manage training vs. serving infrastructure
    """
    pass  # This is a documentation test

