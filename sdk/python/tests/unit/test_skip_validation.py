"""
Tests for skip_feature_view_validation parameter in FeatureStore.apply() and FeatureStore.plan()

This feature allows users to skip Feature View validation when the validation system
is being overly strict. This is particularly important for:
- Feature transformations that go through validation (e.g., _construct_random_input in ODFVs)
- Cases where the type/validation system is being too restrictive

Users should be encouraged to report issues on GitHub when they need to use this flag.
"""

import inspect

from feast.feature_store import FeatureStore


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
