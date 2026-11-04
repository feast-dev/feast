import warnings


def no_warnings(func):
    def wrapper_no_warnings(*args, **kwargs):
        with warnings.catch_warnings(record=True) as record:
            func(*args, **kwargs)

        if len(record) > 0:
            raise AssertionError(
                "Warnings were raised: " + ", ".join([str(w) for w in record])
            )

    return wrapper_no_warnings


def check_warnings(
    expected_warnings=None,  # List of warnings that MUST be present
    forbidden_warnings=None,  # List of warnings that MUST NOT be present
    match_type="contains",  # "exact", "contains", "regex"
    capture_all=True,  # Capture all warnings or just specific types
    fail_on_unexpected=False,  # Fail if unexpected warnings appear
    min_count=None,  # Minimum number of expected warnings
    max_count=None,  # Maximum number of expected warnings
):
    """
    Decorator to automatically capture and validate warnings in test methods.

    Args:
        expected_warnings: List of warning messages that MUST be present
        forbidden_warnings: List of warning messages that MUST NOT be present
        match_type: How to match warnings ("exact", "contains", "regex")
        capture_all: Whether to capture all warnings
        fail_on_unexpected: Whether to fail if unexpected warnings appear
        min_count: Minimum number of warnings expected
        max_count: Maximum number of warnings expected
    """

    def decorator(test_func):
        def wrapper(*args, **kwargs):
            # Setup warning capture
            with warnings.catch_warnings(record=True) as warning_list:
                warnings.simplefilter("always")

                # Execute the test function
                result = test_func(*args, **kwargs)

                # Convert warnings to string messages
                captured_messages = [str(w.message) for w in warning_list]

                # Validate expected warnings are present
                if expected_warnings:
                    for expected_warning in expected_warnings:
                        if not _warning_matches(
                            expected_warning, captured_messages, match_type
                        ):
                            raise AssertionError(
                                f"Expected warning '{expected_warning}' not found. "
                                f"Captured warnings: {captured_messages}"
                            )

                # Validate forbidden warnings are NOT present
                if forbidden_warnings:
                    for forbidden_warning in forbidden_warnings:
                        if _warning_matches(
                            forbidden_warning, captured_messages, match_type
                        ):
                            raise AssertionError(
                                f"Forbidden warning '{forbidden_warning}' was found. "
                                f"Captured warnings: {captured_messages}"
                            )

                # Validate warning count constraints
                if min_count is not None and len(warning_list) < min_count:
                    raise AssertionError(
                        f"Expected at least {min_count} warnings, got {len(warning_list)}"
                    )

                if max_count is not None and len(warning_list) > max_count:
                    raise AssertionError(
                        f"Expected at most {max_count} warnings, got {len(warning_list)}"
                    )

                # Validate no unexpected warnings (if enabled)
                if fail_on_unexpected and expected_warnings:
                    all_expected = expected_warnings + (forbidden_warnings or [])
                    for message in captured_messages:
                        if not any(
                            _warning_matches(exp, [message], match_type)
                            for exp in all_expected
                        ):
                            raise AssertionError(
                                f"Unexpected warning found: '{message}'"
                            )

                return result

        return wrapper

    return decorator


def _warning_matches(pattern, messages, match_type):
    """Helper function to check if pattern matches any message"""
    for message in messages:
        if match_type == "exact":
            if pattern == message:
                return True
        elif match_type == "contains":
            if pattern in message:
                return True
        elif match_type == "regex":
            import re

            if re.search(pattern, message):
                return True
    return False
