from feast.repo_operations import is_valid_name


def test_is_valid_name():
    test_cases = [
        # Valid project name cases
        ("valid_name1", True),
        ("username_1234", True),
        ("valid123", True),
        ("1234567890123456", True),
        ("invalid_name_", True),
        ("12345678901234567", True),
        ("too_long_name_123", True),
        # Invalid project name cases
        ("_invalidName", False),
        ("invalid-Name", False),
        ("invalid name", False),
        ("invalid@name", False),
        ("invalid$name", False),
        ("__", False),
    ]

    for name, expected in test_cases:
        assert (
            is_valid_name(name) == expected
        ), f"Failed for project invalid name: {name}"
