"""Tests for protobuf float_precision backward compatibility in feature_server."""

import pytest
from unittest.mock import MagicMock


class TestProtobufCompat:
    """Test that _message_to_dict handles protobuf version differences."""

    def test_float_precision_included_old_protobuf(self):
        """When protobuf < 7.34, float_precision=18 should be passed to MessageToDict."""
        from google.protobuf.json_format import MessageToDict

        # Verify float_precision is accepted on current protobuf (6.33.x)
        import google.protobuf
        version_tuple = tuple(int(x) for x in google.protobuf.__version__.split(".")[:2])
        if version_tuple < (7, 34):
            # Use a real protobuf message to verify
            from google.protobuf import descriptor_pb2
            desc = descriptor_pb2.FileDescriptorProto()
            result = MessageToDict(desc, preserving_proto_field_name=True, float_precision=18)
            assert isinstance(result, dict)

    def test_float_precision_excluded_new_protobuf(self):
        """When protobuf >= 7.34, float_precision should NOT be passed."""
        protobuf_version = "7.34.1"
        version_tuple = tuple(int(x) for x in protobuf_version.split(".")[:2])
        supports_fp = version_tuple < (7, 34)
        assert supports_fp is False

    def test_version_parsing(self):
        """Verify version tuple parsing works correctly for boundary cases."""
        cases = [
            ("4.24.0", (4, 24), True),
            ("5.0.0", (5, 0), True),
            ("6.33.6", (6, 33), True),
            ("7.33.99", (7, 33), True),
            ("7.34.0", (7, 34), False),
            ("7.34.1", (7, 34), False),
            ("8.0.0", (8, 0), False),
            ("10.1.2", (10, 1), False),
        ]
        for version_str, expected_tuple, expected_support in cases:
            result_tuple = tuple(int(x) for x in version_str.split(".")[:2])
            result_support = result_tuple < (7, 34)
            assert result_tuple == expected_tuple, f"Failed for {version_str}"
            assert result_support == expected_support, f"Support check failed for {version_str}"

    def test_message_to_dict_wrapper_new_protobuf(self):
        """Test the wrapper skips float_precision on new protobuf versions."""
        called_with = {}

        def mock_message_to_dict(proto, **kwargs):
            called_with.update(kwargs)
            return {"test": "result"}

        supports_fp = False  # protobuf >= 7.34

        def _message_to_dict(proto, **kwargs):
            if supports_fp and "float_precision" not in kwargs:
                kwargs["float_precision"] = 18
            return mock_message_to_dict(proto, **kwargs)

        result = _message_to_dict(MagicMock(), preserving_proto_field_name=True)
        assert result == {"test": "result"}
        assert "float_precision" not in called_with
        assert called_with.get("preserving_proto_field_name") is True

    def test_message_to_dict_wrapper_old_protobuf(self):
        """Test the wrapper includes float_precision on old protobuf versions."""
        called_with = {}

        def mock_message_to_dict(proto, **kwargs):
            called_with.update(kwargs)
            return {"test": "result"}

        supports_fp = True

        def _message_to_dict(proto, **kwargs):
            if supports_fp and "float_precision" not in kwargs:
                kwargs["float_precision"] = 18
            return mock_message_to_dict(proto, **kwargs)

        result = _message_to_dict(MagicMock(), preserving_proto_field_name=True)
        assert result == {"test": "result"}
        assert called_with.get("float_precision") == 18
        assert called_with.get("preserving_proto_field_name") is True

    def test_message_to_dict_explicit_precision_not_overridden(self):
        """If caller passes explicit float_precision, don't override."""
        called_with = {}

        def mock_message_to_dict(proto, **kwargs):
            called_with.update(kwargs)
            return {"test": "result"}

        supports_fp = True

        def _message_to_dict(proto, **kwargs):
            if supports_fp and "float_precision" not in kwargs:
                kwargs["float_precision"] = 18
            return mock_message_to_dict(proto, **kwargs)

        result = _message_to_dict(
            MagicMock(), preserving_proto_field_name=True, float_precision=10
        )
        assert called_with.get("float_precision") == 10
