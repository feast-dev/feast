import pytest

from feast.types import Array, Float32, String, from_value_type
from feast.value_type import ValueType, is_valid_tag_key, is_valid_tag_value


def test_primitive_feast_type():
    assert String.to_value_type() == ValueType.STRING
    assert from_value_type(String.to_value_type()) == String
    assert Float32.to_value_type() == ValueType.FLOAT
    assert from_value_type(Float32.to_value_type()) == Float32


def test_array_feast_type():
    array_string = Array(String)
    assert array_string.to_value_type() == ValueType.STRING_LIST
    assert from_value_type(array_string.to_value_type()) == array_string

    array_float_32 = Array(Float32)
    assert array_float_32.to_value_type() == ValueType.FLOAT_LIST
    assert from_value_type(array_float_32.to_value_type()) == array_float_32

    with pytest.raises(ValueError):
        _ = Array(Array)

    with pytest.raises(ValueError):
        _ = Array(Array(String))


def test_all_value_types():
    for value in ValueType:
        # We do not support the NULL type.
        if value != ValueType.NULL:
            assert from_value_type(value).to_value_type() == value


def test_valid_tag_key_cases():
    # Valid cases
    valid_cases = [
        "app",  # Simple name
        "APP",  # Simple name
        "ap_.-p",  # Simple name w/ allowed special characters
        "example.com/my-tag",  # Domain with tag
        "a-b.c-m/d",  # Valid prefix and suffix
        "a.b.c.d.e/f",  # More segments
        "prefix/ta_.-g",  # Tag with a suffix and allowed special characters
        "a/b",  # Valid prefix and suffix
        "long-prefix-with-segments-allowed-in-dns-but-not-too-long/tag",  # Long prefix
        "a/tag",  # Short valid prefix and tag
        "a" * 63,  # 63 characters
        "test.com/B",  # Valid suffix
        "feast.dev",
    ]

    for case in valid_cases:
        assert is_valid_tag_key(case)


def test_invalid_tag_key_cases():
    # Invalid cases
    invalid_cases = [
        "",  # Empty string
        "a!d",  # Dissallowed special characters
        "a-/b",  # Invalid prefix
        "a.co_m/b",  # Invalid prefix
        "a_b.com/b",  # Invalid prefix
        "a.com.A/b",  # Invalid prefix
        "A/b",  # Invalid prefix
        "a/b-",  # Invalid suffix
        "a/-b",  # Invalid suffix
        "/missing",  # Missing prefix
        "a/",  # Missing suffix
        "feast.dev/",  # Forbidden prefix
        "feast.dev/tag",  # Starts with forbidden prefix
        "x" * 64,  # Name too long
        "a." * 63 + "x",  # Name too long
        "a" + "b." * 62 + "x",  # Name too long
        "a/" + "x" * 62,  # Too long overall
        "prefix/tag/extra",  # Too many segments
        "a b c",  # Contains space
        " ",  # Contains space
    ]

    for case in invalid_cases:
        assert not is_valid_tag_key(case)


def test_valid_tag_value_cases():
    # Valid cases
    valid_cases = [
        "",  # Empty string
        "a",  # Single alphanumeric character
        "abc",  # Simple alphanumeric
        "ABC",  # Simple alphanumeric
        "abc123",  # Alphanumeric with numbers
        "a.b.c",  # Valid with periods
        "a_b_c",  # Valid with underscores
        "a-b-c",  # Valid with dashes
        "a.b_c-d",  # Combination of valid characters
        "a" * 63,  # Maximum length allowed
        "a.b_c-d" * 9,  # Length less than or equal to 63
    ]

    for case in valid_cases:
        assert is_valid_tag_value(case)


def test_invalid_tag_value_cases():
    # Invalid cases
    invalid_cases = [
        "a" * 64,  # Too long
        "-ab",  # Doesn't start w/ alphanumeric character
        "ab.",  # Doesn't end w/ alphanumeric character
        "a!b",  # Contains invalid character (!)
        "abc@123",  # Contains invalid character (@)
        "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z",  # Too long
        "a b c",  # Contains space
        " ",  # Contains space
        "abc/def",  # Contains invalid character (/)
    ]

    for case in invalid_cases:
        assert not is_valid_tag_value(case)
