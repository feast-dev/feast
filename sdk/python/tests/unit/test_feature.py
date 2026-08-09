import pytest

from feast.field import Feature, Field
from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2
from feast.types import Array, Bool, Float32, Float64, Int64, String
from feast.value_type import ValueType


def test_feature_serialization_with_description():
    expected_description = "Average daily trips"
    feature = Feature(
        name="avg_daily_trips", dtype=ValueType.FLOAT, description=expected_description
    )
    serialized_feature = feature.to_proto()

    assert serialized_feature.description == expected_description


def test_field_serialization_with_description():
    expected_description = "Average daily trips"
    field = Field(
        name="avg_daily_trips", dtype=Float32, description=expected_description
    )
    feature = Feature(
        name="avg_daily_trips", dtype=ValueType.FLOAT, description=expected_description
    )

    serialized_field = field.to_proto()
    field_from_feature = Field.from_feature(feature)

    assert serialized_field.description == expected_description
    assert field_from_feature.description == expected_description

    field = Field.from_proto(serialized_field)
    assert field.description == expected_description


@pytest.mark.parametrize(
    "dtype,default_value",
    [
        (Int64, 0),
        (Int64, -1),
        (Float64, 0.0),
        (Float64, 1.5),
        (String, "unknown"),
        # Zero-like defaults must read back as configured, not as unset.
        (String, ""),
        (Bool, False),
        (Bool, True),
        (Array(Int64), [1, 2, 3]),
    ],
)
def test_field_default_value_round_trip(dtype, default_value):
    field = Field(name="f", dtype=dtype, default_value=default_value)

    serialized = field.to_proto()
    assert serialized.HasField("default_value")

    deserialized = Field.from_proto(serialized)
    assert deserialized.default_value == default_value
    assert deserialized == field


def test_field_without_default_value_stays_unset():
    field = Field(name="f", dtype=Int64)

    serialized = field.to_proto()
    assert not serialized.HasField("default_value")
    assert Field.from_proto(serialized).default_value is None


def test_field_from_registry_without_default_value_field():
    """Registries written before field 8 existed must keep loading."""
    legacy = FeatureSpecV2(name="legacy", value_type=Int64.to_value_type().value)

    field = Field.from_proto(legacy)

    assert field.name == "legacy"
    assert field.default_value is None


def test_field_equality_detects_different_defaults():
    assert Field(name="f", dtype=Int64, default_value=0) != Field(
        name="f", dtype=Int64, default_value=1
    )
    assert Field(name="f", dtype=Int64, default_value=0) != Field(name="f", dtype=Int64)
    assert Field(name="f", dtype=Int64, default_value=0) == Field(
        name="f", dtype=Int64, default_value=0
    )


@pytest.mark.parametrize(
    "dtype,default_value",
    [
        (Int64, "abc"),
        (Bool, "yes"),
        (Float64, "x"),
        (Array(Int64), 5),
        (Int64, [1, 2]),
        # Coerced by the conversion, so these would silently store 1 and "123".
        (Int64, 1.5),
        (String, 123),
    ],
)
def test_field_rejects_incompatible_default_value(dtype, default_value):
    with pytest.raises(ValueError):
        Field(name="f", dtype=dtype, default_value=default_value)


def test_field_default_value_allows_lossless_widening():
    """int -> Float64 is exact, so it should not be treated as a lossy default."""
    field = Field(name="f", dtype=Float64, default_value=1)

    assert Field.from_proto(field.to_proto()).default_value == 1.0
