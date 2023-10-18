from feast.field import Feature, Field
from feast.types import Float32
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
