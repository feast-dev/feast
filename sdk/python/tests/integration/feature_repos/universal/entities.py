from feast import Entity, ValueType


def driver():
    return Entity(
        name="driver",  # The name is derived from this argument, not object name.
        value_type=ValueType.INT64,
        description="driver id",
        join_key="driver_id",
    )
