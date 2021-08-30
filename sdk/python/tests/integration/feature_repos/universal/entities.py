from feast import Entity, ValueType


def driver(value_type: ValueType = ValueType.INT64):
    return Entity(
        name="driver",  # The name is derived from this argument, not object name.
        value_type=value_type,
        description="driver id",
        join_key="driver_id",
    )


def customer():
    return Entity(name="customer_id", value_type=ValueType.INT64)
