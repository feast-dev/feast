from feast import Entity


def driver():
    return Entity(
        name="driver",  # The name is derived from this argument, not object name.
        description="driver id",
        join_keys=["driver_id"],
    )


def customer():
    return Entity(name="customer_id")


def location():
    return Entity(name="location_id")


def item():
    return Entity(name="item_id")
