#!/usr/bin/env python3
"""
Multiple Join Keys Example for Feast Entities

This example demonstrates the new multiple join keys functionality
introduced in Feast 0.55.0, which resolves the previous limitation:
"ValueError: An entity may only have a single join key"

Author: Feast Contributors
License: Apache 2.0
"""

import warnings
from datetime import timedelta

from feast import Entity, FeatureStore, FeatureView, Field
from feast.data_source import PushSource
from feast.types import Float64, Int64, String
from feast.value_type import ValueType


def main():
    """Demonstrate multiple join keys functionality."""
    print("🎉 Multiple Join Keys Example for Feast")
    print("=" * 50)

    # ============================================================================
    # 1. Create entities with multiple join keys
    # ============================================================================
    print("\n1. Creating entities with multiple join keys...")

    # User entity with multiple identifiers
    user_entity = Entity(
        name="user",
        join_keys=["user_id", "email", "username"],  # Multiple join keys!
        value_type=ValueType.STRING,
        description="User entity with multiple unique identifiers"
    )

    # Product entity with multiple SKUs
    product_entity = Entity(
        name="product",
        join_keys=["product_id", "sku", "barcode"],
        value_type=ValueType.STRING,
        description="Product entity with multiple product identifiers"
    )

    # Location entity with geographic identifiers
    location_entity = Entity(
        name="location",
        join_keys=["location_id", "zip_code", "lat_lon"],
        value_type=ValueType.STRING,
        description="Location entity with multiple geographic identifiers"
    )

    print(f"✅ User entity join keys: {user_entity.join_keys}")
    print(f"✅ Product entity join keys: {product_entity.join_keys}")
    print(f"✅ Location entity join keys: {location_entity.join_keys}")

    # ============================================================================
    # 2. Demonstrate backward compatibility
    # ============================================================================
    print("\n2. Backward compatibility demonstration...")

    # Legacy single join key still works
    driver_entity = Entity(
        name="driver",
        join_keys=["driver_id"],  # Single join key
        value_type=ValueType.INT64
    )

    print(f"✅ Legacy entity join keys: {driver_entity.join_keys}")

    # Show deprecation warning for .join_key property
    print("\n⚠️  Demonstrating deprecation warning:")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        legacy_key = driver_entity.join_key  # This triggers deprecation warning

        if w:
            print(f"   Warning: {w[0].message}")
        print(f"   Legacy key value: {legacy_key}")

    # ============================================================================
    # 3. Create feature views using multiple join keys
    # ============================================================================
    print("\n3. Creating feature views with multiple join keys...")

    # User stats feature view
    user_stats_source = PushSource(
        name="user_stats_source",
        batch_source=None,  # Simplified for example
    )

    user_stats_fv = FeatureView(
        name="user_stats",
        entities=[user_entity],  # Uses all join keys: user_id, email, username
        ttl=timedelta(days=1),
        schema=[
            Field(name="total_orders", dtype=Int64),
            Field(name="avg_order_value", dtype=Float64),
            Field(name="loyalty_score", dtype=Float64),
        ],
        source=user_stats_source,
    )

    print(f"✅ User stats feature view created with entity: {user_stats_fv.entities}")

    # ============================================================================
    # 4. Show practical usage patterns
    # ============================================================================
    print("\n4. Practical usage patterns...")

    # Pattern 1: Access all join keys
    def print_entity_info(entity):
        print(f"   Entity: {entity.name}")
        print(f"   All join keys: {entity.join_keys}")
        print(f"   Primary join key: {entity.join_keys[0]}")
        print(f"   Join key count: {len(entity.join_keys)}")

    print("\n📊 Entity Information:")
    for entity in [user_entity, product_entity, location_entity]:
        print_entity_info(entity)
        print()

    # Pattern 2: Join key validation
    def validate_entity_keys(entity, required_keys):
        """Validate that entity has all required join keys."""
        missing_keys = set(required_keys) - set(entity.join_keys)
        if missing_keys:
            print(f"❌ Entity {entity.name} missing keys: {missing_keys}")
            return False
        print(f"✅ Entity {entity.name} has all required keys")
        return True

    print("\n🔍 Join Key Validation:")
    validate_entity_keys(user_entity, ["user_id", "email"])
    validate_entity_keys(product_entity, ["product_id", "sku", "barcode", "missing_key"])

    # ============================================================================
    # 5. Migration from legacy code
    # ============================================================================
    print("\n5. Migration patterns...")

    print("\n📝 Before (deprecated):")
    print("   entity_key = my_entity.join_key  # ⚠️ Shows deprecation warning")

    print("\n📝 After (recommended):")
    print("   # For single key compatibility:")
    print("   entity_key = my_entity.join_keys[0]")
    print("   ")
    print("   # For multiple keys support:")
    print("   entity_keys = my_entity.join_keys")

    # Example migration
    print("\n🔄 Migration example:")
    legacy_entity = Entity(name="customer", join_keys=["customer_id"])

    # Old way (deprecated, shows warning)
    print("   Old way (with warning):")
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        old_way = legacy_entity.join_key
        print(f"     key = {old_way}")

    # New way (recommended)
    print("   New way (no warning):")
    new_way = legacy_entity.join_keys[0]
    print(f"     key = {new_way}")

    # Best way (for new code)
    print("   Best way (multiple keys support):")
    best_way = legacy_entity.join_keys
    print(f"     keys = {best_way}")

    print("\n🎉 Multiple join keys example completed successfully!")
    print("\nKey takeaways:")
    print("• Multiple join keys are now fully supported")
    print("• Backward compatibility is maintained")
    print("• Use join_keys instead of join_key for new code")
    print("• Existing code works with deprecation warnings")
    print("• Migration is straightforward and non-breaking")


if __name__ == "__main__":
    main()