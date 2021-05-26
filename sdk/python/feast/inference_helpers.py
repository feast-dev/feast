from typing import List

from feast import Entity
from feast.feature_view import FeatureView
from feast.value_type import ValueType


def infer_entity_value_type_from_feature_views(
    entities: List[Entity], feature_views: List[FeatureView]
) -> List[Entity]:
    incomplete_entities = {
        entity.name: entity
        for entity in entities
        if entity.value_type == ValueType.UNKNOWN
    }
    incomplete_entities_keys = incomplete_entities.keys()

    for view in feature_views:
        if not (incomplete_entities_keys & set(view.entities)):
            continue
        col_names_and_types = view.input.get_table_column_names_and_types()
        for entity_name in view.entities:
            if entity_name in incomplete_entities:
                entity_col = list(
                    filter(lambda tup: tup[0] == entity_name, col_names_and_types)
                )
                if len(entity_col) == 0:
                    # Doesn't mention inference error because would also be an error without inferencing
                    raise ValueError(
                        f"""No column in the input source for the {view.name} FeatureView matches
                        its Entity's name."""
                    )

                entity = incomplete_entities[entity_name]
                inferred_value_type = view.input.source_datatype_to_feast_value_type()(
                    entity_col[0][1]
                )
                if (
                    entity.value_type != ValueType.UNKNOWN
                    and entity.value_type != inferred_value_type
                ) or (len(entity_col) > 1):
                    raise ValueError(
                        f"""Entity value_type inference failed for {entity_name} Entity.
                        Multiple viable matches. Please explicitly specify the Entity value_type
                        for this Entity."""
                    )

                entity.value_type = inferred_value_type

    return entities
