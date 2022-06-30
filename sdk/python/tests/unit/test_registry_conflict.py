import pytest

from feast.entity import Entity

@pytest.mark.integration
def test_apply_first_entity(environment):
    entity = Entity(name="first")
    fs = environment.feature_store
    fs.apply([entity])

    entities = fs.list_entities()
    assert len(entities) == 1


@pytest.mark.integration
def test_apply_second_entity(environment):
    entity = Entity(name="second")
    fs = environment.feature_store
    fs.apply([entity])

    entities = fs.list_entities()
    assert len(entities) == 1
