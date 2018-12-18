from feast.sdk.resources.entity import Entity
import os

class TestEntity(object):
    def test_read_from_yaml(self):
        entity_no_tag = Entity.from_yaml_file("tests/sample/valid_entity_no_tag.yaml")
        assert entity_no_tag.name == "myentity"
        assert entity_no_tag.description == "test entity without tag"
        assert len(entity_no_tag.tags) == 0
        
        entity = Entity.from_yaml_file("tests/sample/valid_entity.yaml")
        assert entity.name == "myentity"
        assert entity.description == "test entity with tag"
        assert entity.tags[0] == "tag1"
        assert entity.tags[1] == "tag2"

    def test_dump(self):
        entity = Entity("entity", "description", ["tag1", "tag2"])
        entity.dump("myentity.yaml")
        actual = Entity.from_yaml_file("myentity.yaml")
        assert actual.name == entity.name
        assert actual.description == entity.description
        for t1, t2 in zip(actual.tags, entity.tags):
            assert t1 == t2

        #cleanup
        os.remove("myentity.yaml")