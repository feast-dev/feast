# Copyright 2018 The Feast Authors
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from feast.sdk.resources.entity import Entity
import os


class TestEntity(object):
    def test_read_from_yaml(self):
        entity_no_tag = Entity.from_yaml("tests/sample/valid_entity_no_tag.yaml")
        assert entity_no_tag.name == "myentity"
        assert entity_no_tag.description == "test entity without tag"
        assert len(entity_no_tag.tags) == 0
        
        entity = Entity.from_yaml("tests/sample/valid_entity.yaml")
        assert entity.name == "myentity"
        assert entity.description == "test entity with tag"
        assert entity.tags[0] == "tag1"
        assert entity.tags[1] == "tag2"

    def test_dump(self):
        entity = Entity("entity", "description", ["tag1", "tag2"])
        entity.dump("myentity.yaml")
        actual = Entity.from_yaml("myentity.yaml")
        assert actual.name == entity.name
        assert actual.description == entity.description
        for t1, t2 in zip(actual.tags, entity.tags):
            assert t1 == t2

        #cleanup
        os.remove("myentity.yaml")