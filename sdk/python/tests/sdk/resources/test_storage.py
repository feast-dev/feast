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

from feast.sdk.resources.storage import Storage
import os


class TestStorage(object):
    def test_update_options(self):
        storage = Storage(id="storage1", type="redis")
        assert storage.options == {}
        myDict = {'key': 'value'}
        storage.options = myDict
        assert storage.options == myDict

    def test_from_yaml(self):
        storage = Storage.from_yaml("tests/sample/valid_storage.yaml")
        assert storage.id == "BIGQUERY1"
        assert storage.type == "bigquery"
        expDict = {
            "dataset": "feast",
            "project": "gcp-project",
            "tempLocation": "gs://feast-storage"
        }
        assert storage.options == expDict

    def test_dump(self):
        opt = {"option1": "value1", "option2": "value2"}
        storage = Storage("storage1", "redis", opt)

        storage.dump("storage.yaml")
        storage2 = Storage.from_yaml("storage.yaml")

        assert storage.id == storage2.id
        assert storage.type == storage2.type
        assert storage.options == storage2.options
        assert storage2.options == opt

        #cleanup
        os.remove("storage.yaml")
