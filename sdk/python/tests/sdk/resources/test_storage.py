from feast.sdk.resources.storage import Storage
import os


class TestStorage(object):
    def test_update_options(self):
        storage = Storage(id="storage1", type="redis")
        assert storage.options == {}
        myDict = {'key':'value'}
        storage.options = myDict
        assert storage.options == myDict

    def test_from_yaml(self):
        storage = Storage.from_yaml("tests/sample/valid_storage.yaml")
        assert storage.id == "BIGQUERY1"
        assert storage.type == "bigquery"
        expDict = {"dataset" : "feast", 
            "project" : "gcp-project", 
            "tempLocation" : "gs://feast-storage"}
        assert storage.options == expDict

    def test_dump(self):
        opt = {"option1" : "value1", "option2" : "value2"}
        storage = Storage("storage1", "redis", opt)

        storage.dump("storage.yaml")    
        storage2 = Storage.from_yaml("storage.yaml")

        assert storage.id == storage2.id
        assert storage.type == storage2.type
        assert storage.options == storage2.options
        assert storage2.options == opt

        #cleanup
        os.remove("storage.yaml")