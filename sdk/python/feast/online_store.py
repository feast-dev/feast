# Copyright 2019-2022 The Feast Authors
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


from feast.core.OnlineStore_pb2 import OnlineStore as OnlineStoreProto
from feast.core.OnlineStore_pb2 import StoreType


class OnlineStore:
    """
    Database to store features for online serving
    """

    def __init__(
        self,
        name: str,
        store_type: str,
        description: str,
    ):
        self._name = name
        self._description = description
        self._store_type = self._validate_store_type(store_type)

    def __eq__(self, other):
        return self.to_proto() == other.to_proto()

    def __str__(self):
        return str(self.to_proto())

    def __repr__(self):
        return f"OnlineStore <{self.name}>"

    @property
    def name(self):
        """
        Returns the name of this online store
        """
        return self._name

    @name.setter
    def name(self, name):
        """
        Sets the name of this online store
        """
        self._name = name

    @property
    def store_type(self):
        """
        Returns the type of this online store
        """
        return self._store_type

    @store_type.setter
    def store_type(self, store_type: str):
        """
        Set the type for this online store
        """
        self._store_type = self._validate_store_type(store_type)

    @property
    def description(self):
        """
        Returns the description of this online store
        """
        return self._description

    @description.setter
    def description(self, description):
        """
        Sets the description of this online store
        """
        self._description = description

    @classmethod
    def from_proto(cls, online_store_proto: OnlineStoreProto):
        """
        Creates an OnlineStore from a protobuf representation of an online store

        args:
            online_store_proto: a protobuf representation of an online store

        Returns:
            Returns a OnlineStore object based on the online store protobuf
        """
        online_store = cls(
            name=online_store_proto.name,
            store_type=StoreType.Name(online_store_proto.type),
            description=online_store_proto.description,
        )
        return online_store

    def to_proto(self) -> OnlineStoreProto:
        """
        Converts an OnlineStore object to its protobuf representation.

        Returns:
            OnlineStoreProto protobuf
        """
        online_store_proto = OnlineStoreProto(
            name=self.name,
            type=self.store_type,
            description=self.description,
        )
        return online_store_proto

    @staticmethod
    def _validate_store_type(store_type):
        if store_type in StoreType.keys():
            return store_type
        else:
            raise ValueError(
                f"Unsupported online store type: {store_type}, available store types: {StoreType.keys()}"
            )
