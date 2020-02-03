# Copyright 2019 The Feast Authors
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
import warnings

from tensorflow_metadata.proto.v0 import schema_pb2

from feast.value_type import ValueType


class Field:
    """
    High level field type. This is the parent type to both entities and
    features.
    """

    def __init__(self, name: str, dtype: ValueType):
        self._name = name
        if not isinstance(dtype, ValueType):
            raise ValueError("dtype is not a valid ValueType")
        self._dtype = dtype
        self._presence = None
        self._group_presence = None
        self._shape = None
        self._value_count = None
        self._domain = None
        self._int_domain = None
        self._float_domain = None
        self._string_domain = None
        self._bool_domain = None
        self._struct_domain = None
        self._natural_language_domain = None
        self._image_domain = None
        self._mid_domain = None
        self._url_domain = None
        self._time_domain = None
        self._time_of_day_domain = None

    def __eq__(self, other):
        if self.name != other.name or self.dtype != other.dtype:
            return False
        return True

    @property
    def name(self):
        """
        Getter for name of this field
        """
        return self._name

    @property
    def dtype(self) -> ValueType:
        """
        Getter for data type of this field
        """
        return self._dtype

    @property
    def presence(self) -> schema_pb2.FeaturePresence:
        """
        Getter for presence of this field
        """
        return self._presence

    @presence.setter
    def presence(self, presence: schema_pb2.FeaturePresence):
        if not isinstance(presence, schema_pb2.FeaturePresence):
            raise TypeError("presence must be of FeaturePresence type")
        self._clear_presence_constraints()
        self._presence = presence

    @property
    def group_presence(self) -> schema_pb2.FeaturePresenceWithinGroup:
        """
        Getter for group_presence of this field
        """
        return self._group_presence

    @group_presence.setter
    def group_presence(self, group_presence: schema_pb2.FeaturePresenceWithinGroup):
        if not isinstance(group_presence, schema_pb2.FeaturePresenceWithinGroup):
            raise TypeError("group_presence must be of FeaturePresenceWithinGroup type")
        self._clear_presence_constraints()
        self._group_presence = group_presence

    @property
    def shape(self) -> schema_pb2.FixedShape:
        """
        Getter for shape of this field
        """
        return self._shape

    @shape.setter
    def shape(self, shape: schema_pb2.FixedShape):
        if not isinstance(shape, schema_pb2.FixedShape):
            raise TypeError("shape must be of FixedShape type")
        self._clear_shape_type()
        self._shape = shape

    @property
    def value_count(self) -> schema_pb2.ValueCount:
        """
        Getter for value_count of this field
        """
        return self._value_count

    @value_count.setter
    def value_count(self, value_count: schema_pb2.ValueCount):
        if not isinstance(value_count, schema_pb2.ValueCount):
            raise TypeError("value_count must be of ValueCount type")
        self._clear_shape_type()
        self._value_count = value_count

    @property
    def domain(self) -> str:
        """
        Getter for domain of this field
        """
        return self._domain

    @domain.setter
    def domain(self, domain: str):
        if not isinstance(domain, str):
            raise TypeError("domain must be of str type")
        self._clear_domain_info()
        self._domain = domain

    @property
    def int_domain(self) -> schema_pb2.IntDomain:
        """
        Getter for int_domain of this field
        """
        return self._int_domain

    @int_domain.setter
    def int_domain(self, int_domain: schema_pb2.IntDomain):
        if not isinstance(int_domain, schema_pb2.IntDomain):
            raise TypeError("int_domain must be of IntDomain type")
        self._clear_domain_info()
        self._int_domain = int_domain

    @property
    def float_domain(self) -> schema_pb2.FloatDomain:
        """
        Getter for float_domain of this field
        """
        return self._float_domain

    @float_domain.setter
    def float_domain(self, float_domain: schema_pb2.FloatDomain):
        if not isinstance(float_domain, schema_pb2.FloatDomain):
            raise TypeError("float_domain must be of FloatDomain type")
        self._clear_domain_info()
        self._float_domain = float_domain

    @property
    def string_domain(self) -> schema_pb2.StringDomain:
        """
        Getter for string_domain of this field
        """
        return self._string_domain

    @string_domain.setter
    def string_domain(self, string_domain: schema_pb2.StringDomain):
        if not isinstance(string_domain, schema_pb2.StringDomain):
            raise TypeError("string_domain must be of StringDomain type")
        self._clear_domain_info()
        self._string_domain = string_domain

    @property
    def bool_domain(self) -> schema_pb2.BoolDomain:
        """
        Getter for bool_domain of this field
        """
        return self._bool_domain

    @bool_domain.setter
    def bool_domain(self, bool_domain: schema_pb2.BoolDomain):
        if not isinstance(bool_domain, schema_pb2.BoolDomain):
            raise TypeError("bool_domain must be of BoolDomain type")
        self._clear_domain_info()
        self._bool_domain = bool_domain

    @property
    def struct_domain(self) -> schema_pb2.StructDomain:
        """
        Getter for struct_domain of this field
        """
        return self._struct_domain

    @struct_domain.setter
    def struct_domain(self, struct_domain: schema_pb2.StructDomain):
        if not isinstance(struct_domain, schema_pb2.StructDomain):
            raise TypeError("struct_domain must be of StructDomain type")
        self._clear_domain_info()
        self._struct_domain = struct_domain

    @property
    def natural_language_domain(self) -> schema_pb2.NaturalLanguageDomain:
        """
        Getter for natural_language_domain of this field
        """
        return self._natural_language_domain

    @natural_language_domain.setter
    def natural_language_domain(
        self, natural_language_domain: schema_pb2.NaturalLanguageDomain
    ):
        if not isinstance(natural_language_domain, schema_pb2.NaturalLanguageDomain):
            raise TypeError(
                "natural_language_domain must be of NaturalLanguageDomain type"
            )
        self._clear_domain_info()
        self._natural_language_domain = natural_language_domain

    @property
    def image_domain(self) -> schema_pb2.ImageDomain:
        """
        Getter for image_domain of this field
        """
        return self._image_domain

    @image_domain.setter
    def image_domain(self, image_domain: schema_pb2.ImageDomain):
        if not isinstance(image_domain, schema_pb2.ImageDomain):
            raise TypeError("image_domain must be of ImageDomain type")
        self._clear_domain_info()
        self._image_domain = image_domain

    @property
    def mid_domain(self) -> schema_pb2.MIDDomain:
        """
        Getter for mid_domain of this field
        """
        return self._mid_domain

    @mid_domain.setter
    def mid_domain(self, mid_domain: schema_pb2.MIDDomain):
        if not isinstance(mid_domain, schema_pb2.MIDDomain):
            raise TypeError("mid_domain must be of MIDDomain type")
        self._clear_domain_info()
        self._mid_domain = mid_domain

    @property
    def url_domain(self) -> schema_pb2.URLDomain:
        """
        Getter for url_domain of this field
        """
        return self._url_domain

    @url_domain.setter
    def url_domain(self, url_domain: schema_pb2.URLDomain):
        if not isinstance(url_domain, schema_pb2.URLDomain):
            raise TypeError("url_domain must be of URLDomain type")
        self._clear_domain_info()
        self.url_domain = url_domain

    @property
    def time_domain(self) -> schema_pb2.TimeDomain:
        """
        Getter for time_domain of this field
        """
        return self._time_domain

    @time_domain.setter
    def time_domain(self, time_domain: schema_pb2.TimeDomain):
        if not isinstance(time_domain, schema_pb2.TimeDomain):
            raise TypeError("time_domain must be of TimeDomain type")
        self._clear_domain_info()
        self._time_domain = time_domain

    @property
    def time_of_day_domain(self) -> schema_pb2.TimeOfDayDomain:
        """
        Getter for time_of_day_domain of this field
        """
        return self._time_of_day_domain

    @time_of_day_domain.setter
    def time_of_day_domain(self, time_of_day_domain) -> schema_pb2.TimeOfDayDomain:
        if not isinstance(time_of_day_domain, schema_pb2.TimeOfDayDomain):
            raise TypeError("time_of_day_domain must be of TimeOfDayDomain type")
        self._clear_domain_info()
        self._time_of_day_domain = time_of_day_domain

    def update_presence_constraints(self, feature: schema_pb2.Feature):
        presence_constraints_case = feature.WhichOneof("presence_constraints")
        if presence_constraints_case == "presence":
            self.presence = feature.presence
        elif presence_constraints_case == "group_presence":
            self.group_presence = feature.group_presence

    def update_shape_type(self, feature: schema_pb2.Feature):
        shape_type_case = feature.WhichOneof("shape_type")
        if shape_type_case == "shape":
            self.shape = feature.shape
        elif shape_type_case == "value_count":
            self.value_count = feature.value_count

    def update_domain_info(
        self, feature: schema_pb2.Feature, schema: schema_pb2.Schema = None
    ):
        domain_info_case = feature.WhichOneof("domain_info")
        if domain_info_case == "domain":
            domain_ref = feature.domain
            if schema is None:
                warnings.warn(
                    f"Schema is not provided so domain '{domain_ref}' cannot be "
                    f"referenced and domain for field '{self.name}' will not be updated."
                )
            else:
                domain_ref_to_string_domain = {d.name: d for d in schema.string_domain}
                domain_ref_to_float_domain = {d.name: d for d in schema.float_domain}
                domain_ref_to_int_domain = {d.name: d for d in schema.int_domain}

                if domain_ref in domain_ref_to_string_domain:
                    self.string_domain = domain_ref_to_string_domain[domain_ref]
                elif domain_ref in domain_ref_to_float_domain:
                    self.float_domain = domain_ref_to_float_domain[domain_ref]
                elif domain_ref in domain_ref_to_int_domain:
                    self.int_domain = domain_ref_to_int_domain[domain_ref]
                else:
                    raise ValueError(
                        f"Reference to a domain '{domain_ref}' is missing in the schema. "
                        f"Please check the string_domain, float_domain and int_domain"
                        f"fields in the schema of your Tensorflow metadata, making sure"
                        f"that the domain referenced exists."
                    )
        elif domain_info_case == "int_domain":
            self.int_domain = feature.int_domain
        elif domain_info_case == "float_domain":
            self.float_domain = feature.float_domain
        elif domain_info_case == "string_domain":
            self.string_domain = feature.string_domain
        elif domain_info_case == "bool_domain":
            self.bool_domain = feature.bool_domain
        elif domain_info_case == "struct_domain":
            self.struct_domain = feature.struct_domain
        elif domain_info_case == "natural_language_domain":
            self.natural_language_domain = feature.natural_language_domain
        elif domain_info_case == "image_domain":
            self.image_domain = feature.image_domain
        elif domain_info_case == "mid_domain":
            self.mid_domain = feature.mid_domain
        elif domain_info_case == "url_domain":
            self.url_domain = feature.url_domain
        elif domain_info_case == "time_domain":
            self.time_domain = feature.time_domain
        elif domain_info_case == "time_of_day_domain":
            self.time_of_day_domain = feature.time_of_day_domain

    def to_proto(self):
        """
        Unimplemented to_proto method for a field. This should be extended.
        """
        pass

    def from_proto(self, proto):
        """
        Unimplemented from_proto method for a field. This should be extended.
        """
        pass

    def _clear_presence_constraints(self):
        self._presence = None
        self._group_presence = None

    def _clear_shape_type(self):
        self._shape = None
        self._value_count = None

    def _clear_domain_info(self):
        self._domain = None
        self._int_domain = None
        self._float_domain = None
        self._string_domain = None
        self._bool_domain = None
        self._struct_domain = None
        self._natural_language_domain = None
        self._image_domain = None
        self._mid_domain = None
        self._url_domain = None
        self._time_domain = None
        self._time_of_day_domain = None
