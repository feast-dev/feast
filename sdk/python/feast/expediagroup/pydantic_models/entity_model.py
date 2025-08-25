"""
Pydantic Model for Entity

Copyright 2023 Expedia Group
Author: matcarlin@expediagroup.com
"""

from datetime import datetime
from json import dumps
from typing import Dict, Optional

from pydantic import BaseModel, ConfigDict
from typing_extensions import Self

from feast.entity import Entity
from feast.value_type import ValueType


class EntityModel(BaseModel):
    """
    Pydantic Model of a Feast Entity.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="allow",
        json_encoders={ValueType: lambda v: int(dumps(v.value, default=str))},
    )

    name: str
    join_key: str
    value_type: Optional[ValueType] = None
    description: str = ""
    tags: Optional[Dict[str, str]] = None
    owner: str = ""
    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None

    def to_entity(self) -> Entity:
        """
        Given a Pydantic EntityModel, create and return an Entity.

        Returns:
            An Entity.
        """
        entity = Entity(
            name=self.name,
            join_keys=[self.join_key],
            value_type=self.value_type,
            description=self.description,
            tags=self.tags if self.tags else None,
            owner=self.owner,
        )
        entity.created_timestamp = self.created_timestamp
        entity.last_updated_timestamp = self.last_updated_timestamp
        return entity

    @classmethod
    def from_entity(
        cls,
        entity,
    ) -> Self:  # type: ignore
        """
        Converts an entity object to its pydantic model representation.

        Returns:
            An EntityModel.
        """
        return cls(
            name=entity.name,
            join_key=entity.join_key,
            value_type=entity.value_type,
            description=entity.description,
            tags=entity.tags if entity.tags else None,
            owner=entity.owner,
            created_timestamp=entity.created_timestamp,
            last_updated_timestamp=entity.last_updated_timestamp,
        )
