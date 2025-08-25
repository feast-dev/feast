from typing import Dict, Optional, Union

from pydantic import BaseModel
from typing_extensions import Self

from feast.field import Field
from feast.types import Array, PrimitiveFeastType


class FieldModel(BaseModel):
    """
    Pydantic Model of a Feast Field.
    """

    name: str
    dtype: Union[Array, PrimitiveFeastType]
    description: str = ""
    tags: Optional[Dict[str, str]] = {}

    def to_field(self) -> Field:
        """
        Given a Pydantic FieldModel, create and return a Field.

        Returns:
            A Field.
        """
        return Field(
            name=self.name,
            dtype=self.dtype,
            description=self.description,
            tags=self.tags,
        )

    @classmethod
    def from_field(
        cls,
        field: Field,
    ) -> Self:  # type: ignore
        """
        Converts a Field object to its pydantic FieldModel representation.

        Returns:
            A FieldModel.
        """
        return cls(
            name=field.name,
            dtype=field.dtype,  # type: ignore
            description=field.description,
            tags=field.tags,
        )
