from pydantic.fields import Field
from pydantic.main import BaseModel
from pydantic.tools import parse_obj_as
from pydantic.typing import Literal, Union, Annotated


class Cat(BaseModel):
    pet_type: Literal['cat']
    name: str
    foo: str  # <- Is returned as missing


class Dog(BaseModel):
    pet_type: Literal['dog']  # <- Is returned as unexpected (received cat)
    name: str
    bar: str  # <- Is returned as missing


Pet = Annotated[Union[Cat, Dog], Field(discriminator='pet_type')]
print(parse_obj_as(Pet, dict(pet_type='cat', name='Felix')))
