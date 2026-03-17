from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel


class ComparisonFilter(BaseModel):
    """A filter that compares a metadata field against a value.

    :param type: The comparison operator to apply
    :param key: The metadata field name to filter on
    :param value: The value to compare against
    """

    type: Literal["eq", "ne", "gt", "gte", "lt", "lte", "in", "nin"]
    key: str
    value: Any


class CompoundFilter(BaseModel):
    """A filter that combines multiple filters with a logical operator.

    :param type: The logical operator ("and" requires all filters match,
        "or" requires any filter matches)
    :param filters: The list of filters to combine
    """

    type: Literal["and", "or"]
    filters: List[Union[ComparisonFilter, "CompoundFilter"]]


CompoundFilter.model_rebuild()

FilterType = Optional[Union[ComparisonFilter, CompoundFilter]]


def convert_dict_to_filter(
    filter_dict: Dict[str, Any],
) -> Union[ComparisonFilter, CompoundFilter]:
    """Convert a raw dict (e.g. from OpenAI-compatible JSON) into a typed filter object."""
    filter_type = filter_dict.get("type")
    if filter_type in ("and", "or"):
        return CompoundFilter(
            type=filter_type,
            filters=[convert_dict_to_filter(f) for f in filter_dict["filters"]],
        )
    return ComparisonFilter(
        type=filter_dict["type"],
        key=filter_dict["key"],
        value=filter_dict["value"],
    )
