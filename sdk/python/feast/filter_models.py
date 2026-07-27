from abc import ABC, abstractmethod
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


class FilterTranslator(ABC):
    """Abstract base for translating Feast filters into backend-native expressions.

    Each online store backend provides a concrete subclass that knows how to
    turn :class:`ComparisonFilter` / :class:`CompoundFilter` trees into the
    query language understood by the underlying database or search engine.

    The :meth:`translate` entry-point handles ``None`` via :meth:`_empty` and
    delegates to :meth:`translate_comparison` / :meth:`translate_compound`
    through the shared :meth:`_dispatch` recursion, so subclasses only need to
    implement the three abstract leaf methods.
    """

    @abstractmethod
    def translate(self, filters: FilterType) -> Any:
        """Return the backend-native filter expression."""
        ...

    def _dispatch(self, filter_obj: Union[ComparisonFilter, CompoundFilter]) -> Any:
        """Route to comparison or compound handler (used for recursion)."""
        if isinstance(filter_obj, ComparisonFilter):
            return self.translate_comparison(filter_obj)
        elif isinstance(filter_obj, CompoundFilter):
            return self.translate_compound(filter_obj)
        raise ValueError(f"Unknown filter type: {type(filter_obj)}")

    @abstractmethod
    def translate_comparison(self, f: ComparisonFilter) -> Any:
        """Translate a single comparison into a backend-native expression."""
        ...

    @abstractmethod
    def translate_compound(self, f: CompoundFilter) -> Any:
        """Translate a compound (AND/OR) filter into a backend-native expression."""
        ...


def filters_contain_numeric_comparison(
    filter_obj: Union[ComparisonFilter, CompoundFilter],
) -> bool:
    """Return True if any leaf comparison uses a numeric value."""
    if isinstance(filter_obj, ComparisonFilter):
        return isinstance(filter_obj.value, (int, float)) and not isinstance(
            filter_obj.value, bool
        )
    if isinstance(filter_obj, CompoundFilter):
        return any(filters_contain_numeric_comparison(f) for f in filter_obj.filters)
    return False


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
