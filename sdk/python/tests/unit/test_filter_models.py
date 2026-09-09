import pytest
from pydantic import ValidationError

from feast.filter_models import (
    ComparisonFilter,
    CompoundFilter,
    convert_dict_to_filter,
)


class TestComparisonFilter:
    @pytest.mark.parametrize("op", ["eq", "ne", "gt", "gte", "lt", "lte", "in", "nin"])
    def test_valid_operators(self, op):
        f = ComparisonFilter(type=op, key="field", value="x")
        assert f.type == op
        assert f.key == "field"

    def test_rejects_invalid_operator(self):
        with pytest.raises(ValidationError):
            ComparisonFilter(type="like", key="field", value="x")

    def test_accepts_string_value(self):
        f = ComparisonFilter(type="eq", key="city", value="LA")
        assert f.value == "LA"

    def test_accepts_int_value(self):
        f = ComparisonFilter(type="gt", key="age", value=25)
        assert f.value == 25

    def test_accepts_float_value(self):
        f = ComparisonFilter(type="lte", key="score", value=0.95)
        assert f.value == 0.95

    def test_accepts_bool_value(self):
        f = ComparisonFilter(type="eq", key="active", value=True)
        assert f.value is True

    def test_accepts_list_value(self):
        f = ComparisonFilter(type="in", key="status", value=["a", "b"])
        assert f.value == ["a", "b"]


class TestCompoundFilter:
    def test_and_filter(self):
        f = CompoundFilter(
            type="and",
            filters=[
                ComparisonFilter(type="eq", key="a", value=1),
                ComparisonFilter(type="gt", key="b", value=2),
            ],
        )
        assert f.type == "and"
        assert len(f.filters) == 2

    def test_or_filter(self):
        f = CompoundFilter(
            type="or",
            filters=[
                ComparisonFilter(type="eq", key="a", value=1),
                ComparisonFilter(type="eq", key="b", value=2),
            ],
        )
        assert f.type == "or"
        assert len(f.filters) == 2

    def test_rejects_invalid_type(self):
        with pytest.raises(ValidationError):
            CompoundFilter(
                type="xor",
                filters=[ComparisonFilter(type="eq", key="a", value=1)],
            )

    def test_nested_compound(self):
        f = CompoundFilter(
            type="and",
            filters=[
                ComparisonFilter(type="eq", key="x", value=1),
                CompoundFilter(
                    type="or",
                    filters=[
                        ComparisonFilter(type="gt", key="y", value=5),
                        ComparisonFilter(type="lt", key="z", value=10),
                    ],
                ),
            ],
        )
        assert f.type == "and"
        assert len(f.filters) == 2
        inner = f.filters[1]
        assert isinstance(inner, CompoundFilter)
        assert inner.type == "or"
        assert len(inner.filters) == 2


class TestConvertDictToFilter:
    def test_comparison_dict(self):
        result = convert_dict_to_filter({"type": "eq", "key": "city", "value": "LA"})
        assert isinstance(result, ComparisonFilter)
        assert result.type == "eq"
        assert result.key == "city"
        assert result.value == "LA"

    def test_compound_dict(self):
        result = convert_dict_to_filter(
            {
                "type": "and",
                "filters": [
                    {"type": "eq", "key": "a", "value": 1},
                    {"type": "gt", "key": "b", "value": 2},
                ],
            }
        )
        assert isinstance(result, CompoundFilter)
        assert result.type == "and"
        assert len(result.filters) == 2
        assert all(isinstance(f, ComparisonFilter) for f in result.filters)

    def test_nested_compound_dict(self):
        result = convert_dict_to_filter(
            {
                "type": "or",
                "filters": [
                    {"type": "eq", "key": "x", "value": "a"},
                    {
                        "type": "and",
                        "filters": [
                            {"type": "gt", "key": "y", "value": 5},
                            {"type": "lt", "key": "z", "value": 10},
                        ],
                    },
                ],
            }
        )
        assert isinstance(result, CompoundFilter)
        assert result.type == "or"
        inner = result.filters[1]
        assert isinstance(inner, CompoundFilter)
        assert inner.type == "and"
        assert len(inner.filters) == 2

    def test_or_compound_dict(self):
        result = convert_dict_to_filter(
            {
                "type": "or",
                "filters": [
                    {"type": "eq", "key": "a", "value": 1},
                    {"type": "eq", "key": "b", "value": 2},
                ],
            }
        )
        assert isinstance(result, CompoundFilter)
        assert result.type == "or"

    def test_in_operator_dict(self):
        result = convert_dict_to_filter(
            {"type": "in", "key": "status", "value": ["active", "pending"]}
        )
        assert isinstance(result, ComparisonFilter)
        assert result.type == "in"
        assert result.value == ["active", "pending"]
