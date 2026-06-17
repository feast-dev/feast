"""
Unit tests for FieldConstraints and Imputation: validator paths,
cross-field invariants, and proto round-trip.
"""

import pytest
from pydantic import ValidationError

from feast.field_constraints import FieldConstraints, Imputation


def test_empty_field_constraints_is_valid():
    """A constraint with nothing set is valid; every field is optional."""
    fc = FieldConstraints()
    assert fc.nullable is None
    assert fc.max_null_pct is None
    assert fc.min_value is None
    assert fc.max_value is None
    assert fc.min_compliance is None
    assert fc.allowed_values is None
    assert fc.regex is None
    assert fc.unique is None
    assert fc.custom is None
    assert fc.imputation is None


def test_extra_fields_forbidden():
    with pytest.raises(ValidationError):
        FieldConstraints(nulable=False)  # typo


@pytest.mark.parametrize("value", [0.0, 0.5, 1.0])
def test_max_null_pct_accepts_valid_range(value):
    assert FieldConstraints(max_null_pct=value).max_null_pct == value


@pytest.mark.parametrize("value", [-0.01, 1.01, -1.0, 100.0])
def test_max_null_pct_rejects_out_of_range(value):
    with pytest.raises(ValidationError) as exc:
        FieldConstraints(max_null_pct=value)
    assert "max_null_pct" in str(exc.value)


@pytest.mark.parametrize("value", [0.0, 0.5, 0.999, 1.0])
def test_min_compliance_accepts_valid_range(value):
    assert FieldConstraints(min_compliance=value).min_compliance == value


@pytest.mark.parametrize("value", [-0.01, 1.01])
def test_min_compliance_rejects_out_of_range(value):
    with pytest.raises(ValidationError) as exc:
        FieldConstraints(min_compliance=value)
    assert "min_compliance" in str(exc.value)


def test_regex_accepts_compilable_pattern():
    assert FieldConstraints(regex=r"^[A-Z]{2,5}$").regex == r"^[A-Z]{2,5}$"


def test_regex_rejects_uncompilable_pattern():
    with pytest.raises(ValidationError) as exc:
        FieldConstraints(regex=r"[unclosed")
    assert "regex" in str(exc.value)


@pytest.mark.parametrize("value", ["", "   ", "\t"])
def test_regex_rejects_empty(value):
    with pytest.raises(ValidationError) as exc:
        FieldConstraints(regex=value)
    assert "regex must not be empty" in str(exc.value)


def test_allowed_values_rejects_empty_list():
    with pytest.raises(ValidationError) as exc:
        FieldConstraints(allowed_values=[])
    assert "allowed_values" in str(exc.value)


def test_nullable_false_with_zero_null_pct_is_consistent():
    fc = FieldConstraints(nullable=False, max_null_pct=0)
    assert fc.nullable is False
    assert fc.max_null_pct == 0


def test_nullable_false_with_positive_null_pct_is_contradiction():
    with pytest.raises(ValidationError):
        FieldConstraints(nullable=False, max_null_pct=0.1)


def test_min_greater_than_max_is_rejected():
    with pytest.raises(ValidationError):
        FieldConstraints(min_value=10, max_value=5)


def test_min_equal_to_max_is_allowed():
    fc = FieldConstraints(min_value=42, max_value=42)
    assert fc.min_value == 42 and fc.max_value == 42


@pytest.mark.parametrize("strategy", ["mean", "median"])
def test_imputation_mean_median_no_default_value(strategy):
    imp = Imputation(strategy=strategy)
    assert imp.strategy == strategy
    assert imp.default_value is None


@pytest.mark.parametrize("strategy", ["mean", "median"])
def test_imputation_mean_median_rejects_default_value(strategy):
    with pytest.raises(ValidationError):
        Imputation(strategy=strategy, default_value=0)


def test_imputation_default_requires_default_value():
    with pytest.raises(ValidationError) as exc:
        Imputation(strategy="default")
    assert "default_value" in str(exc.value)


@pytest.mark.parametrize(
    "default_value", [0, 0.0, "UNKNOWN", True, -1.5, "", 1_000_000]
)
def test_imputation_default_accepts_typed_values(default_value):
    imp = Imputation(strategy="default", default_value=default_value)
    assert imp.strategy == "default"
    assert imp.default_value == default_value


def test_imputation_unknown_strategy_rejected():
    with pytest.raises(ValidationError):
        Imputation(strategy="mode")


def _roundtrip_constraints(fc: FieldConstraints) -> FieldConstraints:
    return FieldConstraints.from_proto(fc.to_proto())


def test_field_constraints_proto_roundtrip_empty():
    fc = FieldConstraints()
    assert _roundtrip_constraints(fc) == fc


def test_field_constraints_proto_roundtrip_strict_id():
    fc = FieldConstraints(nullable=False)
    assert _roundtrip_constraints(fc) == fc


def test_field_constraints_proto_roundtrip_numeric_range():
    fc = FieldConstraints(
        nullable=False,
        min_value=0,
        max_value=1,
        min_compliance=0.999,
    )
    assert _roundtrip_constraints(fc) == fc


def test_field_constraints_proto_roundtrip_categorical():
    fc = FieldConstraints(
        nullable=False,
        allowed_values=["VISA", "MASTERCARD", "AMEX"],
    )
    assert _roundtrip_constraints(fc) == fc


def test_field_constraints_proto_roundtrip_regex_and_unique():
    fc = FieldConstraints(
        nullable=False,
        regex=r"^[A-Z0-9]{8}$",
        unique=True,
    )
    assert _roundtrip_constraints(fc) == fc


def test_field_constraints_proto_roundtrip_custom_predicates():
    fc = FieldConstraints(
        custom={
            "amount_under_cap": "amount < 1000000",
            "valid_iso_country": "country IN ('US', 'GB', 'CA')",
        },
    )
    assert _roundtrip_constraints(fc) == fc


@pytest.mark.parametrize("predicate", ["", "   ", "\t"])
def test_custom_rejects_empty_predicate(predicate):
    with pytest.raises(ValidationError) as exc:
        FieldConstraints(custom={"amount_cap": predicate})
    assert "must not be empty" in str(exc.value)


@pytest.mark.parametrize("name", ["", "   "])
def test_custom_rejects_empty_name(name):
    with pytest.raises(ValidationError) as exc:
        FieldConstraints(custom={name: "amount < 100"})
    assert "names must not be empty" in str(exc.value)


def test_custom_rejects_empty_map():
    with pytest.raises(ValidationError) as exc:
        FieldConstraints(custom={})
    assert "must not be empty if set" in str(exc.value)


def test_field_constraints_proto_roundtrip_with_imputation_default_string():
    fc = FieldConstraints(
        nullable=False,
        imputation=Imputation(strategy="default", default_value="UNKNOWN"),
    )
    assert _roundtrip_constraints(fc) == fc


def test_field_constraints_proto_roundtrip_with_imputation_default_double():
    fc = FieldConstraints(
        imputation=Imputation(strategy="default", default_value=3.14),
    )
    assert _roundtrip_constraints(fc) == fc


def test_field_constraints_proto_roundtrip_with_imputation_default_bool():
    fc = FieldConstraints(
        imputation=Imputation(strategy="default", default_value=True),
    )
    assert _roundtrip_constraints(fc) == fc


def test_field_constraints_proto_roundtrip_with_imputation_default_long():
    fc = FieldConstraints(
        imputation=Imputation(strategy="default", default_value=42),
    )
    assert _roundtrip_constraints(fc) == fc


@pytest.mark.parametrize("strategy", ["mean", "median"])
def test_field_constraints_proto_roundtrip_with_imputation_mean_median(strategy):
    fc = FieldConstraints(
        nullable=False,
        min_value=0,
        imputation=Imputation(strategy=strategy),
    )
    assert _roundtrip_constraints(fc) == fc


def test_field_constraints_proto_roundtrip_kitchen_sink():
    """Every field set; round-trip preserves all of them."""
    fc = FieldConstraints(
        nullable=False,
        min_value=0,
        max_value=1,
        min_compliance=0.999,
        regex=r"^.+$",
        unique=False,
        custom={"k": "v"},
        imputation=Imputation(strategy="mean"),
    )
    assert _roundtrip_constraints(fc) == fc
