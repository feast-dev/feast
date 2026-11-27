import dataclasses
import inspect

import pytest

from feast.context import TokenBudget
from feast.context.errors import TokenBudgetExceededError, TokenizerNotFoundError
from feast.context.tokenizer import (
    ApproximateTokenizer,
    Tokenizer,
    TokenizerName,
    get_tokenizer,
)

# Four characters per token: "abcd" is one token under the estimate.
ESTIMATING = ApproximateTokenizer()


class HalvingTokenizer(Tokenizer):
    """A third-party tokenizer, resolved by class path."""

    @property
    def name(self) -> str:
        return "halving"

    def count_tokens(self, text: str) -> int:
        return len(text) // 2


def budget(max_tokens: int = 100, consumed: int = 0) -> TokenBudget:
    """A budget on the deterministic estimate, so counts need no tiktoken."""
    return TokenBudget(max_tokens=max_tokens, tokenizer=ESTIMATING, consumed=consumed)


class TestConstruction:
    def test_defaults_to_cl100k_base(self):
        field = TokenBudget.__dataclass_fields__["tokenizer"]
        assert field.default_factory is get_tokenizer
        assert (
            inspect.signature(get_tokenizer).parameters["spec"].default
            is TokenizerName.CL100K_BASE
        )

    def test_accepts_a_tokenizer_name(self):
        assert TokenBudget.of(10, "approximate").tokenizer is get_tokenizer(
            "approximate"
        )

    def test_accepts_a_tokenizer_instance(self):
        assert TokenBudget(max_tokens=10, tokenizer=ESTIMATING).tokenizer is ESTIMATING

    def test_resolves_a_name_passed_to_the_constructor(self):
        # Config supplies a string; the field still holds a Tokenizer.
        resolved = TokenBudget(max_tokens=10, tokenizer="approximate").tokenizer  # type: ignore[arg-type]
        assert resolved is get_tokenizer("approximate")

    def test_resolves_a_custom_tokenizer_by_class_path(self):
        b = TokenBudget.of(10, "tests.unit.context.test_token_budget.HalvingTokenizer")
        assert b.tokenizer_name == "halving"
        assert b.count_tokens("abcdefgh") == 4
        assert b.consume("abcdefgh").remaining == 6

    def test_unknown_tokenizer_raises(self):
        with pytest.raises(TokenizerNotFoundError):
            TokenBudget.of(10, "gpt-42")

    def test_rejects_negative_max_tokens(self):
        with pytest.raises(ValueError, match="max_tokens"):
            budget(max_tokens=-1)

    def test_rejects_negative_consumed(self):
        with pytest.raises(ValueError, match="consumed"):
            budget(consumed=-1)

    def test_rejects_consumed_above_max_tokens(self):
        with pytest.raises(ValueError, match="must not exceed"):
            budget(max_tokens=10, consumed=11)

    def test_is_immutable(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            budget().max_tokens = 5  # type: ignore[misc]

    def test_budgets_with_the_same_state_are_equal_and_hash_alike(self):
        # Default-constructed, so a tokenizer compared by identity would fail.
        assert TokenBudget(max_tokens=10) == TokenBudget(max_tokens=10)
        assert len({TokenBudget(max_tokens=10), TokenBudget(max_tokens=10)}) == 1

    def test_replace_keeps_the_resolved_tokenizer(self):
        replaced = dataclasses.replace(budget(max_tokens=10), consumed=4)
        assert replaced.tokenizer is ESTIMATING
        assert replaced.remaining == 6


class TestCounting:
    def test_counts_tokens(self):
        assert budget().count_tokens("abcdefgh") == 2

    def test_empty_string_costs_nothing(self):
        assert budget().count_tokens("") == 0

    def test_is_approximate_flags_the_estimate(self):
        assert budget().is_approximate is True


class TestFits:
    @pytest.mark.parametrize(
        "max_tokens, text, expected",
        [
            (10, "abcdefgh", True),
            (2, "abcdefgh", True),  # exactly filling the budget fits
            (1, "a" * 40, False),
            (0, "", True),  # nothing always fits
        ],
    )
    def test_fits_compares_against_what_remains(self, max_tokens, text, expected):
        assert budget(max_tokens=max_tokens).fits(text) is expected

    def test_fits_tokens_compares_against_what_remains(self):
        b = budget(max_tokens=10, consumed=8)
        assert b.fits_tokens(2) is True
        assert b.fits_tokens(3) is False

    def test_fits_tokens_rejects_negative_counts(self):
        with pytest.raises(ValueError, match="tokens"):
            budget().fits_tokens(-1)


class TestConsume:
    def test_returns_a_new_budget(self):
        original = budget(max_tokens=10)
        after = original.consume("abcdefgh")
        assert after is not original
        assert original.remaining == 10
        assert after.remaining == 8
        assert after.consumed == 2

    def test_exhausts_the_budget_exactly(self):
        after = budget(max_tokens=2).consume("abcdefgh")
        assert after.remaining == 0
        assert after.is_exhausted is True

    def test_over_budget_raises_and_leaves_the_original_untouched(self):
        b = budget(max_tokens=2)
        with pytest.raises(TokenBudgetExceededError, match="only 2 of 2 tokens remain"):
            b.consume("a" * 40)
        assert b.remaining == 2

    def test_consume_tokens_charges_a_count_directly(self):
        assert budget(max_tokens=10).consume_tokens(4).remaining == 6

    def test_consume_tokens_rejects_negative_counts(self):
        with pytest.raises(ValueError, match="tokens"):
            budget().consume_tokens(-1)


class TestTryConsume:
    def test_returns_the_new_budget_when_content_fits(self):
        after = budget(max_tokens=10).try_consume("abcdefgh")
        assert after is not None
        assert after.remaining == 8

    def test_returns_none_when_content_does_not_fit(self):
        assert budget(max_tokens=2).try_consume("a" * 40) is None

    def test_greedy_selection_keeps_what_fits(self):
        b = budget(max_tokens=3)
        kept = []
        for section in ["abcd", "a" * 40, "abcdefgh"]:
            candidate = b.try_consume(section)
            if candidate is not None:
                b = candidate
                kept.append(section)
        assert kept == ["abcd", "abcdefgh"]
        assert b.remaining == 0


class TestReset:
    def test_clears_consumption_and_keeps_limit_and_tokenizer(self):
        after = budget(max_tokens=10).consume("abcdefgh").reset()
        assert after.consumed == 0
        assert after == budget(max_tokens=10)


class TestTiktokenBudget:
    @pytest.mark.parametrize(
        "name", [TokenizerName.CL100K_BASE.value, TokenizerName.O200K_BASE.value]
    )
    def test_counts_exact_tokens(self, name):
        pytest.importorskip("tiktoken")
        b = TokenBudget.of(4096, name)
        assert b.tokenizer_name == name
        assert b.is_approximate is False
        assert b.count_tokens("Hello world") == 2
        assert b.consume("Hello world").remaining == 4094

    def test_the_default_budget_counts_cl100k_base(self):
        pytest.importorskip("tiktoken")
        assert TokenBudget(max_tokens=4096).tokenizer_name == (
            TokenizerName.CL100K_BASE.value
        )
