import dataclasses

import pytest

from feast.context import (
    Priority,
    Section,
    SectionOrder,
    TokenBudget,
    priority_select,
)
from feast.context.errors import RequiredSectionError, TokenBudgetExceededError
from feast.context.tokenizer import ApproximateTokenizer

# Four characters per token: "abcd" is one token under the estimate.
ESTIMATING = ApproximateTokenizer()


def budget(max_tokens: int = 100, consumed: int = 0) -> TokenBudget:
    """A budget on the deterministic estimate, so counts need no tiktoken."""
    return TokenBudget(max_tokens=max_tokens, tokenizer=ESTIMATING, consumed=consumed)


def text(tokens: int, letter: str = "a") -> str:
    """Content costing exactly ``tokens`` tokens under the estimate."""
    return letter * (tokens * 4)


class TestPriority:
    def test_orders_from_critical_down(self):
        assert Priority.CRITICAL > Priority.HIGH > Priority.MEDIUM > Priority.LOW

    def test_parses_a_name_whatever_its_case(self):
        assert Priority.parse("critical") is Priority.CRITICAL
        assert Priority.parse(" High ") is Priority.HIGH

    def test_passes_a_priority_through(self):
        assert Priority.parse(Priority.LOW) is Priority.LOW

    def test_unknown_priority_lists_the_known_ones(self):
        with pytest.raises(ValueError, match="critical, high, medium, low"):
            Priority.parse("urgent")

    def test_reads_as_its_lowercase_name(self):
        assert f"{Priority.MEDIUM}" == "medium"

    def test_rejects_a_number_between_levels(self):
        with pytest.raises(ValueError, match="Unknown priority 29"):
            Section("body", Priority.HIGH - 1)  # type: ignore[arg-type]


class TestSection:
    def test_defaults_to_medium(self):
        assert Section("body").priority is Priority.MEDIUM

    def test_resolves_a_priority_name(self):
        assert Section("body", "high").priority is Priority.HIGH  # type: ignore[arg-type]

    def test_rejects_non_string_content(self):
        with pytest.raises(TypeError, match="content must be a string"):
            Section(42)  # type: ignore[arg-type]

    def test_is_immutable(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            Section("body").content = "other"  # type: ignore[misc]

    def test_of_builds_from_a_priority_and_content(self):
        assert Section.of(("low", "body")) == Section("body", Priority.LOW)

    def test_of_builds_from_a_named_triple(self):
        section = Section.of(("high", "body", "profile"))
        assert section == Section("body", Priority.HIGH, "profile")

    def test_of_passes_a_section_through(self):
        section = Section("body")
        assert Section.of(section) is section

    def test_of_rejects_anything_else(self):
        with pytest.raises(TypeError, match="Section or a"):
            Section.of("body")  # type: ignore[arg-type]

    def test_only_critical_sections_are_required(self):
        assert Section("body", Priority.CRITICAL).is_required is True
        assert Section("body", Priority.HIGH).is_required is False

    def test_label_prefers_the_name(self):
        assert Section("body", name="profile").label == "'profile'"

    def test_label_falls_back_to_a_flattened_preview(self):
        assert Section("two\n  words").label == "'two words'"

    def test_label_truncates_a_long_preview(self):
        label = Section("word " * 20).label
        assert label.endswith("...'")
        assert len(label) == len("''") + 40


class TestSelects:
    def test_keeps_everything_that_fits(self):
        selection = priority_select([("high", text(2)), ("low", text(2))], budget(10))
        assert selection.contents == (text(2), text(2))
        assert selection.is_complete is True
        assert selection.dropped == ()

    def test_keeps_declaration_order_by_default(self):
        selection = priority_select(
            [("low", "third"), ("critical", "first"), ("medium", "second")],
            budget(100),
        )
        assert selection.contents == ("third", "first", "second")

    def test_orders_by_priority_when_asked(self):
        selection = priority_select(
            [("low", "third"), ("critical", "first"), ("medium", "second")],
            budget(100),
            order=SectionOrder.PRIORITY,
        )
        assert selection.contents == ("first", "second", "third")

    def test_accepts_an_order_by_name(self):
        selection = priority_select(
            [("low", "second"), ("high", "first")], budget(100), order="priority"
        )
        assert selection.contents == ("first", "second")

    def test_rejects_an_unknown_order(self):
        with pytest.raises(ValueError, match="alphabetical"):
            priority_select([], budget(), order="alphabetical")

    def test_drops_the_least_important_first(self):
        selection = priority_select(
            [
                ("high", text(2, "h")),
                ("medium", text(2, "m")),
                ("low", text(2, "l")),
            ],
            budget(4),
            separator="",
        )
        assert selection.contents == (text(2, "h"), text(2, "m"))
        assert selection.dropped == (Section(text(2, "l"), Priority.LOW),)
        assert selection.is_complete is False

    def test_keeps_a_critical_section_listed_last(self):
        selection = priority_select(
            [("high", text(2, "h")), ("critical", text(2, "c"))],
            budget(2),
            separator="",
        )
        assert selection.contents == (text(2, "c"),)
        assert selection.dropped == (Section(text(2, "h"), Priority.HIGH),)

    def test_breaks_ties_by_declaration_order(self):
        selection = priority_select(
            [("medium", text(2, "a")), ("medium", text(2, "b"))],
            budget(2),
            separator="",
        )
        assert selection.contents == (text(2, "a"),)

    def test_keeps_a_smaller_section_after_refusing_a_larger_one(self):
        selection = priority_select(
            [("high", text(5, "h")), ("medium", text(2, "m")), ("low", text(1, "l"))],
            budget(3),
            separator="",
        )
        assert selection.contents == (text(2, "m"), text(1, "l"))
        assert selection.budget.is_exhausted is True

    def test_ignores_blank_sections(self):
        selection = priority_select(
            [("critical", "   \n "), ("high", "body"), ("low", "")], budget(100)
        )
        assert selection.contents == ("body",)
        assert selection.dropped == ()

    def test_selects_nothing_from_nothing(self):
        selection = priority_select([], budget(10))
        assert selection.contents == ()
        assert selection.is_complete is True
        assert selection.budget == budget(10)

    def test_accepts_sections_as_well_as_tuples(self):
        selection = priority_select(
            [Section("body", Priority.HIGH, "profile"), ("low", "extra")], budget(100)
        )
        assert selection.names == ("profile", None)

    def test_leaves_the_original_budget_untouched(self):
        original = budget(10)
        priority_select([("high", text(4))], original, separator="")
        assert original.consumed == 0


class TestBudgetAccounting:
    def test_charges_the_kept_sections(self):
        selection = priority_select(
            [("high", text(3)), ("low", text(2))], budget(10), separator=""
        )
        assert selection.budget.consumed == 5
        assert selection.budget.remaining == 5

    def test_spends_only_what_is_left_of_a_used_budget(self):
        selection = priority_select(
            [("high", text(3)), ("low", text(3))],
            budget(10, consumed=6),
            separator="",
        )
        assert selection.contents == (text(3),)
        assert selection.budget.remaining == 1

    def test_charges_one_separator_between_kept_sections(self):
        # "\n\n" is one token under the estimate: three sections cost two.
        selection = priority_select(
            [("high", text(3)), ("high", text(3)), ("high", text(3))], budget(100)
        )
        assert selection.budget.consumed == 3 * 3 + 2

    def test_charges_nothing_for_an_empty_separator(self):
        selection = priority_select(
            [("high", text(3)), ("high", text(3))], budget(100), separator=""
        )
        assert selection.budget.consumed == 6

    def test_the_rendered_prompt_fits_the_budget(self):
        selection = priority_select(
            [("high", text(3, "h")), ("medium", text(3, "m")), ("low", text(3, "l"))],
            budget(10),
        )
        rendered = selection.render()
        assert selection.contents == (text(3, "h"), text(3, "m"))
        assert rendered == f"{text(3, 'h')}\n\n{text(3, 'm')}"
        assert ESTIMATING.count_tokens(rendered) <= 10
        assert selection.budget.consumed == ESTIMATING.count_tokens(rendered)

    def test_rejects_a_non_string_separator(self):
        with pytest.raises(TypeError, match="separator must be a string"):
            priority_select([], budget(), separator=None)  # type: ignore[arg-type]


class TestRequiredSections:
    def test_raises_when_a_critical_section_does_not_fit(self):
        with pytest.raises(RequiredSectionError, match="'rules' is critical"):
            priority_select([("critical", text(5), "rules")], budget(4), separator="")

    def test_raises_when_critical_sections_together_overflow(self):
        with pytest.raises(RequiredSectionError, match="'second'"):
            priority_select(
                [("critical", text(3), "first"), ("critical", text(3), "second")],
                budget(5),
                separator="",
            )

    def test_reports_the_shortfall(self):
        with pytest.raises(RequiredSectionError) as excinfo:
            priority_select([("critical", text(5))], budget(4), separator="")
        error = excinfo.value
        assert error.requested == 5
        assert error.remaining == 4
        assert error.max_tokens == 4
        assert error.section == f"'{text(5)}'"

    def test_is_a_token_budget_error(self):
        # Callers already guarding prompt assembly keep catching one type.
        assert issubclass(RequiredSectionError, TokenBudgetExceededError)


class TestSelection:
    @pytest.fixture
    def selection(self):
        return priority_select(
            [("high", "first", "profile"), ("low", "second")], budget(100)
        )

    def test_renders_sections_joined_by_the_separator(self, selection):
        assert selection.render() == "first\n\nsecond"

    def test_renders_as_its_string(self, selection):
        assert f"{selection}" == "first\n\nsecond"

    def test_iterates_over_the_kept_content(self, selection):
        assert list(selection) == ["first", "second"]
        assert " | ".join(selection) == "first | second"

    def test_counts_the_kept_sections(self, selection):
        assert len(selection) == 2

    def test_exposes_the_kept_names(self, selection):
        assert selection.names == ("profile", None)

    def test_is_immutable(self, selection):
        with pytest.raises(dataclasses.FrozenInstanceError):
            selection.kept = ()


class TestTiktokenSelection:
    def test_the_rendered_prompt_fits_an_exact_budget(self):
        pytest.importorskip("tiktoken")
        sections = [
            ("critical", "You are a helpful recommendation assistant."),
            ("high", "The user enjoys slow cinema and long documentaries."),
            ("medium", "Recently watched: " + "an eight part nature series, " * 20),
            ("low", "Device: living room television. Time: Sunday evening."),
        ]
        selection = priority_select(sections, TokenBudget.of(64))
        assert selection.is_complete is False
        assert selection.budget.count_tokens(selection.render()) <= 64
        assert selection.contents[0] == sections[0][1]
