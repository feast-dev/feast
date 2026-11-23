"""Choosing what survives when a prompt does not fit its budget.

Prompt assembly is a packing problem: a user profile, a watch history and a
catalogue of candidates rarely fit together in a context window, and something
has to give. :func:`priority_select` decides what, keeping sections from the
most important down until the budget is spent::

    selection = priority_select(
        [
            ("critical", system_instructions),
            ("high", f"User profile: {profile}"),
            ("medium", f"Recent history: {history}"),
            ("low", f"Device: {device}"),
        ],
        TokenBudget.of(4096),
    )
    prompt = selection.render()

The decision is deterministic in the sections and the budget alone, so an
OnDemandFeatureView that calls it drops the same sections during training as it
does at serving time.
"""

from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Iterable, Iterator, Optional, Union

from feast.context.errors import RequiredSectionError
from feast.context.token_budget import TokenBudget

#: Placed between kept sections when a selection is rendered.
DEFAULT_SEPARATOR = "\n\n"

#: Characters of content quoted in messages about an unnamed section.
_LABEL_PREVIEW_CHARS = 40


class Priority(IntEnum):
    """How readily a section may be dropped to make room for another.

    Ordered, so ``Priority.HIGH > Priority.LOW``. Only these four levels are
    accepted: a plain number such as ``Priority.HIGH - 1`` is rejected.
    """

    #: Nice to have: dropped first.
    LOW = 10
    #: Useful context, expendable under pressure.
    MEDIUM = 20
    #: Kept unless the budget is genuinely tight.
    HIGH = 30
    #: Never dropped; the selection fails instead.
    CRITICAL = 40

    @classmethod
    def parse(cls, spec: "PrioritySpec") -> "Priority":
        """Resolve a priority or its case-insensitive name."""
        if isinstance(spec, Priority):
            return spec
        if isinstance(spec, str):
            try:
                return cls[spec.strip().upper()]
            except KeyError:
                pass
        raise ValueError(
            f"Unknown priority {spec!r}. Use one of: "
            f"{', '.join(level.name.lower() for level in sorted(cls, reverse=True))}."
        )

    def __str__(self) -> str:
        return self.name.lower()


#: What callers may pass wherever a priority is expected.
PrioritySpec = Union[str, Priority]


@dataclass(frozen=True)
class Section:
    """A candidate piece of a prompt, and how hard it is to lose.

    Attributes:
        content: The text itself. Blank content is ignored by selection.
        priority: How readily the section is dropped.
        name: Optional identifier, quoted in error messages and useful for
            asserting on what a selection kept.
    """

    content: str
    priority: Priority = Priority.MEDIUM
    name: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.content, str):
            raise TypeError(f"Section content must be a string, got {self.content!r}.")
        if not isinstance(self.priority, Priority):
            # A name from config lands here; resolve it once, so comparisons
            # and sorting see a Priority.
            object.__setattr__(self, "priority", Priority.parse(self.priority))

    @classmethod
    def of(cls, spec: "SectionSpec") -> "Section":
        """Normalize a section or a ``(priority, content[, name])`` tuple."""
        if isinstance(spec, Section):
            return spec
        if isinstance(spec, tuple) and len(spec) in (2, 3):
            priority, content, *rest = spec
            return cls(
                content=content,
                priority=Priority.parse(priority),
                name=rest[0] if rest else None,
            )
        raise TypeError(
            f"Expected a Section or a (priority, content) tuple, got {spec!r}."
        )

    @property
    def is_required(self) -> bool:
        """Whether the section must be kept, budget notwithstanding."""
        return self.priority is Priority.CRITICAL

    @property
    def label(self) -> str:
        """Quoted identifier for messages: the name, else a content preview."""
        if self.name:
            return repr(self.name)
        flattened = " ".join(self.content.split())
        if len(flattened) > _LABEL_PREVIEW_CHARS:
            return repr(f"{flattened[: _LABEL_PREVIEW_CHARS - 3]}...")
        return repr(flattened)


#: A section, or the tuple shorthand for one.
SectionSpec = Union[
    Section,
    tuple[PrioritySpec, str],
    tuple[PrioritySpec, str, Optional[str]],
]


class SectionOrder(str, Enum):
    """The order kept sections come back in."""

    #: As the caller listed them, so the prompt reads as it was written.
    DECLARED = "declared"
    #: Most important first, ties broken by declaration order.
    PRIORITY = "priority"


@dataclass(frozen=True)
class Selection:
    """The outcome of a :func:`priority_select`: what fit and what did not.

    Iterating yields the kept content, so a selection drops straight into a
    join or a template::

        for text in selection: ...
        prompt = selection.render()

    Attributes:
        budget: The budget after charging the rendered prompt.
        kept: Sections that made it in, ordered as requested.
        dropped: Sections left out, in declaration order.
        separator: What :meth:`render` joins kept sections with.
    """

    budget: TokenBudget
    kept: tuple[Section, ...] = ()
    dropped: tuple[Section, ...] = ()
    separator: str = DEFAULT_SEPARATOR

    @property
    def contents(self) -> tuple[str, ...]:
        """The text of each kept section."""
        return tuple(section.content for section in self.kept)

    @property
    def names(self) -> tuple[Optional[str], ...]:
        """The name of each kept section, None where unnamed."""
        return tuple(section.name for section in self.kept)

    @property
    def is_complete(self) -> bool:
        """Whether everything offered was kept."""
        return not self.dropped

    def render(self) -> str:
        """The kept sections joined by :attr:`separator`."""
        return self.separator.join(self.contents)

    def __iter__(self) -> Iterator[str]:
        return iter(self.contents)

    def __len__(self) -> int:
        return len(self.kept)

    def __str__(self) -> str:
        return self.render()


def priority_select(
    sections: Iterable[SectionSpec],
    budget: TokenBudget,
    *,
    separator: str = DEFAULT_SEPARATOR,
    order: SectionOrder = SectionOrder.DECLARED,
) -> Selection:
    """Keep as much of ``sections`` as ``budget`` allows, least important out first.

    Sections are considered from the highest priority down, ties in declaration
    order, and each is kept if what remains of the budget covers it. A section
    too large to fit is dropped and the scan continues, so a small low-priority
    section can still be kept after a large high-priority one was refused.

    Sections whose content is blank are ignored: they would only double a
    separator. ``critical`` sections are never dropped.

    Each section is charged on its own, plus ``separator`` between each pair
    kept. Joined, the sections can still cost more than that, because a
    tokenizer may split text differently across a boundary. So the rendered
    prompt is then measured as a whole, the least important section kept is
    dropped while it overflows, and the budget is charged for exactly what
    :meth:`Selection.render` returns.

    Args:
        sections: Sections, or ``(priority, content[, name])`` tuples.
        budget: The allowance to fill. Anything already consumed on it stays
            consumed; only what remains is available here.
        separator: Placed between kept sections by
            :meth:`Selection.render`, and charged against the budget.
        order: Whether kept sections come back in declaration or priority
            order.

    Returns:
        A :class:`Selection` holding the kept sections, the dropped ones, and
        the budget left over.

    Raises:
        RequiredSectionError: A ``critical`` section does not fit.
        TypeError: A section is neither a :class:`Section` nor a tuple.
        ValueError: A priority or an order is not a known name.
    """
    if not isinstance(separator, str):
        raise TypeError(f"separator must be a string, got {separator!r}.")
    order = SectionOrder(order)

    candidates = [
        section
        for section in (Section.of(spec) for spec in sections)
        if section.content.strip()
    ]
    separator_tokens = budget.count_tokens(separator)

    remaining = budget
    kept: list[tuple[int, Section]] = []
    dropped: list[tuple[int, Section]] = []
    # Most important first, and among equals whoever was listed first.
    by_importance = sorted(
        enumerate(candidates), key=lambda pair: (-pair[1].priority, pair[0])
    )

    for position, section in by_importance:
        cost = remaining.count_tokens(section.content)
        if kept:
            cost += separator_tokens
        if remaining.fits_tokens(cost):
            remaining = remaining.consume_tokens(cost)
            kept.append((position, section))
        elif section.is_required:
            raise RequiredSectionError(
                section.label, cost, remaining.remaining, remaining.max_tokens
            )
        else:
            dropped.append((position, section))

    # Counted one at a time, the sections can cost less than the prompt they
    # make: cl100k_base counts "Hello!\r\n", "\n\n" and "World" as four tokens
    # apart but five joined. Measure the prompt as it renders, and give up the
    # least important section kept until it fits.
    cost = budget.count_tokens(_render(kept, order, separator))
    while not budget.fits_tokens(cost):
        position, section = kept.pop()
        if section.is_required:
            raise RequiredSectionError(
                section.label, cost, budget.remaining, budget.max_tokens
            )
        dropped.append((position, section))
        cost = budget.count_tokens(_render(kept, order, separator))

    if order is SectionOrder.DECLARED:
        kept.sort(key=lambda pair: pair[0])
    dropped.sort(key=lambda pair: pair[0])

    return Selection(
        budget=budget.consume_tokens(cost),
        kept=tuple(section for _, section in kept),
        dropped=tuple(section for _, section in dropped),
        separator=separator,
    )


def _render(
    kept: list[tuple[int, Section]], order: SectionOrder, separator: str
) -> str:
    """Join kept sections as :meth:`Selection.render` will."""
    if order is SectionOrder.DECLARED:
        kept = sorted(kept, key=lambda pair: pair[0])
    return separator.join(section.content for _, section in kept)
