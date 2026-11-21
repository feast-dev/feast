"""Token accounting for prompt assembly."""

from dataclasses import dataclass, field
from typing import Optional

from feast.context.errors import TokenBudgetExceededError
from feast.context.tokenizer import (
    ApproximateTokenizer,
    Tokenizer,
    TokenizerName,
    TokenizerSpec,
    get_tokenizer,
)


@dataclass(frozen=True)
class TokenBudget:
    """An immutable token allowance measured with a specific tokenizer.

    ``consume()`` returns a new budget instead of mutating this one, so a
    budget is safe to share across threads and to reuse between assemblies::

        budget = TokenBudget.of(4096, "cl100k_base")
        budget.count_tokens("Hello world")   # -> 2
        budget = budget.consume("Hello world")
        budget.remaining                     # -> 4094

    Attributes:
        max_tokens: Size of the allowance. Zero admits only empty content.
        tokenizer: The tokenizer doing the counting. A name is accepted too
            and resolved at construction; :meth:`of` types that properly and
            can forbid the fallback to the estimate.
        consumed: Tokens already spent.
    """

    max_tokens: int
    tokenizer: Tokenizer = field(default_factory=get_tokenizer)
    consumed: int = 0

    def __post_init__(self) -> None:
        if self.max_tokens < 0:
            raise ValueError(f"max_tokens must not be negative, got {self.max_tokens}.")
        if self.consumed < 0:
            raise ValueError(f"consumed must not be negative, got {self.consumed}.")
        if self.consumed > self.max_tokens:
            raise ValueError(
                f"consumed ({self.consumed}) must not exceed max_tokens "
                f"({self.max_tokens})."
            )
        if not isinstance(self.tokenizer, Tokenizer):
            # A name from config lands here; resolve it once, so counting
            # never re-enters resolution and equality stays value-based.
            object.__setattr__(self, "tokenizer", get_tokenizer(self.tokenizer))

    @classmethod
    def of(
        cls,
        max_tokens: int,
        tokenizer: TokenizerSpec = TokenizerName.CL100K_BASE,
        *,
        consumed: int = 0,
        fallback: bool = True,
    ) -> "TokenBudget":
        """Build a budget from a tokenizer name, instance, or class path.

        Args:
            fallback: Set False to raise instead of degrading to the character
                estimate when the tokenizer cannot be loaded.
        """
        return cls(
            max_tokens=max_tokens,
            tokenizer=get_tokenizer(tokenizer, fallback=fallback),
            consumed=consumed,
        )

    @property
    def remaining(self) -> int:
        """Tokens still available."""
        return self.max_tokens - self.consumed

    @property
    def is_exhausted(self) -> bool:
        """Whether the allowance is fully spent."""
        return self.remaining == 0

    @property
    def tokenizer_name(self) -> str:
        """Name of the tokenizer actually counting."""
        return self.tokenizer.name

    @property
    def is_approximate(self) -> bool:
        """Whether counts are estimated rather than exact.

        True when the requested tokenizer could not be loaded and the budget
        fell back to counting characters.
        """
        return isinstance(self.tokenizer, ApproximateTokenizer)

    def count_tokens(self, text: str) -> int:
        """Tokens ``text`` would occupy, regardless of what remains."""
        return self.tokenizer.count_tokens(text)

    def fits(self, text: str) -> bool:
        """Whether ``text`` fits in the remaining allowance."""
        return self.count_tokens(text) <= self.remaining

    def fits_tokens(self, tokens: int) -> bool:
        """Whether ``tokens`` more tokens fit in the remaining allowance."""
        _check_not_negative(tokens)
        return tokens <= self.remaining

    def consume(self, text: str) -> "TokenBudget":
        """Charge ``text`` against the budget, returning the new budget.

        Raises:
            TokenBudgetExceededError: ``text`` does not fit; guard with
                ``fits()`` or use ``try_consume()``.
        """
        return self.consume_tokens(self.count_tokens(text))

    def consume_tokens(self, tokens: int) -> "TokenBudget":
        """Charge ``tokens`` against the budget, returning the new budget.

        Raises:
            TokenBudgetExceededError: ``tokens`` exceeds what remains.
        """
        if not self.fits_tokens(tokens):
            raise TokenBudgetExceededError(tokens, self.remaining, self.max_tokens)
        return self._replace_consumed(self.consumed + tokens)

    def try_consume(self, text: str) -> Optional["TokenBudget"]:
        """Charge ``text`` if it fits, else return None.

        Lets a caller keep whichever candidate sections still fit, without
        catching exceptions.
        """
        tokens = self.count_tokens(text)
        if not self.fits_tokens(tokens):
            return None
        return self._replace_consumed(self.consumed + tokens)

    def reset(self) -> "TokenBudget":
        """A budget with the same limit and tokenizer, nothing consumed."""
        return self._replace_consumed(0)

    def _replace_consumed(self, consumed: int) -> "TokenBudget":
        return TokenBudget(
            max_tokens=self.max_tokens,
            tokenizer=self.tokenizer,
            consumed=consumed,
        )

    def __str__(self) -> str:
        return (
            f"TokenBudget({self.consumed}/{self.max_tokens} tokens used, "
            f"tokenizer={self.tokenizer_name})"
        )


def _check_not_negative(tokens: int) -> None:
    if tokens < 0:
        raise ValueError(f"tokens must not be negative, got {tokens}.")
