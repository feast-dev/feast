"""Tokenizers and how a name resolves to one.

Resolution follows the same convention as online store types in
``repo_config``: a built-in name maps to a class path, and anything else is
itself the fully-qualified path of a :class:`Tokenizer` subclass with a
no-argument constructor::

    TokenBudget(max_tokens=4096, tokenizer="cl100k_base")
    TokenBudget(max_tokens=4096, tokenizer="my_pkg.tokenizers.LlamaTokenizer")
"""

import logging
import math
from abc import ABC, abstractmethod
from enum import Enum
from functools import lru_cache
from typing import Any, Union

from feast.context.errors import TokenizerNotFoundError, TokenizerUnavailableError
from feast.errors import FeastInvalidBaseClass
from feast.importer import import_class

logger = logging.getLogger(__name__)

#: Average characters per token for English prose.
DEFAULT_CHARS_PER_TOKEN = 4

#: Quoted in error messages when tiktoken is missing.
TIKTOKEN_INSTALL_HINT = "pip install tiktoken"


class TokenizerName(str, Enum):
    """Built-in tokenizer names."""

    #: GPT-4, GPT-3.5-turbo, text-embedding-3-*.
    CL100K_BASE = "cl100k_base"
    #: GPT-4o and the o-series models.
    O200K_BASE = "o200k_base"
    #: Character estimate; no third-party dependency.
    APPROXIMATE = "approximate"


TOKENIZER_CLASS_FOR_TYPE = {
    TokenizerName.CL100K_BASE.value: "feast.context.tokenizer.Cl100kBaseTokenizer",
    TokenizerName.O200K_BASE.value: "feast.context.tokenizer.O200kBaseTokenizer",
    TokenizerName.APPROXIMATE.value: "feast.context.tokenizer.ApproximateTokenizer",
}


class Tokenizer(ABC):
    """Counts the tokens a model would consume for a piece of text.

    Implementations must be stateless and thread-safe: one instance per name is
    shared across every :class:`TokenBudget`. A third-party subclass needs a
    no-argument constructor and a class name ending in ``Tokenizer``, which is
    what resolution by class path looks for.

    Two tokenizers that count identically compare equal, so budgets built from
    the same name are interchangeable as dict keys and set members.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Identifier reported in budgets and logs."""

    @abstractmethod
    def count_tokens(self, text: str) -> int:
        """Number of tokens in ``text``. Returns 0 for the empty string."""

    def _identity(self) -> tuple[object, ...]:
        """Whatever makes two instances count the same way."""
        return (self.name,)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Tokenizer) and self._identity() == other._identity()

    def __hash__(self) -> int:
        return hash(self._identity())

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r})"


class ApproximateTokenizer(Tokenizer):
    """Character-count estimate used when no real tokenizer is available.

    Counts ``ceil(len(text) / chars_per_token)``: close enough for budgeting,
    not for exact context-window arithmetic. Code and non-Latin scripts drift
    furthest from the ratio.
    """

    def __init__(self, chars_per_token: int = DEFAULT_CHARS_PER_TOKEN) -> None:
        if chars_per_token <= 0:
            raise ValueError(
                f"chars_per_token must be positive, got {chars_per_token}."
            )
        self._chars_per_token = chars_per_token

    @property
    def name(self) -> str:
        return TokenizerName.APPROXIMATE.value

    @property
    def chars_per_token(self) -> int:
        return self._chars_per_token

    def count_tokens(self, text: str) -> int:
        return math.ceil(len(text) / self._chars_per_token)

    def _identity(self) -> tuple[object, ...]:
        return (self.name, self._chars_per_token)


class TiktokenTokenizer(Tokenizer):
    """Exact token counts from a tiktoken encoding.

    Raises:
        TokenizerUnavailableError: tiktoken is missing or the encoding failed
            to load.
        TokenizerNotFoundError: tiktoken does not know ``encoding_name``.
    """

    def __init__(self, encoding_name: str) -> None:
        self._encoding_name = encoding_name
        self._encoding = _load_tiktoken_encoding(encoding_name)

    @property
    def name(self) -> str:
        return self._encoding_name

    def count_tokens(self, text: str) -> int:
        if not text:
            return 0
        # Feature values are arbitrary text: count a literal "<|endoftext|>"
        # rather than raise, which is tiktoken's default for special tokens.
        return len(self._encoding.encode(text, disallowed_special=()))


class Cl100kBaseTokenizer(TiktokenTokenizer):
    """cl100k_base, in the no-argument form that resolution by name needs."""

    def __init__(self) -> None:
        super().__init__(TokenizerName.CL100K_BASE.value)


class O200kBaseTokenizer(TiktokenTokenizer):
    """o200k_base, in the no-argument form that resolution by name needs."""

    def __init__(self) -> None:
        super().__init__(TokenizerName.O200K_BASE.value)


def _load_tiktoken_encoding(encoding_name: str) -> Any:
    """Load a tiktoken encoding. tiktoken memoizes these internally."""
    try:
        import tiktoken
    except ImportError as e:
        raise TokenizerUnavailableError(
            encoding_name, f"tiktoken is not installed ({e})", TIKTOKEN_INSTALL_HINT
        ) from e

    try:
        return tiktoken.get_encoding(encoding_name)
    except ValueError as e:
        raise TokenizerNotFoundError(
            encoding_name, tiktoken.list_encoding_names()
        ) from e
    except Exception as e:
        # Encodings download on first use; an offline host fails here.
        raise TokenizerUnavailableError(
            encoding_name,
            f"tiktoken failed to load the encoding ({e})",
            TIKTOKEN_INSTALL_HINT,
        ) from e


#: What callers may pass wherever a tokenizer is expected.
TokenizerSpec = Union[str, TokenizerName, Tokenizer]


def get_tokenizer(
    spec: TokenizerSpec = TokenizerName.CL100K_BASE,
    *,
    fallback: bool = True,
) -> Tokenizer:
    """Resolve ``spec`` to a tokenizer instance.

    A :class:`Tokenizer` passes through; a name is built once, then cached.

    Args:
        spec: Tokenizer instance, built-in name, or the class path of a
            ``Tokenizer`` subclass with a no-argument constructor.
        fallback: On missing dependencies, return
            :class:`ApproximateTokenizer` with a warning instead of raising.
            Set False when exact counts are required.

    Raises:
        TokenizerNotFoundError: ``spec`` is neither a built-in name nor a
            class path ending in ``Tokenizer``.
        TokenizerUnavailableError: the tokenizer cannot be built and
            ``fallback`` is False.
    """
    if isinstance(spec, Tokenizer):
        return spec
    name = spec.value if isinstance(spec, TokenizerName) else spec
    if not isinstance(name, str) or not name.strip():
        raise TypeError(f"Tokenizer name must be a non-empty string, got {spec!r}.")

    try:
        return _build_tokenizer(name.strip())
    except TokenizerUnavailableError as e:
        if not fallback:
            raise
        logger.warning(
            "%s Falling back to a character-based estimate; token counts will "
            "be approximate.",
            e,
        )
        # Through the cache, so budgets that fell back still compare equal.
        return _build_tokenizer(TokenizerName.APPROXIMATE.value)


@lru_cache(maxsize=None)
def _build_tokenizer(name: str) -> Tokenizer:
    """Import and instantiate the tokenizer ``name`` refers to."""
    tokenizer_type = TOKENIZER_CLASS_FOR_TYPE.get(name, name)
    if "." not in tokenizer_type or not tokenizer_type.endswith("Tokenizer"):
        raise TokenizerNotFoundError(name, TOKENIZER_CLASS_FOR_TYPE.keys())

    module_name, class_name = tokenizer_type.rsplit(".", 1)
    tokenizer = import_class(module_name, class_name, "Tokenizer")()
    if not isinstance(tokenizer, Tokenizer):
        # import_class checks the base class by name, so an unrelated class
        # called "Tokenizer" gets this far.
        raise FeastInvalidBaseClass(tokenizer_type, "Tokenizer")
    return tokenizer
