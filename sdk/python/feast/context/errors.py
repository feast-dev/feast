from typing import Iterable

from fastapi import status as HttpStatusCode

from feast.errors import FeastError


class ContextError(FeastError):
    """Base class for all feast.context failures."""


class TokenizerNotFoundError(ContextError):
    def __init__(self, name: str, available: Iterable[str]):
        super().__init__(
            f"Unknown tokenizer '{name}'. Built-in tokenizers: "
            f"{', '.join(sorted(available))}. For your own, pass the class "
            f"path of a Tokenizer subclass, e.g. 'my_pkg.MyTokenizer'."
        )

    def http_status_code(self) -> int:
        return HttpStatusCode.HTTP_400_BAD_REQUEST


class TokenizerUnavailableError(ContextError):
    """A known tokenizer cannot be loaded.

    Covers a missing dependency, and one present but unable to load its
    encoding — tiktoken fetching a BPE file on a host with no network, say.
    """

    def __init__(self, name: str, reason: str, install_hint: str):
        super().__init__(
            f"Tokenizer '{name}' is unavailable: {reason}. Install its "
            f"dependencies with: {install_hint}"
        )
        self.tokenizer_name = name

    def http_status_code(self) -> int:
        return HttpStatusCode.HTTP_400_BAD_REQUEST


class TokenBudgetExceededError(ContextError):
    def __init__(self, requested: int, remaining: int, max_tokens: int):
        super().__init__(
            f"Cannot consume {requested} tokens: only {remaining} of "
            f"{max_tokens} tokens remain in the budget."
        )
        self.requested = requested
        self.remaining = remaining
        self.max_tokens = max_tokens

    def http_status_code(self) -> int:
        return HttpStatusCode.HTTP_400_BAD_REQUEST
