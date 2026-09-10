"""Token-budget-aware helpers for assembling LLM context in feature views.

Plain functions over immutable values, so an OnDemandFeatureView calling them
gets the same prompt offline and online.
"""

from feast.context.errors import (
    ContextError,
    TokenBudgetExceededError,
    TokenizerNotFoundError,
    TokenizerUnavailableError,
)
from feast.context.token_budget import TokenBudget
from feast.context.tokenizer import (
    ApproximateTokenizer,
    Cl100kBaseTokenizer,
    O200kBaseTokenizer,
    TiktokenTokenizer,
    Tokenizer,
    TokenizerName,
    TokenizerSpec,
    get_tokenizer,
)

__all__ = [
    "ApproximateTokenizer",
    "Cl100kBaseTokenizer",
    "ContextError",
    "O200kBaseTokenizer",
    "TiktokenTokenizer",
    "TokenBudget",
    "TokenBudgetExceededError",
    "Tokenizer",
    "TokenizerName",
    "TokenizerNotFoundError",
    "TokenizerSpec",
    "TokenizerUnavailableError",
    "get_tokenizer",
]
