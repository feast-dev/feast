import builtins

import pytest

from feast.context import tokenizer as tokenizer_module


@pytest.fixture
def no_tiktoken(monkeypatch):
    """Make ``import tiktoken`` fail, as it does when it is not installed."""
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "tiktoken":
            raise ImportError("No module named 'tiktoken'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    # Drop tokenizers an earlier test may have built and cached.
    tokenizer_module._build_tokenizer.cache_clear()
    yield
    tokenizer_module._build_tokenizer.cache_clear()
