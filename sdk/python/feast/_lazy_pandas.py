from __future__ import annotations

from types import ModuleType


class _LazyPandasProxy:
    """Load pandas only when a code path actually touches it."""

    _module: ModuleType | None = None

    def _load(self) -> ModuleType:
        if self._module is None:
            import pandas as pandas_module

            self._module = pandas_module
        return self._module

    def __getattr__(self, name: str):
        return getattr(self._load(), name)


pd = _LazyPandasProxy()
