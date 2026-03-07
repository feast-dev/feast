from types import SimpleNamespace

import uvicorn

from feast.feature_server import get_app


class _FakeRegistry:
    def proto(self):
        return object()


class _FakeStore:
    def __init__(self):
        self.config = SimpleNamespace()
        self.registry = _FakeRegistry()
        self._provider = SimpleNamespace(
            async_supported=SimpleNamespace(
                online=SimpleNamespace(read=False, write=False)
            )
        )

    def _get_provider(self):
        return self._provider

    async def initialize(self):
        return None

    def refresh_registry(self):
        return None

    async def close(self):
        return None


if __name__ == "__main__":
    app = get_app(_FakeStore())
    uvicorn.run(app, host="0.0.0.0", port=6566, log_level="error")
