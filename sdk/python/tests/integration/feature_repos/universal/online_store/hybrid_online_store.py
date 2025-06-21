from sdk.python.tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class HybridOnlineStoreCreator(OnlineStoreCreator):
    def create_online_store(self):
        # Use Redis and SQLite as two backends for demonstration/testing, but mock Redis config for unit tests
        return {
            "type": "hybrid_online_store.HybridOnlineStore",
            "online_stores": [
                {
                    "type": "redis",
                    "conf": {
                        "redis_type": "redis",
                        "connection_string": "localhost:6379"
                    }
                },
                {
                    "type": "sqlite",
                    "conf": {
                        "path": "/tmp/feast_hybrid_test.db"
                    }
                }
            ]
        }

    def teardown(self):
        # Implement any resource cleanup if needed (e.g., remove test DB files)
        pass
