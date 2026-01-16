---
status: pending
priority: p2
issue_id: "006"
tags: [code-review, performance, offline-store, online-store]
dependencies: []
---

# No Catalog Connection Caching

## Problem Statement

Both offline and online stores create a new Iceberg catalog connection on every operation via `load_catalog()`. This adds 100-200ms latency overhead per request from TCP handshake, TLS negotiation, and authentication, especially for REST catalogs.

**Why it matters:**
- **High Latency:** 100-200ms penalty per operation
- **Connection Exhaustion:** Risk with REST/Glue catalogs under load
- **No Query Plan Caching:** DuckDB/Iceberg optimizations lost
- **Scalability:** Doesn't scale to high concurrent request volumes

## Findings

**Locations:**
- Offline: `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:144-147`
- Online: `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:323-334`

**Current Implementation (repeated on every call):**
```python
# Offline store
catalog = load_catalog(config.offline_store.catalog_name, **catalog_props)

# Online store
def _load_catalog(self, config: IcebergOnlineStoreConfig):
    return load_catalog(config.catalog_name, **catalog_config)
```

**Evidence from performance-oracle agent:**
- 100-200ms latency per catalog load for REST catalogs
- Redis/DynamoDB use `self._client` for persistent connections
- Projected impact: 100 concurrent requests = 100 catalog connections

## Proposed Solutions

### Solution 1: Class-Level Catalog Cache (Recommended)
**Pros:** Simple, thread-safe with proper key design
**Cons:** Memory usage for cached catalogs
**Effort:** Small
**Risk:** Low

**Implementation:**
```python
from typing import Dict, Any

class IcebergOnlineStore(OnlineStore):
    _catalog_cache: Dict[tuple, Any] = {}
    _cache_lock = threading.Lock()
    
    @classmethod
    def _get_cached_catalog(cls, config: IcebergOnlineStoreConfig):
        cache_key = (
            config.catalog_type,
            config.catalog_name,
            config.uri,
            config.warehouse,
            frozenset(config.storage_options.items()),
        )
        
        with cls._cache_lock:
            if cache_key not in cls._catalog_cache:
                catalog_config = {
                    "type": config.catalog_type,
                    "warehouse": config.warehouse,
                    **config.storage_options,
                }
                if config.uri:
                    catalog_config["uri"] = config.uri
                cls._catalog_cache[cache_key] = load_catalog(
                    config.catalog_name, **catalog_config
                )
        
        return cls._catalog_cache[cache_key]
```

### Solution 2: Instance-Level Connection Pool
**Pros:** Fine-grained control, connection lifecycle management
**Cons:** More complex, needs TTL/refresh logic
**Effort:** Medium
**Risk:** Medium

## Recommended Action

Implement Solution 1 for both offline and online stores.

## Acceptance Criteria

- [ ] Catalog cache implemented with frozen config tuple as key
- [ ] Thread-safe access with lock
- [ ] Benchmark shows 100-200ms improvement per operation
- [ ] Cache invalidation on config change tested

## Work Log

**2026-01-16:** Identified by performance-oracle agent

## Resources

- Performance review: `/home/tommyk/.claude/plans/mellow-petting-kettle.md`
