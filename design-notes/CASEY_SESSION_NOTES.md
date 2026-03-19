# MongoDB Feast Integration — Session Notes
_Last updated: 2026-03-16. Resume here after OS upgrade._

---

## Status at a Glance

| Component | Branch | Status |
|---|---|---|
| **Online Store** | `INTPYTHON-297-MongoDB-Feast-Integration` | ✅ **Merged to upstream/master** |
| **Offline Store** | `FEAST-OfflineStore-INTPYTHON-297` | 🔧 In progress — next focus |

---

## Online Store — COMPLETE ✅

### What was done
- Implemented `MongoDBOnlineStore` with full sync + async API
- Refactored write path: extracted `_build_write_ops` static method to eliminate code
  duplication between `online_write_batch` and `online_write_batch_async`
- Added Feast driver metadata to MongoDB client instantiations
- Registered MongoDB in the feast-operator (kubebuilder enums, `ValidOnlineStoreDBStorePersistenceTypes`, operator YAMLs)
- Updated online store status from `alpha` → `preview` in docs
- All 5 unit tests pass (including Docker-based testcontainers integration test)

### Key files
- `sdk/python/feast/infra/online_stores/mongodb_online_store/mongodb.py` — main implementation
- `sdk/python/tests/unit/online_store/test_mongodb_online_retrieval.py` — test suite
- `sdk/python/tests/universal/feature_repos/universal/online_store/mongodb.py` — universal test repo config

### Git history cleanup (this session)
The PR had two merge commits (`632e103a6`, `26ce79b37`) that blocked squash-and-merge.
Resolution:
1. `git fetch --all`
2. Created clean branch `FEAST-OnlineStore-INTPYTHON-297` from `upstream/master`
3. Cherry-picked all 47 commits (oldest → newest), skipping the two merge commits
4. Resolved conflicts: directory rename (`tests/integration/` → `tests/universal/`),
   `pixi.lock` auto-resolved, `detect-secrets` false positives got `# pragma: allowlist secret`
5. Force-pushed to `INTPYTHON-297-MongoDB-Feast-Integration` — maintainer squash-merged ✅

### Versioning
Version is derived dynamically via `setuptools_scm` from git tags (no hardcoded version).
Latest tag at time of merge: **`v0.60.0`**. Feature ships in the next release after that.
Update JIRA with the next release tag once the maintainers cut it.

---

## Offline Store — IN PROGRESS 🔧

### Branch
```
FEAST-OfflineStore-INTPYTHON-297
```

### Commits on branch (not yet in upstream/master)
```
cd3eef677  Started work on full Mongo/MQL implementation. Kept MongoDBOfflineStoreIbis and MongoDBOfflineStoreNative
71469f69a  feat: restore test-python-universal-mongodb-online Makefile target
904505244  fix: pass onerror to pkgutil.walk_packages
946d84e4c  fix: broaden import exception handling in doctest runner
55de0e9b5  fix: catch FeastExtrasDependencyImportError in doctest runner
157a71d77  refactor: improve MongoDB offline store code quality
67632af2f  feat: Add MongoDB offline store (ibis-based PIT join, v1 alpha)
```

### Key files
- `sdk/python/feast/infra/offline_stores/contrib/mongodb_offline_store/mongodb.py`
  - Contains **two prototype implementations**:
    - `MongoDBOfflineStoreIbis` — uses Ibis for point-in-time joins (delegates to `get_historical_features_ibis`)
    - `MongoDBOfflineStoreNative` — native MQL implementation (started in `cd3eef677`, in progress)
- `sdk/python/feast/infra/offline_stores/contrib/mongodb_offline_store/mongodb_source.py` — `MongoDBSource` data source

### Architecture: Ibis vs Native
- **Ibis approach**: delegates PIT join to `feast.infra.offline_stores.ibis` helpers.
  Pro: less code, consistency with other ibis-backed stores.
  Con: requires ibis-mongodb connector; PIT correctness depends on ibis translation.
- **Native approach**: implements PIT join directly in MQL (MongoDB aggregation pipeline).
  Pro: no extra dependency, full control.
  Con: more complex; MQL aggregation pipelines can be verbose.
- Decision pending benchmarking / correctness validation between the two.

### Next steps for offline store
1. Finish `MongoDBOfflineStoreNative` MQL implementation (started in latest commit)
2. Validate PIT correctness for both implementations against the Feast universal test suite
3. Run: `make test-python-universal-mongodb-offline` (target may need creating — see `71469f69a`)
4. Choose Ibis vs Native based on results; remove the other
5. Add to operator (same pattern as online store: kubebuilder enums, install.yaml)
6. Open PR — follow same DCO + linear history discipline as online store

---

## Environment Notes

- **Python env**: always use `uv run pytest ...` (uses `.venv` in repo root, Python 3.11)
- **Do NOT use**: system Python (`/Library/Frameworks/Python.framework/...`) or conda envs
- **Docker**: must be running for the testcontainers integration test
- **Stale container**: `72d14b345b6a` (mongo:latest, port 57120) — leftover from testing, safe to stop
- **DCO**: all commits must be signed: `git commit -s`
- **No push/merge without explicit user approval**

---

## Git Workflow Reminder
To keep history clean (lesson from online store PR):
- Always branch from `upstream/master` (after `git fetch --all`)
- Never merge upstream into a feature branch — rebase or cherry-pick instead
- Before opening a PR, verify with: `git log --merges <branch> ^upstream/master --oneline`
  (must return empty)

