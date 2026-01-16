# Rescheduled Work Plan: P1 and P2 Bug Fixes

**Date Created:** 2026-01-16
**Status:** Planning
**Context:** 16 TODOs remain pending after API quota limits during parallel resolution

---

## Executive Summary

### Completed in Previous Session (5/21)
- ✅ **P0 Critical (2/2):** SQL injection, credential exposure
- ✅ **P1 Important (1/6):** Append-only documentation
- ✅ **P2 Moderate (2/13):** Created timestamp dedup, logger cleanup

### Remaining Work (16/21)

**P1 Important (5 items)** - ~8-10 hours estimated
- Performance bugs, validation gaps, test coverage

**P2 Moderate (11 items)** - ~15-20 hours estimated
- Code quality, optimizations, feature additions

---

## Priority 1 (P1) - High Impact Issues

### Immediate Priority (Next Session)

#### 1. Issue 019: MOR Double-Scan Bug 🔴 CRITICAL
**Status:** Pending
**Priority:** P1
**Type:** Performance + Correctness Bug
**Estimated Effort:** 30 minutes

**Problem:**
- `scan.plan_files()` called twice, causing:
  - 2x metadata I/O for every query
  - Generator exhaustion bug (file_paths = [])
  - Potential runtime failures on actual MOR tables

**Fix:**
```python
# Materialize once
scan_tasks = list(scan.plan_files())
has_deletes = any(task.delete_files for task in scan_tasks)
file_paths = [task.file.file_path for task in scan_tasks]
```

**Why Prioritize:** Both performance AND correctness bug affecting all queries

**Dependencies:** None
**Test Required:** Yes (verify single scan)
**Agent to Resume:** a5f8a39 (hit quota limit)

---

#### 2. Issue 020: TTL Value Validation
**Status:** Pending
**Priority:** P1
**Type:** Security - Input Validation
**Estimated Effort:** 30 minutes

**Problem:**
- TTL values not validated before SQL interpolation
- inf, nan, or negative TTLs could cause SQL errors
- Potential for system info leakage via error messages

**Fix:**
```python
if fv.ttl and fv.ttl.total_seconds() > 0:
    ttl_seconds = fv.ttl.total_seconds()

    # Validate bounds
    if not (1 <= ttl_seconds <= 31536000):  # 1 sec to 1 year
        raise ValueError(f"Invalid TTL: {fv.ttl}")

    if not math.isfinite(ttl_seconds):
        raise ValueError(f"Non-finite TTL: {ttl_seconds}")
```

**Why Prioritize:** Closes security gap in Issue 017 fix

**Dependencies:** [003] (resolved)
**Test Required:** Yes (edge cases: inf, nan, negative, zero, very large)
**Agent to Resume:** a4be230 (hit quota limit)

---

#### 3. Issue 021: Overly Broad Exception Handling
**Status:** Pending
**Priority:** P1
**Type:** Error Handling - Silent Failures
**Estimated Effort:** 1 hour

**Problem:**
- Bare `except Exception:` masks critical failures
- Authentication errors silently ignored
- Permission errors swallowed
- Makes debugging difficult

**Locations:**
- `iceberg.py:290-294` (online store table deletion)
- `iceberg.py:360-363` (namespace creation)

**Fix:**
```python
from pyiceberg.exceptions import (
    NoSuchTableError,
    NamespaceAlreadyExistsError,
)

# Table deletion - catch specific exceptions
try:
    catalog.drop_table(table_identifier)
except (NoSuchTableError, NoSuchNamespaceError):
    logger.debug(f"Table {table_identifier} not found")
# Let auth/network failures propagate!

# Namespace creation - catch specific exception only
try:
    catalog.create_namespace(config.namespace)
except NamespaceAlreadyExistsError:
    pass  # Expected
# Let auth/network failures propagate!
```

**Why Prioritize:** Production issues may go unnoticed

**Dependencies:** [015]
**Test Required:** Yes (verify auth failures propagate)
**Agent to Resume:** None (new work)

---

#### 4. Issue 022: Missing Test Coverage
**Status:** Pending
**Priority:** P1
**Type:** Quality Assurance
**Estimated Effort:** 2 hours

**Problem:**
3 critical bug fixes have no test coverage:
- Exception swallowing fix (015)
- Credential exposure fix (014)
- MOR detection optimization (009)

**Tests to Add:**

```python
# Test 1: Exception swallowing fix
def test_namespace_creation_propagates_permission_errors():
    mock_catalog.create_namespace.side_effect = PermissionError("Access denied")
    with pytest.raises(PermissionError, match="Access denied"):
        store._get_or_create_namespace(mock_catalog, config)

# Test 2: Credential exposure fix (ALREADY DONE in Issue 018!)
# Skip - TestCredentialSecurityFixes covers this

# Test 3: MOR detection optimization
def test_mor_detection_early_exit():
    # Mock 1000 files, delete at position 5
    # Verify only ~5 files checked (early exit)
    assert iteration_count[0] <= 10
```

**Why Prioritize:** Prevents regressions on critical fixes

**Dependencies:** [015, 014]
**Test Required:** N/A (this IS the test work)
**Agent to Resume:** None (new work)

---

#### 5. Issue 016: Duplicate Function
**Status:** Pending
**Priority:** P2 (originally), promoted to P1 for quick win
**Type:** Code Quality
**Estimated Effort:** 5 minutes

**Problem:**
- `_arrow_to_iceberg_type` duplicated at lines 521-539 and 690-706
- 18 lines of duplicate code

**Fix:**
```python
# Delete lines 690-706 (second occurrence)
# Keep lines 521-539 as canonical version
```

**Why Prioritize:** Trivial fix, quick win, reduces LOC

**Dependencies:** None
**Test Required:** No (verify existing tests pass)
**Agent to Resume:** ab24254 (hit quota limit)

---

### P1 Summary

| Issue | Type | Effort | Impact | Resume Agent |
|-------|------|--------|--------|--------------|
| 019 | Perf Bug | 30m | High | a5f8a39 |
| 020 | Security | 30m | Medium | a4be230 |
| 021 | Error Handling | 1h | Medium | New |
| 022 | Testing | 2h | High | New |
| 016 | Code Quality | 5m | Low | ab24254 |

**Total P1 Effort:** ~4 hours 5 minutes
**Expected Completion:** 1 session

---

## Priority 2 (P2) - Moderate Impact Issues

### Code Quality & Cleanup (Quick Wins)

#### 6. Issue 002: SQL Injection Identifiers (DUPLICATE)
**Status:** Pending
**Priority:** P1 (in TODO), but DUPLICATE of 017
**Type:** Security
**Estimated Effort:** 5 minutes

**Action:** Mark as resolved (duplicate of Issue 017 which is complete)

**No code changes needed** - Issue 017 already fixed this

---

### Performance Optimizations

#### 7. Issue 006: No Catalog Caching
**Status:** Pending
**Priority:** P2
**Type:** Performance
**Estimated Effort:** 2 hours

**Problem:**
- Catalog loaded on every operation (100-200ms overhead)
- No connection reuse

**Fix:**
```python
class IcebergOfflineStore:
    _catalog_cache: Dict[FrozenConfig, Catalog] = {}

    @classmethod
    def _get_catalog(cls, config):
        cache_key = freeze_config(config)
        if cache_key not in cls._catalog_cache:
            cls._catalog_cache[cache_key] = load_catalog(config)
        return cls._catalog_cache[cache_key]
```

**Impact:** 100-200ms latency reduction per operation

**Agent to Resume:** afd6d35 (hit quota limit)

---

#### 8. Issue 007: Python Loop Deduplication
**Status:** Pending
**Priority:** P2
**Type:** Performance
**Estimated Effort:** 3 hours

**Problem:**
- O(n) Python loop with `.as_py()` per-cell conversion
- 10-30 seconds for 1M rows

**Fix:**
```python
# Replace Python loop with PyArrow vectorized operations
sorted_table = arrow_table.sort_by([
    ("entity_key", "ascending"),
    ("event_ts", "descending"),
    ("created_ts", "descending")
])

# Use group_by for deduplication
deduplicated = sorted_table.group_by("entity_key").aggregate([
    ("event_ts", "max"),
    ("created_ts", "max"),
    # ... other fields
])
```

**Impact:** 10-100x performance improvement

**Agent to Resume:** a1ee644 (hit quota limit)

---

#### 9. Issue 009: Memory Materialization (VERIFY FIX)
**Status:** Pending
**Priority:** P2
**Type:** Performance Optimization
**Estimated Effort:** 1 hour

**Action:** Verify fix from Issue 019 (MOR double-scan) resolves this

**Current Status:**
- Uses `any()` for early exit (good!)
- But has double-scan bug (Issue 019)
- Once 019 is fixed, this should be resolved

**Test Required:** Yes (verify early exit behavior)

**Agent to Resume:** aab06d7 (hit quota limit)

---

### Feature Additions

#### 10. Issue 010: Hardcoded event_timestamp
**Status:** Pending
**Priority:** P2
**Type:** Flexibility
**Estimated Effort:** 1 hour

**Problem:**
- Query hardcodes "event_timestamp" column name
- Fails if entity DataFrame uses different name

**Fix:**
```python
# Detect timestamp column from entity DataFrame
timestamp_col = entity_df.columns[entity_df.columns.str.contains('timestamp')][0]

# Or make configurable via parameter
def get_historical_features(..., timestamp_field: str = "event_timestamp"):
```

**Impact:** Works with non-standard timestamp column names

**Agent to Resume:** a2a53b3 (hit quota limit)

---

#### 11. Issue 011: Incomplete Type Mapping
**Status:** Pending
**Priority:** P2
**Type:** Feature Completeness
**Estimated Effort:** 2 hours

**Problem:**
- List, Map, Struct, Decimal types return UNKNOWN

**Fix:**
```python
def iceberg_to_feast_value_type(iceberg_type):
    # Add mappings for complex types
    if isinstance(iceberg_type, ListType):
        return ValueType.STRING_LIST  # or BYTES_LIST, INT64_LIST
    if isinstance(iceberg_type, MapType):
        return ValueType.STRING  # Serialize as JSON
    if isinstance(iceberg_type, StructType):
        return ValueType.STRING  # Serialize as JSON
    if isinstance(iceberg_type, DecimalType):
        return ValueType.DOUBLE  # or STRING with precision
```

**Impact:** Support for complex Iceberg types

**Agent to Resume:** ab62280 (hit quota limit)

---

#### 12. Issue 012: Small File Problem (VERIFY FIX)
**Status:** Pending
**Priority:** P2
**Type:** Performance
**Estimated Effort:** 30 minutes

**Action:** Verify fix from commit d36083a65 (partition count → 32)

**Current Status:**
- partition_count reduced from 256 to 32 ✅
- Test exists and passing ✅

**Action Required:**
- Add documentation about compaction
- Mark as resolved

**Agent to Resume:** a148f17 (hit quota limit)

---

#### 13. Issue 013: Missing offline_write_batch
**Status:** Pending
**Priority:** P2
**Type:** Feature Addition
**Estimated Effort:** 4 hours

**Problem:**
- Offline store lacks `offline_write_batch()` method
- Cannot push features to Iceberg

**Fix:**
```python
@staticmethod
def offline_write_batch(
    config: RepoConfig,
    feature_view: FeatureView,
    table: pyarrow.Table,
    progress: Optional[Callable[[int], None]] = None,
):
    catalog = load_catalog(config)
    iceberg_table = catalog.load_table(feature_view.batch_source.table)

    # Append to Iceberg table
    iceberg_table.append(table)
```

**Impact:** Feature materialization to Iceberg

**Agent to Resume:** abd0d10 (hit quota limit)

---

### Documentation & Verification

#### 14. Issue 014: Credential Exposure (VERIFY FIX)
**Status:** Pending
**Priority:** P2
**Type:** Security
**Estimated Effort:** 30 minutes

**Action:** Verify fix from Issue 018 covers this

**Current Status:**
- Issue 018 fixed credential exposure in SQL SET ✅
- TestCredentialSecurityFixes verifies no exposure ✅

**Action Required:**
- Verify exception messages don't contain credentials
- Add test for exception sanitization (part of Issue 022)
- Mark as resolved

**Agent to Resume:** a2bd4a8 (hit quota limit)

---

#### 15. Issue 015: Exception Swallowing (VERIFY FIX)
**Status:** Pending
**Priority:** P2
**Type:** Error Handling
**Estimated Effort:** 30 minutes

**Action:** Verify partial fix from commit d36083a65

**Current Status:**
- Namespace creation checks "already exists" ✅
- But still uses bare `except Exception` (Issue 021 to fix)

**Action Required:**
- Merge with Issue 021 work
- Mark as resolved when 021 is complete

**Agent to Resume:** aeb8912 (hit quota limit)

---

#### 16. Issue 005: Non-Deterministic Tie-Breaking (VERIFY FIX)
**Status:** Pending (should be resolved)
**Priority:** P1
**Type:** Correctness
**Estimated Effort:** 15 minutes

**Action:** Verify fix from commit d36083a65 is complete

**Current Status:**
- Online store uses created_ts as tiebreaker ✅
- Tests passing ✅
- Already renamed to 005-resolved-p1-non-deterministic-tie-breaking.md

**Action Required:**
- Update status in file to "resolved"
- No code changes needed

---

### P2 Summary

| Issue | Type | Effort | Impact | Status |
|-------|------|--------|--------|--------|
| 002 | Duplicate | 5m | N/A | Mark resolved |
| 005 | Verify | 15m | N/A | Already fixed |
| 006 | Performance | 2h | Medium | New work |
| 007 | Performance | 3h | High | New work |
| 009 | Verify | 1h | Low | Test after 019 |
| 010 | Feature | 1h | Low | New work |
| 011 | Feature | 2h | Medium | New work |
| 012 | Verify | 30m | N/A | Already fixed |
| 013 | Feature | 4h | Medium | New work |
| 014 | Verify | 30m | N/A | Covered by 018 |
| 015 | Verify | 30m | N/A | Merge with 021 |

**Total P2 Effort:** ~14 hours 30 minutes
**Expected Completion:** 2-3 sessions

---

## Execution Schedule

### Session 1: Quick Wins + P1 Critical (4 hours)
**Goal:** Complete all P1 issues

1. ✅ **5 min** - Issue 016: Delete duplicate function
2. ✅ **30 min** - Issue 019: Fix MOR double-scan bug
3. ✅ **30 min** - Issue 020: Add TTL value validation
4. ✅ **1 hour** - Issue 021: Fix overly broad exceptions
5. ✅ **2 hours** - Issue 022: Add missing test coverage

**Commit:** "fix(iceberg): resolve P1 performance and validation issues"

---

### Session 2: Verifications + Easy P2 (3 hours)
**Goal:** Close verification tasks and quick P2 wins

1. ✅ **5 min** - Issue 002: Mark as duplicate of 017
2. ✅ **15 min** - Issue 005: Verify and update status
3. ✅ **30 min** - Issue 012: Verify partition count fix
4. ✅ **30 min** - Issue 014: Verify credential exposure fix
5. ✅ **30 min** - Issue 015: Verify exception swallowing
6. ✅ **1 hour** - Issue 009: Test MOR optimization after 019 fix

**Commit:** "chore(iceberg): verify and close resolved issues"

---

### Session 3: Performance Optimizations (5 hours)
**Goal:** Major performance improvements

1. ✅ **2 hours** - Issue 006: Implement catalog caching
2. ✅ **3 hours** - Issue 007: Vectorize deduplication loop

**Commit:** "perf(iceberg): add catalog caching and vectorized deduplication"

---

### Session 4: Feature Additions (7 hours)
**Goal:** Complete feature work

1. ✅ **1 hour** - Issue 010: Configurable timestamp column
2. ✅ **2 hours** - Issue 011: Complete type mapping
3. ✅ **4 hours** - Issue 013: Implement offline_write_batch

**Commit:** "feat(iceberg): add offline_write_batch and improve type support"

---

## Total Timeline

**P1 Work:** 1 session (4 hours)
**P2 Verifications:** 1 session (3 hours)
**P2 Performance:** 1 session (5 hours)
**P2 Features:** 1 session (7 hours)

**Total:** 4 sessions, ~19 hours of development work

**Suggested Schedule:**
- Week 1: Sessions 1-2 (P1 + verifications)
- Week 2: Sessions 3-4 (P2 optimizations + features)

---

## Risk Assessment

### Low Risk (Safe to Implement)
- Issue 016: Duplicate function deletion
- Issue 019: MOR double-scan fix
- Issue 020: TTL validation
- All verification tasks (002, 005, 009, 012, 014, 015)

### Medium Risk (Requires Testing)
- Issue 021: Exception handling changes
- Issue 022: Test coverage additions
- Issue 006: Catalog caching
- Issue 010: Configurable timestamp

### High Risk (Needs Design Review)
- Issue 007: Vectorized deduplication (major refactor)
- Issue 011: Type mapping (may affect data serialization)
- Issue 013: offline_write_batch (new feature, needs design)

---

## Dependencies Graph

```
Issue 019 (MOR fix) → Issue 009 (verify optimization works)
Issue 017 (resolved) → Issue 020 (extend validation)
Issue 018 (resolved) → Issue 014 (verify coverage)
Issue 015 (partial) → Issue 021 (complete fix)
Issue 021 → Issue 022 (test exception handling)
```

---

## Success Metrics

### Code Quality
- [ ] All P1 issues resolved
- [ ] 90%+ of P2 issues resolved
- [ ] All new code has test coverage
- [ ] No regressions in existing tests

### Performance
- [ ] 100-200ms latency reduction (catalog caching)
- [ ] 10-100x deduplication speedup
- [ ] 2x metadata I/O reduction (MOR fix)

### Security
- [ ] TTL validation prevents SQL errors
- [ ] No credential exposure in any code path
- [ ] Auth failures propagate correctly

---

## Agent Resume IDs

For resuming work when API quota resets:

**P1 Issues:**
- 016: ab24254
- 019: a5f8a39
- 020: a4be230

**P2 Issues:**
- 006: afd6d35
- 007: a1ee644
- 009: aab06d7
- 010: a2a53b3
- 011: ab62280
- 012: a148f17
- 013: abd0d10
- 014: a2bd4a8
- 015: aeb8912

---

## Next Actions

**Immediate (This Session):**
1. Review this plan
2. Approve session schedule
3. Begin Session 1 (P1 quick wins)

**When API Quota Resets:**
1. Resume agents using IDs above
2. Execute Session 1 tasks
3. Create commit for P1 fixes

**For Each Session:**
1. Create feature branch from feat/iceberg-storage
2. Implement fixes
3. Run tests
4. Commit with descriptive message
5. Merge to feat/iceberg-storage
6. Update TODO statuses

---

**Plan Created:** 2026-01-16
**Next Review:** Before Session 1
**Estimated Completion:** 2-3 weeks (4 sessions)
