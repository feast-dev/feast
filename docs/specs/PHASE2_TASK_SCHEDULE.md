# Phase 2 Completion Task Schedule - UV Native Workflow

**Date**: 2026-01-14  
**Objective**: Complete Phase 2 integration testing using UV native commands ONLY  
**Tracked in**: docs/specs/plan.md

---

## ✅ Prerequisites Complete

1. ✅ Code implementation (100% complete)
2. ✅ Functional tests passed
3. ✅ Python version constraint added (`<3.14`)
4. ✅ Documentation updated
5. ✅ Iceberg optional dependency configured

---

## 📋 Task Schedule

### Task 2.1: Environment Setup ✅ COMPLETE

**Objective**: Sync dependencies with UV to create proper Python 3.12 environment

**Executed**:

```bash
cd /home/tommyk/projects/dataops/feast
uv sync --extra iceberg
uv run python --version
uv run pytest --version
```

**Results**:
- ✅ Python 3.12.12 selected (compatible with pyarrow wheels)
- ✅ PyArrow 17.0.0 installed from wheel (38MB download, instant)
- ✅ PyIceberg 0.10.0 installed
- ✅ DuckDB 1.1.3 installed
- ✅ Pytest 8.4.2 available
- ✅ 75 packages total installed

**Completion Time**: ~30 seconds

---

### Task 2.2: Test Collection Verification ✅ COMPLETE

**Objective**: Verify test parametrization works correctly

**Executed**:

```bash
cd /home/tommyk/projects/dataops/feast
uv run pytest sdk/python/tests/integration/offline_store/test_universal_historical_retrieval.py::test_historical_features_main \
  --collect-only -q
```

**Results**:
- ✅ **44 tests collected** for test_historical_features_main
- ✅ Tests parametrized across all AVAILABLE_OFFLINE_STORES including Iceberg
- ⚠️ 3 deprecation warnings (from lark, pyiceberg - expected, not blocking)

**Completion Time**: ~1 second

---

### Task 2.3: Smoke Test - Single Test Execution ⏭️ NEXT

**Objective**: Run ONE single test to verify basic functionality

```bash
cd /home/tommyk/projects/dataops/feast

# Run first parametrized test only
uv run pytest sdk/python/tests/integration/offline_store/test_universal_historical_retrieval.py::test_historical_features_main \
  -v --maxfail=1 -x 2>&1 | tee smoke_test.log

# Check result
tail -n 50 smoke_test.log
```

**Expected Outcomes**:

**Option A: Test Passes** ✅
- Proceed to Task 2.4 (full test suite)

**Option B: Test Fails** ❌
- Analyze failure traceback
- Categorize failure type:
  - Schema mismatch
  - Entity join error
  - TTL handling error
  - Timestamp conversion error
  - Other
- Create targeted fix task
- Re-run smoke test

---

### Task 2.4: Full Integration Test Suite (30-60 min)

**Objective**: Run all universal historical retrieval tests

```bash
cd /home/tommyk/projects/dataops/feast

# Run all tests with detailed output
uv run pytest sdk/python/tests/integration/offline_store/test_universal_historical_retrieval.py \
  -v --maxfail=10 --tb=short 2>&1 | tee iceberg_integration_tests.log

# Generate summary
echo "=== TEST SUMMARY ===" >> iceberg_integration_tests.log
grep -E "(PASSED|FAILED|ERROR|SKIPPED)" iceberg_integration_tests.log | \
  awk '{print $NF}' | sort | uniq -c >> iceberg_integration_tests.log
```

**Expected Results**:
- All tests PASSED or documented failures with fix plan
- Log file saved for analysis

**Failure Analysis**:
```bash
# Extract all failures
grep "FAILED" iceberg_integration_tests.log > failures.txt

# Count failure types
grep -o "AssertionError\|ValueError\|TypeError\|KeyError" iceberg_integration_tests.log | sort | uniq -c
```

---

### Task 2.5: Results Documentation (15 min)

**Objective**: Update TEST_RESULTS.md with complete results

**Steps**:

1. Count test results:
```bash
PASSED=$(grep -c "PASSED" iceberg_integration_tests.log)
FAILED=$(grep -c "FAILED" iceberg_integration_tests.log)
TOTAL=$((PASSED + FAILED))
echo "Passed: $PASSED/$TOTAL"
```

2. Update `docs/specs/TEST_RESULTS.md` with:
   - Total tests run
   - Pass/fail counts
   - List of failures (if any)
   - Failure categorization

3. Update `docs/specs/plan.md`:
   - Mark Phase 2 tasks complete
   - Update status summary

---

### Task 2.6: Phase 2 Completion (If Tests Pass) (30 min)

**Objective**: Finalize Phase 2 and prepare for Phase 3

**Steps**:

1. **Code Review**:
```bash
# Check for TODOs
uv run ruff check sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/

# Check type hints
cd sdk/python && uv run mypy feast/infra/offline_stores/contrib/iceberg_offline_store/
```

2. **Git Commit**:
```bash
cd /home/tommyk/projects/dataops/feast

git add .
git status

# Review changes
git diff --cached --stat

# Commit
git commit -m "feat(offline-store): Complete Iceberg offline store Phase 2

- Implement IcebergOfflineStore with hybrid COW/MOR strategy  
- Add IcebergSource with protobuf serialization
- Fix timestamp precision handling (ns → us conversion)
- Fix field_id validation in schema generation
- Add comprehensive type mapping for Iceberg types
- Integrate with universal test framework
- Pin Python <3.14 for pyarrow wheel compatibility

Phase 2 complete. All integration tests passing.

Co-authored-by: OpenCode <opencode@anomaly.co>
"
```

3. **Update Documentation**:
   - Mark Phase 2 COMPLETE in plan.md
   - Create Phase 3 planning document
   - Update CHANGELOG.md

---

### Task 2.7: Failure Resolution (If Tests Fail)

**Objective**: Systematically fix all test failures

**Process**:

1. **Categorize Failures**:
   - Group by failure reason (schema, entity, TTL, etc.)
   - Prioritize by impact (blocking vs edge case)

2. **Create Fix Tasks**:
   - One subtask per failure category
   - Estimate complexity (simple, medium, complex)

3. **Fix and Re-test**:
   ```bash
   # After each fix
   uv run pytest sdk/python/tests/integration/offline_store/test_universal_historical_retrieval.py::test_<specific_test> \
     -v --tb=short
   ```

4. **Re-run Full Suite**:
   ```bash
   # After all fixes
   uv run pytest sdk/python/tests/integration/offline_store/test_universal_historical_retrieval.py \
     -v --maxfail=10 --tb=short 2>&1 | tee iceberg_tests_rerun.log
   ```

---

## 🎯 Success Criteria

Phase 2 is COMPLETE when:

- ✅ `uv sync --extra iceberg` succeeds (Python <3.14)
- ✅ All imports work via `uv run python -c "..."`
- ✅ Smoke test passes (single test execution)
- ✅ Integration test suite passes (or failures documented with fix plan)
- ✅ Test results documented in TEST_RESULTS.md
- ✅ Changes committed to git
- ✅ plan.md updated with Phase 2 COMPLETE status

---

## 📊 Progress Tracking

| Task | Status | Duration | Notes |
|------|--------|----------|-------|
| 2.1: Environment Setup | ⏭️ NEXT | 5 min | UV sync with Python constraint |
| 2.2: Test Collection | ⏳ Pending | 5 min | Verify 44 tests collected |
| 2.3: Smoke Test | ⏳ Pending | 10 min | Run single test |
| 2.4: Full Test Suite | ⏳ Pending | 30-60 min | All integration tests |
| 2.5: Documentation | ⏳ Pending | 15 min | Update TEST_RESULTS.md |
| 2.6: Phase 2 Completion | ⏳ Pending | 30 min | Git commit, Phase 3 prep |
| 2.7: Failure Resolution | ⏳ Conditional | Variable | If tests fail |

---

## 🚀 Execute Next Task

**Ready to proceed**: Run Task 2.1 Step 1.1

```bash
cd /home/tommyk/projects/dataops/feast
uv sync --extra iceberg
```

---

**All work tracked in**: docs/specs/plan.md  
**Command reference**: UV native only - no pip, pytest, python direct usage  
**Current status**: Ready to execute Task 2.1
