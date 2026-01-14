# Immediate Task Schedule - Phase 2 Completion

**Date**: 2026-01-14  
**Status**: Phase 2 at 95% - Ready for integration testing  
**Tracked in**: docs/specs/plan.md

---

## Task 2.1: Integration Test Execution ⏭️ NEXT

**Objective**: Run universal offline store tests and verify Iceberg implementation

**Prerequisites**: ✅ All met
- ✅ Code implementation complete
- ✅ Import verification passed
- ✅ Functional tests passed
- ✅ Dependencies synced (`uv sync --extra iceberg`)

**Steps**:

### Step 1: Sync Dependencies
```bash
cd /home/tommyk/projects/dataops/feast
uv sync --extra iceberg
```

**Expected**: Dependencies installed/verified
**Duration**: 30-60 seconds

---

### Step 2: Verify Import (Smoke Test)
```bash
uv run python -c "
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import IcebergOfflineStore
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import IcebergSource
from tests.integration.feature_repos.universal.data_sources.iceberg import IcebergDataSourceCreator
print('✅ Imports successful')
"
```

**Expected**: ✅ Imports successful
**Duration**: 2-3 seconds

---

### Step 3: Run Integration Tests
```bash
uv run pytest sdk/python/tests/integration/offline_store/test_universal_historical_retrieval.py \
  -v --maxfail=5 --tb=short 2>&1 | tee iceberg_integration_tests.log
```

**Expected Outcomes**:

**Option A: Tests Pass** ✅
- Document results in TEST_RESULTS.md
- Mark Phase 2 COMPLETE in plan.md
- Proceed to Task 2.2 (Documentation update)
- Begin Phase 3 planning

**Option B: Tests Fail** ❌
- Analyze failure patterns from log
- Categorize failures (entity handling, TTL, schema, joins, etc.)
- Create Task 2.2 subtasks for each category
- Fix and re-run

**Duration**: 5-15 minutes (depends on number of parametrized tests)

---

### Step 4: Document Results
```bash
# Update TEST_RESULTS.md with pass/fail status
# Update plan.md Phase 2 status
# If failures exist, create detailed failure analysis
```

---

## Task 2.2: Post-Test Actions

### If Tests Pass (Expected)

**Task 2.2a: Update Documentation**
- ✅ Mark Phase 2 COMPLETE in plan.md
- ✅ Update TEST_RESULTS.md with success metrics
- ✅ Create Phase 2 completion summary

**Task 2.2b: Code Review**
- Review all modified files for code quality
- Check for any TODOs or FIXMEs
- Verify type hints are complete

**Task 2.2c: Commit Changes**
```bash
git add .
git commit -m "feat(offline-store): Complete Iceberg offline store implementation

- Implement IcebergOfflineStore with hybrid COW/MOR strategy
- Add IcebergSource with protobuf serialization
- Fix timestamp precision handling (ns → us conversion)
- Fix field_id validation in schema generation
- Add comprehensive type mapping for Iceberg types
- Integrate with universal test framework

Phase 2 complete. All integration tests passing.
"
```

---

### If Tests Fail (Contingency)

**Task 2.2-FAIL: Failure Analysis**

1. **Parse Test Log**:
   - Extract all FAILED test names
   - Group by failure reason
   - Identify common patterns

2. **Create Subtasks**:
   - One subtask per failure category
   - Prioritize by impact (blocking vs. edge case)
   - Estimate fix complexity

3. **Example Failure Categories**:
   - Entity join failures
   - TTL handling errors
   - Schema mismatch issues
   - Timestamp precision problems (should be fixed)
   - Feature name mapping errors

4. **Fix Process**:
   - Create targeted unit test for failure
   - Implement fix
   - Verify with unit test
   - Re-run integration tests
   - Repeat until all pass

---

## Task 2.3: Phase 3 Planning (After Phase 2 Complete)

**Objective**: Design Iceberg online store implementation

**Deliverables**:
1. Update `docs/specs/iceberg_online_store.md` with detailed design
2. Create implementation task breakdown
3. Research partition strategies for low-latency reads
4. Design online store configuration options

**Timeline**: 1-2 days planning before implementation

---

## Success Criteria for Phase 2

- ✅ All code compiles without errors
- ✅ All imports successful
- ✅ Functional tests pass
- ✅ Integration tests pass (or documented failures with fix plan)
- ✅ Documentation updated
- ✅ Changes committed to git

**Current Progress**: 95% (waiting on integration test execution)

---

## UV Command Reference (REQUIRED)

**ALWAYS use uv native commands**:

```bash
# Dependency management
uv sync --extra iceberg              # Sync all dependencies
uv add <package>                     # Add new dependency
uv remove <package>                  # Remove dependency

# Running code
uv run pytest <args>                 # Run tests
uv run python <script>               # Run Python script
uv run <command>                     # Run any command in venv

# Never use
❌ pytest <args>                     # Missing uv run
❌ python <script>                   # Missing uv run
❌ pip install <package>             # Use uv add
❌ uv pip install <package>          # Use uv add
❌ source venv/bin/activate          # uv handles activation
```

---

**All work tracked in**: docs/specs/plan.md  
**Next action**: Run Task 2.1 Step 1 (uv sync --extra iceberg)
