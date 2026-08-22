# Iceberg Local Materialization Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the local compute engine materialize a derived FeatureView into an `IcebergSource` sink with deterministic PyIceberg upsert semantics.

**Architecture:** Keep sink selection in `LocalOutputNode`, but put all catalog, table-creation, schema-validation, duplicate-key, and upsert behavior behind a focused `IcebergSource.write_materialized_table()` method. `LocalFeatureBuilder` passes resolved `ColumnInfo` to the output node so join keys and the event timestamp use the same mapped names as the computed Arrow table. Existing online/offline writes remain independent and unchanged.

**Tech Stack:** Python, PyArrow, PyIceberg 0.10+, Feast local compute DAG, pytest, ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-08-16-iceberg-local-materialization-sink-design.md`

## Global Constraints

- Limit this PR to the local compute engine. Spark `MERGE INTO` is a follow-up.
- Reuse the existing `sink_source` field with an `IcebergSource`; do not add a new public sink protocol or protobuf fields.
- Use mapped entity join keys plus the mapped event timestamp as the upsert key.
- Reject duplicate keys in each incoming batch before any catalog mutation.
- Create a missing table, but require its namespace to exist.
- Require exact column names and compatible Arrow/Iceberg types for an existing table; do not evolve its schema.
- Preserve the lightweight REST client for existing read/validation paths. Use PyIceberg for writes for every catalog type, including REST.
- Do not retry commit conflicts and do not imply transactional consistency with online/offline writes.

## File Map

- Modify `pyproject.toml`: raise the optional Iceberg dependency floor to PyIceberg 0.10.
- Modify `sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py`: construct a PyIceberg catalog for writes and own validation/create/upsert behavior.
- Modify `sdk/python/feast/infra/compute_engines/local/nodes.py`: detect an Iceberg sink and invoke it with resolved keys.
- Modify `sdk/python/feast/infra/compute_engines/local/feature_builder.py`: pass `ColumnInfo` into `LocalOutputNode`.
- Modify `sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py`: unit-test PyIceberg configuration and the writer contract.
- Modify `sdk/python/tests/unit/infra/compute_engines/local/test_nodes.py`: unit-test local routing, mapped keys, empty input, and independent writes.
- Create `sdk/python/tests/component/iceberg/__init__.py`: component-test package marker.
- Create `sdk/python/tests/component/iceberg/test_local_materialization_sink.py`: exercise a real SQL catalog and filesystem warehouse.
- Modify `docs/reference/data-sources/iceberg.md`: document local sink configuration, guarantees, and limitations.

---

### Task 1: Establish the PyIceberg 0.10 writer contract

**Files:**
- Modify: `pyproject.toml`
- Test: `sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py`
- Modify: `sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py`

- [ ] **Step 1: Raise the optional dependency floor**

Change the extra to:

```toml
iceberg = ["pyiceberg>=0.10.0"]
```

Do not regenerate the default, minimal, or CI requirements files: `lock-python-dependencies-all` compiles extras `ci`, `minimal`, and `minimal-sdist-build`, none of which select `iceberg`.

- [ ] **Step 2: Write failing tests for a dedicated PyIceberg catalog loader**

Add parameterized tests proving `get_pyiceberg_catalog()` calls `pyiceberg.catalog.load_catalog()` for both REST and non-REST sources. Assert that it passes `type`, `uri`, `warehouse`, token from `token_env_var`, and `catalog_properties` without changing `get_catalog_client()` behavior.

```python
@patch("pyiceberg.catalog.load_catalog")
def test_get_pyiceberg_catalog_for_rest(mock_load_catalog):
    source = IcebergSource(
        catalog_type="rest",
        endpoint="http://catalog.test",
        warehouse="warehouse",
        namespace="features",
        table="driver_stats",
        token_env_var="ICEBERG_TOKEN",
        catalog_properties={"prefix": "tenant"},
    )
    with patch.dict("os.environ", {"ICEBERG_TOKEN": "secret"}):
        source.get_pyiceberg_catalog()
    mock_load_catalog.assert_called_once_with(
        "feast_iceberg",
        type="rest",
        prefix="tenant",
        uri="http://catalog.test",
        warehouse="warehouse",
        token="secret",
    )
```

- [ ] **Step 3: Run the focused tests and confirm the expected failure**

Run:

```bash
uv run pytest -q sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py -k pyiceberg_catalog
```

Expected: fail because `IcebergSource.get_pyiceberg_catalog` does not exist.

- [ ] **Step 4: Implement `get_pyiceberg_catalog()` and centralize configuration**

Add a private `_pyiceberg_catalog_config() -> Dict[str, str]` helper and:

```python
def get_pyiceberg_catalog(self) -> Any:
    """Load a PyIceberg catalog for mutation-capable operations."""
    try:
        from pyiceberg.catalog import load_catalog
    except ImportError as exc:
        raise ImportError(
            "Iceberg materialization requires PyIceberg; install feast[iceberg]."
        ) from exc
    return load_catalog(self.catalog_name, **self._pyiceberg_catalog_config())
```

Have the non-REST branch of `get_catalog_client()` reuse this method. Leave its REST branch on `IcebergRestClient`.

- [ ] **Step 5: Run focused tests and static checks**

Run:

```bash
uv run pytest -q sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py
uv run ruff check sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py
uv run ruff format --check sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py
```

Expected: all pass.

- [ ] **Step 6: Commit the dependency and catalog boundary**

```bash
git add pyproject.toml sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py
git commit -s -m "feat: Add PyIceberg write catalog support"
```

---

### Task 2: Implement strict create-or-upsert semantics on `IcebergSource`

**Files:**
- Modify: `sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py`
- Test: `sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py`

- [ ] **Step 1: Write failing unit tests for pre-mutation validation**

Cover these cases with mocked catalogs/tables:

- a missing join-key column raises `ValueError` naming the missing key;
- a null value in any key column raises `ValueError`;
- duplicate composite keys raise `ValueError` before `load_table`, `create_table`, or `upsert`;
- an existing table with missing, unexpected, or incompatible columns raises a single actionable `ValueError` describing every mismatch;
- an exact existing schema calls `table.upsert(incoming, join_cols=join_cols)`;
- a missing table calls `create_table(identifier, schema=incoming.schema)` and then upserts;
- a missing namespace propagates as an error and never calls `create_namespace`;
- entityless input succeeds when the timestamp is the sole key.

Use the public contract:

```python
source.write_materialized_table(
    incoming,
    join_cols=["driver_id", "event_timestamp"],
)
```

- [ ] **Step 2: Run the writer tests and confirm the expected failure**

Run:

```bash
uv run pytest -q sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py -k materialized_table
```

Expected: fail because `write_materialized_table()` does not exist.

- [ ] **Step 3: Implement key validation without pandas conversion**

Add private helpers that operate on `pyarrow.Table`:

```python
def _validate_upsert_keys(table: pa.Table, join_cols: list[str]) -> None:
    missing = sorted(set(join_cols) - set(table.column_names))
    if missing:
        raise ValueError(f"Iceberg upsert key columns are missing: {missing}")
    # Reject null keys and duplicate StructArray rows before catalog access.
```

Use Arrow compute or a struct of the key arrays to find duplicates. Include the key names and duplicate count in the error; do not log row values because entity keys may be sensitive.

- [ ] **Step 4: Implement strict existing-table schema validation**

Convert the target PyIceberg schema to Arrow via PyIceberg's Arrow schema conversion utilities. Compare sets of column names, then compare each shared field with nullability and type normalization limited to representations PyIceberg accepts losslessly. The error must separately list `missing`, `unexpected`, and `incompatible` entries.

Do not add, rename, widen, or reorder target columns. Reordering the incoming table to the target schema order before `upsert` is allowed after validation.

- [ ] **Step 5: Implement create-or-load and upsert**

Implement:

```python
def write_materialized_table(
    self,
    table: pa.Table,
    join_cols: list[str],
) -> None:
```

Behavior:

1. Validate non-empty `join_cols`, key presence, nulls, and duplicates.
2. Load `f"{namespace}.{iceberg_table}"` using `get_pyiceberg_catalog()`.
3. On PyIceberg `NoSuchTableError`, call `catalog.create_table(identifier, schema=table.schema)`; do not catch `NoSuchNamespaceError` and do not create a namespace.
4. For an existing table, validate the incoming schema strictly and reorder columns to target order.
5. Call `iceberg_table.upsert(table, join_cols=join_cols)`, the PyIceberg 0.10 API. Do not overload catalog properties as snapshot properties; snapshot metadata is optional in the design and is omitted until PyIceberg exposes a stable per-upsert metadata API.
6. Let commit-conflict and catalog errors propagate with the table identifier in a wrapping message; do not retry.

- [ ] **Step 6: Run tests and type/lint checks**

Run:

```bash
uv run pytest -q sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py -k 'materialized_table or pyiceberg_catalog'
uv run ruff check sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py
uv run bash -c "cd sdk/python && mypy feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py"
```

Expected: all pass.

- [ ] **Step 7: Commit the Iceberg writer**

```bash
git add sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py
git commit -s -m "feat: Add Iceberg materialization upserts"
```

---

### Task 3: Route local derived-view output to Iceberg

**Files:**
- Modify: `sdk/python/feast/infra/compute_engines/local/nodes.py`
- Modify: `sdk/python/feast/infra/compute_engines/local/feature_builder.py`
- Test: `sdk/python/tests/unit/infra/compute_engines/local/test_nodes.py`

- [ ] **Step 1: Write failing output-node tests**

Create real `ColumnInfo` values and mocked feature views. Cover:

- `LocalOutputNode` calls an Iceberg sink once with mapped `join_keys_columns` plus `timestamp_column`;
- entityless views pass only `timestamp_column`;
- absent timestamp columns fail before invoking the sink;
- non-Iceberg sinks are ignored by this PR and existing online/offline behavior is unchanged;
- zero-row input performs no online, offline, or Iceberg write;
- when online and offline are also enabled, all three independent writes occur;
- Iceberg failure propagates rather than being swallowed.

Avoid unconstrained `MagicMock` attribute behavior by explicitly setting `feature_view.source_views` and `feature_view.sink_source`.

- [ ] **Step 2: Run the focused node tests and confirm failure**

Run:

```bash
uv run pytest -q sdk/python/tests/unit/infra/compute_engines/local/test_nodes.py -k iceberg
```

Expected: fail because `LocalOutputNode` does not accept `ColumnInfo` or write an Iceberg sink.

- [ ] **Step 3: Pass resolved `ColumnInfo` from the builder**

Change the constructor to require `column_info: ColumnInfo` and build the node with:

```python
column_info = self.get_column_info(view)
node = LocalOutputNode(
    "output",
    self.dag_root.view,
    column_info,
    inputs=[input_node],
)
```

Update every direct unit-test construction of `LocalOutputNode` to supply a realistic `ColumnInfo`; do not make it optional merely to preserve old tests.

- [ ] **Step 4: Add narrowly scoped Iceberg sink routing**

After the empty-table early return and independently of online/offline flags:

```python
sink_source = getattr(self.feature_view, "sink_source", None)
if self.feature_view.source_views and isinstance(sink_source, IcebergSource):
    join_cols = [
        *self.column_info.join_keys_columns,
        self.column_info.timestamp_column,
    ]
    sink_source.write_materialized_table(
        input_table,
        join_cols=join_cols,
    )
```

Deduplicate `join_cols` while preserving order. Validate that `timestamp_column` is non-empty and present. Keep existing online and offline writes intact; the chosen order must be documented in code and tests as sequential but non-transactional.

- [ ] **Step 5: Run local node and builder regression tests**

Run:

```bash
uv run pytest -q sdk/python/tests/unit/infra/compute_engines/local/test_nodes.py
uv run pytest -q sdk/python/tests/unit/infra/compute_engines/test_local_compute_engine.py sdk/python/tests/unit/infra/compute_engines/test_local_job.py
uv run ruff check sdk/python/feast/infra/compute_engines/local/nodes.py sdk/python/feast/infra/compute_engines/local/feature_builder.py sdk/python/tests/unit/infra/compute_engines/local/test_nodes.py
uv run bash -c "cd sdk/python && mypy feast/infra/compute_engines/local/nodes.py feast/infra/compute_engines/local/feature_builder.py"
```

Expected: all pass.

- [ ] **Step 6: Commit local routing**

```bash
git add sdk/python/feast/infra/compute_engines/local/nodes.py sdk/python/feast/infra/compute_engines/local/feature_builder.py sdk/python/tests/unit/infra/compute_engines/local/test_nodes.py
git commit -s -m "feat: Write local materializations to Iceberg"
```

---

### Task 4: Prove idempotent upserts with a real local catalog

**Files:**
- Create: `sdk/python/tests/component/iceberg/__init__.py`
- Create: `sdk/python/tests/component/iceberg/test_local_materialization_sink.py`

- [ ] **Step 1: Write the component test**

Use `tmp_path` for both a SQLite catalog database and a filesystem warehouse. Load a PyIceberg SQL catalog, create the namespace explicitly, and configure `IcebergSource` with `catalog_type="sql"`, a SQLite `uri` in `catalog_properties`, the filesystem warehouse URI, namespace `features`, and table `driver_stats`.

The test must:

1. write two distinct composite keys;
2. write the identical batch again and assert the table still has two rows;
3. write one existing key with a changed feature value and assert that row is updated, not appended;
4. assert the table schema and key columns remain unchanged;
5. query through `catalog.load_table("features.driver_stats").scan().to_arrow()` rather than inspecting implementation mocks.

- [ ] **Step 2: Run the component test and address only environment-real issues**

Run:

```bash
uv run --extra iceberg pytest -q sdk/python/tests/component/iceberg/test_local_materialization_sink.py
```

Expected: pass with no external services. If SQL catalog support requires a declared PyIceberg extra in 0.10, add that extra to Feast's `iceberg` dependency and document why in `pyproject.toml`; do not replace the test with mocks.

- [ ] **Step 3: Run Iceberg regression tests**

Run:

```bash
uv run --extra iceberg pytest -q sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests sdk/python/tests/component/iceberg
```

Expected: all pass.

- [ ] **Step 4: Commit the component coverage**

```bash
git add sdk/python/tests/component/iceberg
git commit -s -m "test: Cover local Iceberg materialization upserts"
```

---

### Task 5: Document the user workflow and limitations

**Files:**
- Modify: `docs/reference/data-sources/iceberg.md`

- [ ] **Step 1: Add a local materialization-sink example**

Show a derived FeatureView that uses an existing source view and:

```python
sink_source=IcebergSource(
    catalog_type="sql",
    catalog_properties={"uri": "sqlite:////tmp/iceberg_catalog.db"},
    warehouse="file:///tmp/iceberg_warehouse",
    namespace="features",
    table="driver_stats_transformed",
    timestamp_field="event_timestamp",
)
```

Show the installation command `pip install "feast[iceberg]"` and a local-engine materialization command.

- [ ] **Step 2: Document exact semantics**

State that:

- only the local compute engine supports this sink in this release;
- PyIceberg performs the write;
- the namespace must already exist, while the table may be created;
- keys are mapped entity join keys plus mapped event timestamp;
- duplicate or null incoming keys are rejected;
- existing schemas must match and are never evolved automatically;
- repeated identical materializations are idempotent;
- online, offline, and Iceberg writes are independent and not transactional;
- Spark support will use distributed Iceberg `MERGE INTO` separately.

- [ ] **Step 3: Check documentation formatting and links**

Run:

```bash
uv run pre-commit run --files docs/reference/data-sources/iceberg.md
```

Expected: all configured documentation hooks pass.

- [ ] **Step 4: Commit documentation**

```bash
git add docs/reference/data-sources/iceberg.md
git commit -s -m "docs: Document local Iceberg materialization sinks"
```

---

### Task 6: Final verification and PR readiness

**Files:**
- Verify all files listed above.

- [ ] **Step 1: Run the complete focused test matrix**

Run:

```bash
uv run --extra iceberg pytest -q sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests sdk/python/tests/unit/infra/compute_engines/local sdk/python/tests/unit/infra/compute_engines/test_local_compute_engine.py sdk/python/tests/unit/infra/compute_engines/test_local_job.py sdk/python/tests/component/iceberg
```

Expected: all pass.

- [ ] **Step 2: Run formatting, lint, and targeted typing**

Run:

```bash
uv run ruff format --check sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py sdk/python/feast/infra/compute_engines/local/nodes.py sdk/python/feast/infra/compute_engines/local/feature_builder.py sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py sdk/python/tests/unit/infra/compute_engines/local/test_nodes.py sdk/python/tests/component/iceberg/test_local_materialization_sink.py
uv run ruff check sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py sdk/python/feast/infra/compute_engines/local/nodes.py sdk/python/feast/infra/compute_engines/local/feature_builder.py sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py sdk/python/tests/unit/infra/compute_engines/local/test_nodes.py sdk/python/tests/component/iceberg/test_local_materialization_sink.py
uv run bash -c "cd sdk/python && mypy feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py feast/infra/compute_engines/local/nodes.py feast/infra/compute_engines/local/feature_builder.py"
```

Expected: all pass.

- [ ] **Step 3: Run repository hooks on the changed files**

Run:

```bash
uv run pre-commit run --files pyproject.toml docs/reference/data-sources/iceberg.md sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/iceberg_source.py sdk/python/feast/infra/compute_engines/local/nodes.py sdk/python/feast/infra/compute_engines/local/feature_builder.py sdk/python/feast/infra/data_sources/contrib/iceberg_catalog/tests/test_iceberg_source.py sdk/python/tests/unit/infra/compute_engines/local/test_nodes.py sdk/python/tests/component/iceberg/__init__.py sdk/python/tests/component/iceberg/test_local_materialization_sink.py
```

Expected: all hooks pass. If a hook rewrites a file, inspect the diff and rerun until clean.

- [ ] **Step 4: Audit scope and behavior**

Run:

```bash
git diff --check
git status --short
git diff --stat HEAD~5
git log --oneline --decorate -6
```

Confirm there are no Spark changes, protobuf changes, generic sink abstractions, generated dependency lock churn, namespace creation, schema evolution, retry loops, or unrelated edits.

- [ ] **Step 5: Request code review before publishing**

Use `superpowers:requesting-code-review` to compare the implementation with the design spec and this plan. Resolve correctness findings, rerun the affected checks, and only then prepare the branch for publication.
