# Real-Time Compute Engine Technical Specification

## Executive Summary

This technical specification outlines the architecture for refactoring `OnDemandFeatureView` (ODFV) to inherit from `FeatureView` and integrate with a new `RealTimeComputeEngine`. This unifies the feature view hierarchy, eliminates code duplication, and enables ODFV to leverage the existing compute engine infrastructure (DAG execution, backend abstraction, dependency resolution) that currently powers `BatchFeatureView` and `StreamFeatureView`.

**Key Objectives:**
1. Make ODFV a first-class FeatureView by inheriting from FeatureView
2. Create RealTimeComputeEngine that reuses existing DAG infrastructure
3. Unify write semantics (push/materialize) across all feature view types
4. Introduce explicit read semantics (transform flag) for predictable behavior
5. Eliminate `write_to_online_store` and `transform_on_write` flags in favor of declarative ODFV configuration

## Current Architecture

### Class Hierarchy

```
BaseFeatureView
├── FeatureView
│   ├── BatchFeatureView (transformations via Spark/Ray compute engines)
│   └── StreamFeatureView (stream processing via compute engines)
└── OnDemandFeatureView (isolated, no compute engine integration)
```

**Problems:**
- ODFV reimplements entity/schema handling from FeatureView
- ODFV cannot use TTL, materialization intervals, or compute engines
- Inconsistent write semantics (`write_to_online_store` vs materialization)
- ODFV transformations are hardcoded to current implementation
- No dependency graph resolution for ODFV sources

### Current ODFV Execution Model

```python
# Current: Transformation logic embedded in utils
def _augment_response_with_on_demand_transforms(
    online_features_response,
    feature_refs,
    requested_on_demand_feature_views,
    full_feature_names,
):
    # Hardcoded pandas transformation
    # No DAG, no backend abstraction, no caching
    for odfv in requested_on_demand_feature_views:
        # Execute transformation inline
        result = odfv.transform(input_df)
```

**Issues:**
- Transformation always happens on read (no caching)
- No backend choice (hardcoded to pandas)
- No dependency resolution
- Can't materialize ODFV results

## Proposed Architecture

### New Class Hierarchy

```
BaseFeatureView
└── FeatureView
    ├── BatchFeatureView (batch transformations via Spark/Ray/etc)
    ├── StreamFeatureView (stream processing via Kafka/etc)
    └── OnDemandFeatureView (real-time transformations via RealTimeComputeEngine)
```

### Unified Compute Engine Pattern

All feature view types use the same compute engine abstraction:

```
FeatureView
    ↓
FeatureResolver.resolve()  ← Builds dependency DAG
    ↓
ExecutionPlan(nodes)       ← Topological sort
    ↓
DAGNode.execute()          ← Backend-specific execution
    ↓
Result (Arrow Table or Python Obj)
```

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     Feature Server                          │
│  ┌───────────┐  ┌──────────────┐  ┌──────────────────────┐ │
│  │   /push   │  │ /materialize │  │ /get-online-features │ │
│  └─────┬─────┘  └──────┬───────┘  └──────────┬───────────┘ │
│        │                │                      │             │
└────────┼────────────────┼──────────────────────┼─────────────┘
         │                │                      │
         ▼                ▼                      ▼
┌─────────────────────────────────────────────────────────────┐
│              RealTimeComputeEngine                          │
│                                                              │
│  compute_from_push()    materialize()    transform()        │
│         │                    │                │             │
│         └────────────────────┴────────────────┘             │
│                          │                                   │
│                          ▼                                   │
│              ┌───────────────────────┐                      │
│              │   FeatureResolver     │  ← Reused!           │
│              │  (builds DAG from     │                      │
│              │   source_views)       │                      │
│              └───────────┬───────────┘                      │
│                          │                                   │
│                          ▼                                   │
│              ┌───────────────────────┐                      │
│              │   ExecutionPlan       │  ← Reused!           │
│              │  (topological sort)   │                      │
│              └───────────┬───────────┘                      │
│                          │                                   │
│                          ▼                                   │
│         ┌────────────────────────────────────────┐          │
│         │     Transformation Nodes (Reused!)     │          │
│         ├────────────────┬───────────────────────┤          │
│         │ LocalTransform │  PythonTransformNode  │          │
│         │     Node       │  + PythonBackend      │          │
│         │ + DataFrame    │  (Python objects)     │          │
│         │    Backend     │                       │          │
│         │ (Pandas/Polars)│                       │          │
│         └────────────────┴───────────────────────┘          │
│                          │                                   │
└──────────────────────────┼─────────────────────────────────┘
                           │
                           ▼
                 ┌──────────────────┐
                 │  Online Store    │
                 │  Offline Store   │
                 └──────────────────┘
```

## Core Design Principles

### 1. Source Handling

**Decision**: Leverage FeatureView's existing `source_views` mechanism. Simplify type signature to only `Union[FeatureView, RequestSource]`.

**Current ODFV:**
```python
sources: List[Union[FeatureView, FeatureViewProjection, RequestSource]]
```

**Proposed:**
```python
sources: List[Union[FeatureView, RequestSource]]  # Simpler!
```

**Implementation:**
```python
class OnDemandFeatureView(FeatureView):
    def __init__(self, sources: List[Union[FeatureView, RequestSource]], ...):
        # Separate sources into feature views and request sources
        feature_view_sources = [s for s in sources if isinstance(s, FeatureView)]
        request_sources = [s for s in sources if isinstance(s, RequestSource)]

        # Pass feature views to FeatureView.__init__
        # FeatureView already supports source_views: List[FeatureView]
        super().__init__(
            source=feature_view_sources,  # FeatureView handles this
            ...
        )

        # Internally, use projections (every FeatureView has .projection)
        self.source_feature_view_projections = {
            fv.name: fv.projection  # BaseFeatureView provides .projection
            for fv in feature_view_sources
        }

        # Keep ODFV-specific request sources
        self.source_request_sources = {
            rs.name: rs for rs in request_sources
        }
```

**Why this works:**
- Every FeatureView has a `.projection` property (from BaseFeatureView)
- FeatureView already supports `source_views` for dependencies
- Users can still use `fv.with_projection()` for advanced feature selection
- FeatureResolver already knows how to handle `source_views`

### 2. TTL Semantics

**Decision**: Default TTL to 0 (infinite) for ODFV.

**Rationale:**
- TTL=0 means results are never stale (always recompute or read from materialized cache)
- ODFV results don't age like batch features (they depend on current source features)
- Non-zero TTL controls how long materialized results are valid

```python
class OnDemandFeatureView(FeatureView):
    def __init__(
        self,
        ttl: timedelta = timedelta(days=0),  # 0 = infinite
        ...
    ):
        super().__init__(ttl=ttl, ...)
```

### 3. Materialization Semantics

**Decision**: Unify write semantics - `online` and `offline` flags control both computation and storage.

#### Write Path Behavior

**During `/push` or `materialize()`:**

1. **If `online=True` OR `offline=True`**: Compute ODFV transformation
2. **Write to stores based on flags:**
   - `online=True` → Write to online store
   - `offline=True` → Write to offline store
3. **If `online=False` AND `offline=False`**: Skip ODFV entirely (no computation)

```python
odfv = OnDemandFeatureView(
    sources=[user_fv, request_source],
    schema=[Field(name="score", dtype=Float32)],
    online=True,   # Compute AND write to online store
    offline=True,  # Compute AND write to offline store
    ttl=timedelta(hours=1),
)

# Write path: Materialize
store.materialize(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31),
    feature_views=[odfv]
)
# → ODFV transformation executed
# → Results written to online store (online=True)
# → Results written to offline store (offline=True)
```

#### Read Path Behavior

**New `transform` parameter controls read-time computation:**

- **`transform=False` (default)**: Cache-only mode
  - Read ODFV results from online/offline store
  - If not materialized → return null, log warning
  - Safe, predictable, no surprise computation

- **`transform=True`**: Force recomputation mode
  - Fetch source features from stores
  - Execute ODFV transformation via RealTimeComputeEngine
  - Return computed results
  - Does NOT write back to stores (read-only)

```python
# Default: Read from cache only
response = store.get_online_features(
    features=["user_score_odfv:score"],
    entity_rows=[{"user_id": 1}],
    # transform=False (default)
)
# → Reads from online store
# → If ODFV not materialized: returns null + warning

# Force recomputation
response = store.get_online_features(
    features=["user_score_odfv:score"],
    entity_rows=[{"user_id": 1}],
    transform=True
)
# → Fetches user_fv features from online store
# → Executes ODFV transformation
# → Returns computed score
# → Does NOT write to online store
```

**Benefits:**
- **Predictable**: Default behavior never triggers computation
- **Explicit**: Users opt-in to transformation with `transform=True`
- **Consistent**: Same pattern for online and offline retrieval
- **Safe**: No hidden compute costs

### 4. Online/Offline Flags

**Decision:**
- `online=True, offline=False` (default) - ODFV results written to online store
- `online=True, offline=True` - ODFV results written to both stores
- `online=False, offline=True` - ODFV results written to offline store only (training)

```python
class OnDemandFeatureView(FeatureView):
    def __init__(
        self,
        online: bool = True,   # Write to online store when materialized
        offline: bool = False,  # Write to offline store when materialized
        ...
    ):
        super().__init__(online=online, offline=offline, ...)
```

## Compute Engine Integration

### Reusing Existing Infrastructure

The key insight: **All the infrastructure already exists**. We just need to wire it together for ODFV.

#### Existing Components (Already Implemented)

1. **FeatureResolver** (`feast/infra/compute_engines/feature_resolver.py`)
   - Builds dependency DAG from `FeatureView.source_views`
   - Detects cycles
   - Performs topological sort

2. **ExecutionPlan** (`feast/infra/compute_engines/dag/plan.py`)
   - Executes DAG nodes in order
   - Caches intermediate results
   - Passes ExecutionContext through pipeline

3. **DAGNode** (`feast/infra/compute_engines/dag/node.py`)
   - Abstract execution node
   - Manages inputs/outputs
   - Executes with context

4. **LocalTransformationNode** (`feast/infra/compute_engines/local/nodes.py`)
   - Executes transformations using DataFrameBackend
   - Converts Arrow ↔ backend-native format
   - Returns ArrowTableValue

5. **DataFrameBackend** (`feast/infra/compute_engines/backends/`)
   - Abstract interface: `PandasBackend`, `PolarsBackend`
   - Provides join, groupby, filter, etc.
   - Backend-agnostic DataFrame operations

### RealTimeComputeEngine Implementation

The RealTimeComputeEngine is primarily a **composition layer** that orchestrates existing components:

```python
# feast/infra/compute_engines/realtime/compute.py

from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.compute_engines.feature_resolver import FeatureResolver
from feast.infra.compute_engines.local.nodes import LocalTransformationNode, LocalReadNode
from feast.infra.compute_engines.dag.plan import ExecutionPlan
from feast.infra.compute_engines.backends.factory import BackendFactory

class RealTimeComputeEngine(ComputeEngine):
    """
    Compute engine for on-demand feature transformations.

    This engine reuses existing compute infrastructure:
    - FeatureResolver for dependency resolution
    - ExecutionPlan for DAG execution
    - LocalTransformationNode for transformations
    - DataFrameBackend for pandas/polars execution

    New responsibilities:
    - Orchestrate push-triggered computation
    - Validate RequestSource dependencies
    - Integrate with feature server endpoints
    """

    def __init__(self, repo_config, offline_store, online_store, **kwargs):
        super().__init__(
            repo_config=repo_config,
            offline_store=offline_store,
            online_store=online_store,
            **kwargs
        )
        # Get configured backend (pandas, polars, etc.)
        self.backend = BackendFactory.get_backend(
            repo_config.realtime_engine_backend or "pandas"
        )

    def compute_from_push(
        self,
        push_source_name: str,
        df: pd.DataFrame,
        write_to: PushMode,
        registry: BaseRegistry,
        project: str,
    ):
        """
        Compute downstream ODFV features triggered by push event.

        Workflow:
        1. Find ODFVs that depend on the pushed source
        2. For each ODFV with online=True or offline=True:
           a. Use FeatureResolver to build dependency DAG
           b. Convert to ExecutionPlan with transformation nodes
           c. Execute plan with pushed data
           d. Write results based on online/offline flags

        Args:
            push_source_name: Name of the push source (e.g., "user_events")
            df: Pushed DataFrame containing raw data
            write_to: Where to write (ONLINE, OFFLINE, ONLINE_AND_OFFLINE)
            registry: Feature registry
            project: Feast project name
        """
        # 1. Find dependent ODFVs
        dependent_odfvs = self._find_dependent_odfvs(
            push_source_name, registry, project
        )

        for odfv in dependent_odfvs:
            # Skip if there's nowhere to write
            if not (odfv.online or odfv.offline):
                continue

            # 2. Build dependency DAG using FeatureResolver (reuse!)
            resolver = FeatureResolver()
            dag_root = resolver.resolve(odfv)  # Automatically handles source_views

            # 3. Topological sort (reuse!)
            sorted_fv_nodes = resolver.topological_sort(dag_root)

            # 4. Build ExecutionPlan with transformation nodes
            execution_plan = self._build_execution_plan(
                feature_view_nodes=sorted_fv_nodes,
                backend=self.backend,
                registry=registry,
                project=project,
            )

            # 5. Execute plan
            context = self._create_execution_context(
                project=project,
                registry=registry,
                initial_data=df,  # Pushed data available in context
            )

            try:
                result = execution_plan.execute(context)
            except ValueError as e:
                # Missing RequestSource fields, etc.
                logger.warning(
                    f"Cannot compute ODFV '{odfv.name}' from push: {e}"
                )
                continue

            # 6. Write results based on flags
            result_df = result.data.to_pandas()  # ArrowTable → pandas

            if odfv.online and write_to in [PushMode.ONLINE, PushMode.ONLINE_AND_OFFLINE]:
                self.online_store.write(
                    config=self.repo_config,
                    table=odfv,
                    data=result_df,
                )

            if odfv.offline and write_to in [PushMode.OFFLINE, PushMode.ONLINE_AND_OFFLINE]:
                self.offline_store.write(
                    config=self.repo_config,
                    table=odfv,
                    data=result_df,
                )

    def transform(
        self,
        feature_view: OnDemandFeatureView,
        input_data: pd.DataFrame,
        registry: BaseRegistry,
        project: str,
    ) -> Optional[pd.DataFrame]:
        """
        Execute ODFV transformation for serving-time computation.

        Used when transform=True in get_online_features().

        Args:
            feature_view: OnDemandFeatureView to compute
            input_data: DataFrame with source features + request data
            registry: Feature registry
            project: Feast project name

        Returns:
            Transformed DataFrame, or None if inputs are missing
        """
        # Build and execute same as compute_from_push
        resolver = FeatureResolver()
        dag_root = resolver.resolve(feature_view)
        sorted_nodes = resolver.topological_sort(dag_root)

        execution_plan = self._build_execution_plan(
            feature_view_nodes=sorted_nodes,
            backend=self.backend,
            registry=registry,
            project=project,
        )

        context = self._create_execution_context(
            project=project,
            registry=registry,
            initial_data=input_data,
        )

        try:
            result = execution_plan.execute(context)
            return result.data.to_pandas()
        except ValueError as e:
            logger.warning(f"Cannot transform ODFV '{feature_view.name}': {e}")
            return None

    def _build_execution_plan(
        self,
        feature_view_nodes: List[FeatureViewNode],
        backend: DataFrameBackend,
        registry: BaseRegistry,
        project: str,
    ) -> ExecutionPlan:
        """
        Convert FeatureViewNodes to DAGNodes with appropriate backends.

        For each FeatureViewNode:
        - If it's a regular FeatureView: Create LocalReadNode
        - If it's an OnDemandFeatureView: Create LocalTransformationNode

        Args:
            feature_view_nodes: Topologically sorted FeatureViewNodes
            backend: DataFrameBackend (Pandas, Polars, etc.)
            registry: Feature registry
            project: Feast project name

        Returns:
            ExecutionPlan ready to execute
        """
        dag_nodes = []
        node_map = {}  # Map FV names to DAGNodes for wiring inputs

        for fv_node in feature_view_nodes:
            if isinstance(fv_node.view, OnDemandFeatureView):
                # Create transformation node (reuse LocalTransformationNode!)
                transformation_fn = fv_node.view.feature_transformation.transform

                # Map input FeatureViewNodes to DAGNodes
                input_dag_nodes = [
                    node_map[input_fv.view.name]
                    for input_fv in fv_node.inputs
                ]

                node = LocalTransformationNode(
                    name=fv_node.view.name,
                    transformation_fn=transformation_fn,
                    backend=backend,
                    inputs=input_dag_nodes,
                )
            else:
                # Regular FeatureView - create read node
                input_dag_nodes = [
                    node_map[input_fv.view.name]
                    for input_fv in fv_node.inputs
                ] if fv_node.inputs else None

                node = LocalReadNode(
                    name=fv_node.view.name,
                    feature_view=fv_node.view,
                    inputs=input_dag_nodes,
                )

            dag_nodes.append(node)
            node_map[fv_node.view.name] = node

        return ExecutionPlan(dag_nodes)

    def _find_dependent_odfvs(
        self,
        push_source_name: str,
        registry: BaseRegistry,
        project: str,
    ) -> List[OnDemandFeatureView]:
        """
        Find ODFVs that depend on the push source.

        Strategy:
        1. Find FeatureView(s) that use this push source
        2. Find ODFVs that have those FeatureViews in source_views

        Args:
            push_source_name: Name of the push source
            registry: Feature registry
            project: Feast project name

        Returns:
            List of dependent OnDemandFeatureViews
        """
        # Find FeatureViews using this push source
        all_fvs = registry.list_feature_views(project=project)
        source_fvs = [
            fv for fv in all_fvs
            if (fv.stream_source and fv.stream_source.name == push_source_name)
            or (fv.batch_source and fv.batch_source.name == push_source_name)
        ]

        if not source_fvs:
            return []

        # Find ODFVs that depend on those FeatureViews
        all_odfvs = registry.list_on_demand_feature_views(project=project)
        dependent_odfvs = []

        for odfv in all_odfvs:
            # Check if any source_view matches our source FeatureViews
            for source_fv in source_fvs:
                if source_fv in odfv.source_views:
                    dependent_odfvs.append(odfv)
                    break

        return dependent_odfvs

    def _create_execution_context(
        self,
        project: str,
        registry: BaseRegistry,
        initial_data: pd.DataFrame,
    ) -> ExecutionContext:
        """
        Create ExecutionContext with initial data pre-loaded.

        Args:
            project: Feast project name
            registry: Feature registry
            initial_data: Initial DataFrame (from push or online store)

        Returns:
            ExecutionContext ready for execution
        """
        context = ExecutionContext(
            project=project,
            repo_config=self.repo_config,
            offline_store=self.offline_store,
            online_store=self.online_store,
            entity_defs=[],  # Populated as needed
            entity_df=initial_data,
        )
        return context

    def _validate_inputs(
        self,
        feature_view: OnDemandFeatureView,
        input_data: pd.DataFrame,
    ) -> bool:
        """
        Validate that input_data contains all required fields.

        Checks:
        - All FeatureView source features are present
        - All RequestSource fields are present

        Args:
            feature_view: OnDemandFeatureView to validate
            input_data: Input DataFrame

        Returns:
            True if all inputs are available, False otherwise
        """
        missing_fields = []

        # Check FeatureView source requirements
        for fv_name, fv_projection in feature_view.source_feature_view_projections.items():
            for feature in fv_projection.features:
                if feature.name not in input_data.columns:
                    missing_fields.append(f"{fv_name}:{feature.name}")

        # Check RequestSource requirements
        for req_source_name, req_source in feature_view.source_request_sources.items():
            for field in req_source.schema:
                if field.name not in input_data.columns:
                    missing_fields.append(f"{req_source_name}:{field.name}")

        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

        return True
```

### Integration with Feature Server

#### Updated `/push` Endpoint

```python
# feast/feature_server.py

@app.post("/push")
async def push(request: PushFeaturesRequest):
    """
    Push raw data to source feature views.

    Behavior:
    1. Write raw data to the push source
    2. Trigger RealTimeComputeEngine to compute dependent ODFVs
    3. Write ODFV results based on online/offline flags

    Note: Removed transform_on_write parameter.
    Computation is controlled by ODFV.online/offline flags.
    """
    df = pd.DataFrame(request.df)

    # Get RealTimeComputeEngine
    realtime_engine = store._get_realtime_compute_engine()

    # Compute and write downstream ODFVs
    await realtime_engine.compute_from_push(
        push_source_name=request.push_source_name,
        df=df,
        write_to=PushMode[request.to.upper()],
        registry=store.registry,
        project=store.project,
    )

    return {"status": "success"}
```

#### Updated `/get-online-features` Endpoint

```python
class GetOnlineFeaturesRequest(BaseModel):
    entities: Dict[str, List[Any]]
    feature_service: Optional[str] = None
    features: List[str] = []
    full_feature_names: bool = False
    transform: bool = False  # NEW: Force ODFV recomputation

@app.post("/get-online-features")
async def get_online_features(request: GetOnlineFeaturesRequest):
    """
    Get online features with optional transformation.

    transform=False (default): Read from cache only
    transform=True: Force recomputation of ODFV features
    """
    features = await _get_features(request, store)

    read_params = dict(
        features=features,
        entity_rows=request.entities,
        full_feature_names=request.full_feature_names,
        transform=request.transform,  # Pass through to provider
    )

    response = await run_in_threadpool(
        lambda: store.get_online_features(**read_params)
    )

    return MessageToDict(response.proto, ...)
```

#### Updated Online Store Integration

```python
# feast/infra/online_stores/online_store.py

def get_online_features(
    self,
    config: RepoConfig,
    features: Union[List[str], FeatureService],
    entity_rows: ...,
    registry: BaseRegistry,
    project: str,
    full_feature_names: bool = False,
    transform: bool = False,  # NEW parameter
) -> OnlineResponse:
    """
    Get online features with optional ODFV transformation.
    """
    # Prepare entities and group features
    (
        join_key_values,
        grouped_refs,
        entity_name_to_join_key_map,
        requested_on_demand_feature_views,
        feature_refs,
        requested_result_row_names,
        online_features_response,
    ) = utils._prepare_entities_to_read_from_online_store(...)

    # 1. Read regular feature views from online store (existing logic)
    for table, requested_features in grouped_refs:
        read_rows = self.online_read(config, table, entity_keys, requested_features)
        utils._populate_response_from_feature_data(...)

    # 2. Handle ODFV features
    if requested_on_demand_feature_views:
        if transform:
            # Force recomputation using RealTimeComputeEngine
            realtime_engine = config.get_realtime_compute_engine()

            for odfv in requested_on_demand_feature_views:
                # Prepare input data (source features from step 1)
                input_df = utils._prepare_odfv_input_data(
                    odfv, online_features_response
                )

                # Execute transformation
                result = realtime_engine.transform(
                    feature_view=odfv,
                    input_data=input_df,
                    registry=registry,
                    project=project,
                )

                if result is not None:
                    # Add to response (do NOT write to store - read-only)
                    utils._populate_response_with_odfv_result(
                        online_features_response, odfv, result
                    )
                else:
                    # Transformation failed (missing inputs)
                    logger.warning(f"ODFV '{odfv.name}' transformation failed")
        else:
            # Default: Cache-only mode - read from online store
            for odfv in requested_on_demand_feature_views:
                cached_result = self.online_read(
                    config=config,
                    table=odfv,  # ODFV is now a FeatureView!
                    entity_keys=entity_keys,
                    requested_features=requested_features,
                )

                if cached_result:
                    # Cache hit - add to response
                    utils._populate_response_from_feature_data(
                        cached_result, ..., online_features_response
                    )
                else:
                    # Cache miss - return null, log warning
                    logger.warning(
                        f"ODFV '{odfv.name}' not materialized, returning null. "
                        f"Use transform=True to compute or materialize the ODFV first."
                    )
                    # Add null values to response

    utils._drop_unneeded_columns(online_features_response, requested_result_row_names)
    return OnlineResponse(online_features_response)
```

## Breaking Changes & Migration

### Breaking Change 1: Inheritance Hierarchy

**Before:**
```python
class OnDemandFeatureView(BaseFeatureView): ...
```

**After:**
```python
class OnDemandFeatureView(FeatureView): ...
```

**Impact:**
- Code checking `isinstance(odfv, BaseFeatureView)` still works (FeatureView inherits from BaseFeatureView)
- Code checking `not isinstance(odfv, FeatureView)` breaks
- Rare edge case

**Migration:**
```python
# Before (breaks)
if isinstance(obj, BaseFeatureView) and not isinstance(obj, FeatureView):
    # This was checking for ODFV
    handle_odfv(obj)

# After
if isinstance(obj, OnDemandFeatureView):
    handle_odfv(obj)
```

### Breaking Change 2: Remove `transform_on_write`

**Before:**
```python
POST /push
{
  "push_source_name": "events",
  "df": {...},
  "to": "online",
  "transform_on_write": true
}
```

**After:**
```python
# Computation controlled by ODFV definition
odfv = OnDemandFeatureView(
    online=True,  # Will compute and write during push
    ...
)

POST /push
{
  "push_source_name": "events",
  "df": {...},
  "to": "online"
  # No transform_on_write parameter
}
```

**Migration:**
- Users relying on `transform_on_write=True`: Set `online=True` on ODFV definition
- Users using `transform_on_write=False`: Set `online=False` on ODFV (or rely on default `online=True` but don't materialize)

### Breaking Change 3: Default Read Behavior

**Before:**
```python
# Always computes ODFV on-demand
response = store.get_online_features(features=["odfv:feat"], ...)
```

**After:**
```python
# Default: Read from cache, return null if not materialized
response = store.get_online_features(features=["odfv:feat"], ...)

# Explicit recomputation
response = store.get_online_features(features=["odfv:feat"], ..., transform=True)
```

**Impact:**
- Users expecting automatic computation will get null values
- Must either materialize ODFVs or use `transform=True`

**Migration:**
```python
# Option 1: Materialize ODFVs (recommended for production)
store.materialize(feature_views=[my_odfv])
response = store.get_online_features(...)  # Reads cached results

# Option 2: Always transform (for development/testing)
response = store.get_online_features(..., transform=True)
```

### Breaking Change 4: Protobuf Schema

**Before:**
```protobuf
message OnDemandFeatureView {
  string name = 1;
  repeated Field features = 2;
  // No ttl, online, offline fields
}
```

**After:**
```protobuf
message OnDemandFeatureView {
  string name = 1;
  repeated Field features = 2;
  google.protobuf.Duration ttl = 3;  # NEW
  bool online = 4;                    # NEW
  bool offline = 5;                   # NEW
  // ... other FeatureView fields
}
```

**Impact:**
- Registry incompatibility
- Old ODFVs can't be read by new code without migration

**Migration:**
```python
# Automatic migration utility
from feast.migration import migrate_registry

migrate_registry(
    registry_path="data/registry.db",
    # Applies defaults: ttl=0, online=True, offline=False
)
```

## Implementation Summary

### What's New (Minimal!)

1. **RealTimeComputeEngine** (`feast/infra/compute_engines/realtime/compute.py`)
   - ~300 lines of orchestration code
   - Reuses FeatureResolver, ExecutionPlan, LocalTransformationNode, DataFrameBackend

2. **ODFV Inheritance Change** (`feast/on_demand_feature_view.py`)
   - Change `class OnDemandFeatureView(BaseFeatureView)` → `(FeatureView)`
   - Update `__init__` to call `super().__init__` with proper parameters
   - Add `online`, `offline`, `ttl` parameters

3. **Feature Server Updates** (`feast/feature_server.py`)
   - Remove `transform_on_write` from `PushFeaturesRequest`
   - Add `transform` to `GetOnlineFeaturesRequest`
   - Integrate RealTimeComputeEngine

4. **Online Store Updates** (`feast/infra/online_stores/online_store.py`)
   - Add `transform` parameter to `get_online_features()`
   - Implement cache-only vs transform logic

### What's Reused (Everything Else!)

✅ **FeatureResolver** - Dependency graph building
✅ **ExecutionPlan** - DAG execution orchestration
✅ **LocalTransformationNode** - Transformation execution
✅ **DataFrameBackend** - Pandas/Polars abstraction
✅ **DAGNode, DAGValue** - Execution primitives
✅ **Topological sort** - Cycle detection, ordering

**Total new code: ~500 lines**
**Reused code: ~2000 lines**
**Code ratio: 1:4 new:reused**

## Configuration

### Feature Store YAML

```yaml
project: my_project
provider: local

# Configure real-time compute engine (optional)
realtime_engine:
  type: local  # or 'remote' for future distributed execution
  backend: pandas  # or 'polars' for faster execution

online_store:
  type: redis
  connection_string: localhost:6379

offline_store:
  type: file
```

### ODFV Definition

```python
from feast import OnDemandFeatureView, Field
from feast.types import Float32, Int64
from datetime import timedelta

@on_demand_feature_view(
    sources=[user_fv, order_fv, request_source],
    schema=[
        Field(name="user_order_score", dtype=Float32),
        Field(name="user_lifetime_orders", dtype=Int64),
    ],
    online=True,          # Write to online store when materialized
    offline=True,         # Write to offline store when materialized
    ttl=timedelta(hours=1),  # Cached results expire after 1 hour
    mode="pandas",        # Backend: pandas, polars, python
)
def user_order_features(inputs: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "user_order_score": inputs["user_age"] * inputs["order_total"],
        "user_lifetime_orders": inputs["order_count"] + 1,
    })
```

## End-to-End Workflows

### Workflow 1: Batch Materialization

```python
# 1. Define ODFV
odfv = OnDemandFeatureView(
    sources=[batch_fv, request_source],
    schema=[Field(name="score", dtype=Float32)],
    online=True,
    offline=True,
)

# 2. Register
store.apply([batch_fv, odfv])

# 3. Materialize (computes transformations, writes to stores)
store.materialize(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31),
    feature_views=[odfv]
)

# 4. Serve (reads from cache)
response = store.get_online_features(
    features=["odfv:score"],
    entity_rows=[{"user_id": 1}],
    # transform=False (default)
)
```

### Workflow 2: Real-Time Push with Auto-Computation

```python
# 1. Define ODFV with online=True
odfv = OnDemandFeatureView(
    sources=[stream_fv, request_source],
    schema=[Field(name="score", dtype=Float32)],
    online=True,  # Auto-compute on push
)

# 2. Push data (triggers ODFV computation)
store.push(
    push_source_name="user_events",
    df=pd.DataFrame({"user_id": [1], "event_count": [10]}),
    to=PushMode.ONLINE,
)
# → stream_fv written to online store
# → ODFV transformation executed
# → ODFV results written to online store

# 3. Serve (reads cached ODFV results)
response = store.get_online_features(
    features=["odfv:score"],
    entity_rows=[{"user_id": 1}],
)
```

### Workflow 3: On-Demand Computation (Development)

```python
# 1. Define ODFV (not materialized)
odfv = OnDemandFeatureView(
    sources=[batch_fv, request_source],
    schema=[Field(name="score", dtype=Float32)],
    online=False,  # Don't write to store
)

# 2. Serve with transform=True (computes on-demand)
response = store.get_online_features(
    features=["odfv:score"],
    entity_rows=[{"user_id": 1}],
    transform=True,  # Force computation
)
# → Fetches batch_fv features
# → Executes ODFV transformation
# → Returns computed score
# → Does NOT write to online store
```

---

**Document Version:** 1.0
**Last Updated:** 2025-12-31
**Status:** Draft for Review