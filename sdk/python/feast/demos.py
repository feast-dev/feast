# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Demo notebook generation for Feast projects.

Usage::

    from feast import copy_demo_notebooks
    copy_demo_notebooks()

This will search for ``feature_store.yaml`` in the current directory and every
file inside the ``feast-config/`` directory, then write tailored Jupyter
notebooks into a ``./feast-demo-notebooks/<project>/`` directory for each
project found.
"""

import json
import logging
import os
import pathlib
from typing import Any, Optional

import yaml

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Discovery helpers
# ---------------------------------------------------------------------------


def _find_feature_store_yamls(repo_path: pathlib.Path) -> list[pathlib.Path]:
    """Return all feature-store config paths found under *repo_path*.

    Searches:
    1. ``repo_path/feature_store.yaml``
    2. Every file directly inside ``repo_path/feast-config/``
       — each file is treated as a separate project config.
    """
    found: list[pathlib.Path] = []

    direct = repo_path / "feature_store.yaml"
    if direct.exists():
        found.append(direct)

    feast_config_dir = repo_path / "feast-config"
    if feast_config_dir.is_dir():
        for entry in sorted(feast_config_dir.iterdir()):
            if entry.is_file():
                found.append(entry)

    return found


def _parse_yaml(yaml_path: pathlib.Path) -> dict[str, Any]:
    with open(yaml_path) as fh:
        return yaml.safe_load(os.path.expandvars(fh.read())) or {}


def _extract_store_info(config: dict[str, Any]) -> dict[str, Any]:
    """Summarise the key fields from a raw ``feature_store.yaml`` dict."""
    info: dict[str, Any] = {
        "project": config.get("project", "my_feast_project"),
        "provider": config.get("provider", "local"),
        "online_store_type": "sqlite",
        "offline_store_type": "file",
        "registry_type": "file",
        "auth_type": "no_auth",
        "vector_enabled": False,
        "embedding_dim": None,
    }

    online = config.get("online_store", {})
    if isinstance(online, dict):
        info["online_store_type"] = online.get("type", "sqlite").lower()
        info["vector_enabled"] = bool(online.get("vector_enabled", False))
        if online.get("embedding_dim"):
            info["embedding_dim"] = online["embedding_dim"]
    elif isinstance(online, str):
        info["online_store_type"] = online.lower()

    offline = config.get("offline_store", {})
    if isinstance(offline, dict):
        info["offline_store_type"] = offline.get("type", "file").lower()
    elif isinstance(offline, str):
        info["offline_store_type"] = offline.lower()

    registry = config.get("registry", {})
    if isinstance(registry, dict):
        # Operator client YAML uses "registry_type" key; standard Feast uses "type"
        info["registry_type"] = (
            registry.get("registry_type") or registry.get("type", "file")
        ).lower()
    # string registry value is a plain file path — keep default "file"

    auth = config.get("auth", {})
    if isinstance(auth, dict):
        info["auth_type"] = auth.get("type", "no_auth").lower()

    return info


# ---------------------------------------------------------------------------
# Notebook cell builders
# ---------------------------------------------------------------------------


def _md(source: str) -> dict[str, Any]:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": source,
    }


def _code(source: str, tags: Optional[list[str]] = None) -> dict[str, Any]:
    meta: dict[str, Any] = {}
    if tags:
        meta["tags"] = tags
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": meta,
        "outputs": [],
        "source": source,
    }


def _notebook(cells: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
        },
        "cells": cells,
    }


# ---------------------------------------------------------------------------
# Per-store setup snippets
# ---------------------------------------------------------------------------


def _is_operator_client(info: dict[str, Any]) -> bool:
    """Return True when the feature_store.yaml was generated by the Feast operator.

    The operator sets provider=local with registry_type=remote, online_store.type=remote,
    and offline_store.type=remote.
    """
    return (
        info["registry_type"] == "remote"
        and info["online_store_type"] == "remote"
        and info["offline_store_type"] == "remote"
    )


# ---------------------------------------------------------------------------
# Notebook generators
# ---------------------------------------------------------------------------


def _apply_md(info: dict[str, Any]) -> dict[str, Any]:
    """Return the markdown cell that introduces the apply / registry-sync section."""
    if info["registry_type"] == "remote":
        return _md(
            "## 4. Registry Sync\n\nRefresh the registry cache to load the latest feature definitions."
        )
    return _md(
        "## 4. Apply Feature Definitions\n\n"
        "Register entities, feature views, and services into the registry. "
        "Skip if already applied."
    )


def _apply_code(info: dict[str, Any]) -> dict[str, Any]:
    """Return the code cell that applies (local) or refreshes (remote) the registry."""
    if info["registry_type"] == "remote":
        return _code(
            "store.refresh_registry()\n"
            "fvs = store.list_feature_views()\n"
            "print(f'Registry synced — {len(fvs)} feature view(s) available.')"
        )
    # Local file registry — auto-apply if empty, then refresh.
    return _code(
        "fvs      = store.list_feature_views()\n"
        "entities = store.list_entities()\n"
        "\n"
        "if fvs or entities:\n"
        "    print(f'Registry ready: {len(entities)} entity/entities, {len(fvs)} feature view(s)')\n"
        "else:\n"
        "    print('Registry is empty — running feast apply ...')\n"
        "    !feast -f {FEAST_FS_YAML} apply\n"
        "    store.refresh_registry()\n"
        "    print('Apply complete.')"
    )


def _path_setup_cell(yaml_abs: str) -> dict[str, Any]:
    """Return a code cell that sets ``FEAST_FS_YAML`` to the absolute path of
    the feature-store config resolved at generation time."""
    return _code(
        "import os\n"
        "\n"
        f"FEAST_FS_YAML = r{repr(yaml_abs)}\n"
        "\n"
        "assert os.path.exists(FEAST_FS_YAML), (\n"
        "    f'Config not found at {FEAST_FS_YAML!r}. '\n"
        "    'Update FEAST_FS_YAML to the correct path.'\n"
        ")\n"
        "print(f'Using feature_store.yaml: {FEAST_FS_YAML}')",
        tags=["parameters"],
    )


def _nb_overview(info: dict[str, Any], yaml_abs: str) -> dict[str, Any]:
    project = info["project"]
    ost = info["online_store_type"]
    offst = info["offline_store_type"]
    auth = info["auth_type"]
    provider = info["provider"]
    vector_enabled = info["vector_enabled"]

    cells: list[dict[str, Any]] = [
        _md(
            f"# Feature Store Overview — `{project}`\n\n"
            "Explore the entities, feature views, feature services, and data sources "
            "registered in this project."
        ),
        _md("## 1. Prerequisites"),
        _code(
            "# Verify feast installation\nimport feast\nprint(f'Feast version: {feast.__version__}')"
        ),
        _md("## 2. Feature Store Path"),
        _path_setup_cell(yaml_abs),
        _md(
            f"## 3. Connect to the Feature Store\n"
            f"The feature store for project **`{project}`** is configured with:\n\n"
            f"| Setting | Value |\n"
            f"|---------|-------|\n"
            f"| Provider | `{provider}` |\n"
            f"| Online store | `{ost}` |\n"
            f"| Offline store | `{offst}` |\n"
            f"| Auth | `{auth}` |\n"
            + (
                f"| Vector search | enabled (embedding dim: {info['embedding_dim']}) |\n"
                if vector_enabled
                else ""
            )
        ),
        _code(
            "from feast import FeatureStore\n"
            "\n"
            "store = FeatureStore(fs_yaml_file=FEAST_FS_YAML)\n"
            "print(f'Connected to project: {store.project}')"
        ),
        _apply_md(info),
        _apply_code(info),
        _md("## 5. List Entities"),
        _code(
            "entities = store.list_entities()\n"
            "print(f'Found {len(entities)} entity/entities\\n')\n"
            "for e in entities:\n"
            "    print(f'  • {e.name}  (join_key={e.join_key}, type={e.value_type})')"
        ),
        _md("## 6. List Feature Views"),
        _code(
            "feature_views = store.list_feature_views()\n"
            "print(f'Found {len(feature_views)} batch feature view(s)\\n')\n"
            "for fv in feature_views:\n"
            "    feature_names = [f.name for f in fv.features]\n"
            "    print(f'  • {fv.name}')\n"
            "    print(f'    Features : {feature_names}')\n"
            "    print(f'    Entities : {fv.entities}')\n"
            "    print(f'    TTL      : {fv.ttl}')\n"
        ),
        _md("## 7. List On-Demand Feature Views"),
        _code(
            "odfvs = store.list_on_demand_feature_views()\n"
            "if odfvs:\n"
            "    print(f'Found {len(odfvs)} on-demand feature view(s)\\n')\n"
            "    for odfv in odfvs:\n"
            "        print(f'  • {odfv.name}')\n"
            "else:\n"
            "    print('No on-demand feature views defined.')"
        ),
        _md("## 8. List Feature Services"),
        _code(
            "services = store.list_feature_services()\n"
            "if services:\n"
            "    print(f'Found {len(services)} feature service(s)\\n')\n"
            "    for svc in services:\n"
            "        views = [p.name for p in svc.feature_view_projections]\n"
            "        print(f'  • {svc.name}  -> views: {views}')\n"
            "else:\n"
            "    print('No feature services defined.')"
        ),
        _md("## 9. List Data Sources"),
        _code(
            "sources = store.list_data_sources()\n"
            "print(f'Found {len(sources)} data source(s)\\n')\n"
            "for src in sources:\n"
            "    print(f'  • {src.name}  ({type(src).__name__})')"
        ),
        _md(
            "## Next Steps\n\n"
            "- **`02_historical_features_training.ipynb`** — retrieve historical features for training.\n"
            "- **`03_online_features_serving.ipynb`** — materialize and serve online features."
        ),
    ]
    return _notebook(cells)


def _nb_historical(info: dict[str, Any], yaml_abs: str) -> dict[str, Any]:
    project = info["project"]

    cells: list[dict[str, Any]] = [
        _md(
            f"# Historical Features & Training Datasets — `{project}`\n\n"
            "Retrieve point-in-time correct feature values to build ML training datasets."
        ),
        _md("## 1. Feature Store Path"),
        _path_setup_cell(yaml_abs),
        _md("## 2. Connect to the Feature Store"),
        _code(
            "from feast import FeatureStore\n"
            "\n"
            "store = FeatureStore(fs_yaml_file=FEAST_FS_YAML)\n"
            "print(f'Project : {store.project}')\n"
            "print('Feature views:', [fv.name for fv in store.list_feature_views()])"
        ),
        _md(
            "## 3. Discover Available Features\n\nList feature views and read a sample of entity data."
        ),
        _code(
            "import pandas as pd\n"
            "from datetime import datetime, timedelta, timezone\n"
            "\n"
            "fvs      = store.list_feature_views()\n"
            "entities = store.list_entities()\n"
            "\n"
            "if not fvs:\n"
            "    print('No feature views found — run `feast apply` first.')\n"
            "else:\n"
            "    first_fv = fvs[0]\n"
            "\n"
            "    # Identify the entity join key.\n"
            "    entity_name = entities[0].join_key if entities else 'entity_id'\n"
            "    if first_fv.entities:\n"
            "        fv_entity = next(\n"
            "            (e for e in entities if e.name in set(first_fv.entities)),\n"
            "            entities[0] if entities else None,\n"
            "        )\n"
            "        if fv_entity:\n"
            "            entity_name = fv_entity.join_key\n"
            "\n"
            "    # Read latest entity values from the offline store.\n"
            "    # This uses the same mechanism Feast uses for materialization.\n"
            "    source = first_fv.batch_source\n"
            "    provider = store._get_provider()\n"
            "    sample_df = provider.offline_store.pull_latest_from_table_or_query(\n"
            "        config=store.config,\n"
            "        data_source=source,\n"
            "        join_key_columns=[entity_name],\n"
            "        feature_name_columns=[f.name for f in first_fv.features],\n"
            "        timestamp_field=source.timestamp_field,\n"
            "        created_timestamp_column=source.created_timestamp_column or '',\n"
            "        start_date=datetime(2000, 1, 1, tzinfo=timezone.utc),\n"
            "        end_date=datetime.now(tz=timezone.utc),\n"
            "    ).to_df()\n"
            "\n"
            "    print(f'Feature view    : {first_fv.name}')\n"
            "    print(f'Entity join key : {entity_name!r}')\n"
            "    print(f'Rows in source  : {len(sample_df):,}')\n"
            "    print(f'Columns         : {list(sample_df.columns)}')\n"
            "    if len(sample_df) > 0:\n"
            "        display(sample_df.head())\n"
            "    else:\n"
            "        print('No data found — check that your data source has been populated.')"
        ),
        _md(
            "## 4. Build an Entity DataFrame\n\n"
            "Specify which entity IDs and at what timestamps you want features for."
        ),
        _code(
            "if not fvs:\n"
            "    raise SystemExit('No feature views — run feast apply first.')\n"
            "\n"
            "# Use real entity IDs and timestamps from the sample.\n"
            "if entity_name in sample_df.columns and len(sample_df) > 0:\n"
            "    entity_ids = sample_df[entity_name].dropna().unique()[:5].tolist()\n"
            "    # Detect the timestamp column from the source's configuration.\n"
            "    ts_col = source.timestamp_field if source.timestamp_field in sample_df.columns else None\n"
            "    if not ts_col:\n"
            "        ts_col = next((c for c in sample_df.columns if 'timestamp' in c.lower()), None)\n"
            "    if ts_col:\n"
            "        timestamps = (\n"
            "            sample_df[sample_df[entity_name].isin(entity_ids)]\n"
            "            .sort_values(ts_col, ascending=False)\n"
            "            .drop_duplicates(subset=[entity_name])[ts_col]\n"
            "            .tolist()\n"
            "        )\n"
            "    else:\n"
            "        timestamps = [datetime.now() - timedelta(hours=i) for i in range(len(entity_ids))]\n"
            "else:\n"
            "    entity_ids = [1001, 1002, 1003]\n"
            "    timestamps = [datetime.now() - timedelta(hours=i) for i in range(len(entity_ids))]\n"
            "    print('Using placeholder entity IDs — replace with real values from your data.')\n"
            "\n"
            "entity_df = pd.DataFrame(\n"
            "    {\n"
            "        entity_name: entity_ids[:len(timestamps)],\n"
            "        'event_timestamp': timestamps[:len(entity_ids)],\n"
            "    }\n"
            ")\n"
            "print(f'Entity IDs      : {entity_ids}')\n"
            "print(f'Rows            : {len(entity_df)}')\n"
            "entity_df"
        ),
        _md("## 5. Choose Features to Retrieve"),
        _code(
            "# List all available feature views and their features.\n"
            "print('Available feature views:')\n"
            "for fv in fvs:\n"
            "    features = [f.name for f in fv.features]\n"
            "    print(f'  {fv.name}: {features}')\n"
            "\n"
            "# Select features from the first feature view.\n"
            "# Using a single view avoids name collisions across views with identical column names.\n"
            "feature_refs = [f'{first_fv.name}:{f.name}' for f in first_fv.features]\n"
            "print('\\nWill retrieve:', feature_refs)"
        ),
        _md("## 6. Retrieve Historical Features"),
        _code(
            "if feature_refs:\n"
            "    training_df = store.get_historical_features(\n"
            "        entity_df=entity_df,\n"
            "        features=feature_refs,\n"
            "    ).to_df()\n"
            "    print(f'Training dataset shape: {training_df.shape}')\n"
            "    training_df.head()\n"
            "else:\n"
            "    print('No feature views found — run `feast apply` first.')"
        ),
        _md(
            "## 7. (Optional) Retrieve via FeatureService\n\nRetrieve features using a versioned FeatureService instead of individual feature references."
        ),
        _code(
            "services = store.list_feature_services()\n"
            "if not services:\n"
            "    print('No feature services found — define one in your feature repo.')\n"
            "else:\n"
            "    svc = services[0]\n"
            "\n"
            "    # Detect extra request-data columns required by ODFVs in this service.\n"
            "    odfv_map = {v.name: v for v in store.list_on_demand_feature_views()}\n"
            "    missing_cols = {\n"
            "        field.name: field.dtype\n"
            "        for proj in svc.feature_view_projections\n"
            "        if proj.name in odfv_map\n"
            "        for rs in odfv_map[proj.name].source_request_sources.values()\n"
            "        for field in rs.schema\n"
            "        if field.name not in entity_df.columns\n"
            "    }\n"
            "\n"
            "    if missing_cols:\n"
            "        print('This service requires the following extra columns in entity_df:')\n"
            "        for col, dtype in missing_cols.items():\n"
            "            print(f'  entity_df[{col!r}] = <your {dtype} values here>')\n"
            "        print('Add them to entity_df above and re-run this cell.')\n"
            "    else:\n"
            "        # Check if service needs entity keys not already in entity_df.\n"
            "        svc_entities = set()\n"
            "        for proj in svc.feature_view_projections:\n"
            "            fv_match = next((fv for fv in fvs if fv.name == proj.name), None)\n"
            "            if fv_match:\n"
            "                for ent_name in fv_match.entities:\n"
            "                    ent_obj = next((e for e in entities if e.name == ent_name), None)\n"
            "                    if ent_obj:\n"
            "                        svc_entities.add(ent_obj.join_key)\n"
            "        missing_keys = svc_entities - set(entity_df.columns)\n"
            "        if missing_keys:\n"
            "            print(f'This service requires additional entity columns: {missing_keys}')\n"
            "            print('Add them to entity_df above and re-run this cell.')\n"
            "        else:\n"
            "            print(f'Using feature service: {svc.name}')\n"
            "            training_df_svc = store.get_historical_features(\n"
            "                entity_df=entity_df,\n"
            "                features=svc,\n"
            "                full_feature_names=True,\n"
            "            ).to_df()\n"
            "            print(f'Dataset shape: {training_df_svc.shape}')\n"
            "            training_df_svc.head()"
        ),
        _md("## 8. Use the Training Dataset"),
        _code(
            "# Example: split into features (X) and labels (y)\n"
            "# Adjust column names to match your actual feature names and label.\n"
            "if feature_refs and 'training_df' in dir():\n"
            "    label_col = 'label'  # TODO: replace with your label column\n"
            "    feature_cols = [c for c in training_df.columns\n"
            "                    if c not in ('event_timestamp', entity_name, label_col)]\n"
            "    X = training_df[feature_cols]\n"
            "    print('Feature matrix shape:', X.shape)\n"
            "    print('Feature columns:', feature_cols)"
        ),
        _md(
            "## Next Steps\n\n"
            "- **`03_online_features_serving.ipynb`** — materialize and serve online features."
        ),
    ]
    return _notebook(cells)


def _nb_online(info: dict[str, Any], yaml_abs: str) -> dict[str, Any]:
    project = info["project"]
    auth = info["auth_type"]
    vector_enabled = info["vector_enabled"]

    cells: list[dict[str, Any]] = [
        _md(
            f"# Online Feature Serving — `{project}`\n\n"
            "Materialize features and retrieve them at low latency for inference."
        ),
        _md("## 1. Feature Store Path"),
        _path_setup_cell(yaml_abs),
        _md("## 2. Connect to the Feature Store"),
        _code(
            "from feast import FeatureStore\n"
            "\n"
            "store = FeatureStore(fs_yaml_file=FEAST_FS_YAML)\n"
            "print(f'Project : {store.project}')"
        ),
    ]

    # Materialization section.
    materialize_md = (
        "## 3. Materialize Features\n\n"
        + (
            "> **Optional** — materialization is typically handled server-side.\n\n"
            if _is_operator_client(info)
            else ""
        )
        + "Load feature values into the online store for low-latency serving.\n\n"
        "| Method | When to use |\n"
        "|--------|-------------|\n"
        "| `materialize_incremental` | Regular runs — only new data since last run |\n"
        "| `materialize` | First run or full refresh of a time window |"
    )
    cells += [
        _md(materialize_md),
        _code(
            "from datetime import datetime, timedelta, timezone\n"
            "\n"
            "fvs = store.list_feature_views()\n"
            "\n"
            "if not fvs:\n"
            "    print('No feature views found — run feast apply first (see section 3).')\n"
            "else:\n"
            "    # Check last materialization watermarks across all feature views.\n"
            "    last_written = [\n"
            "        fv.materialization_intervals[-1][1]\n"
            "        for fv in fvs\n"
            "        if fv.materialization_intervals\n"
            "    ]\n"
            "\n"
            "    if not last_written:\n"
            "        # No materialization history — do a full initial load.\n"
            "        end_date   = datetime.now(tz=timezone.utc)\n"
            "        start_date = end_date - timedelta(days=30)\n"
            "        print(f'First materialization: loading {start_date.date()} → {end_date.date()} ...')\n"
            "        store.materialize(start_date=start_date, end_date=end_date)\n"
            "    else:\n"
            "        # Incremental: only pick up data since the last run.\n"
            "        end_date = datetime.now(tz=timezone.utc)\n"
            "        print(f'Incremental materialization up to {end_date} ...')\n"
            "        store.materialize_incremental(end_date=end_date)\n"
            "\n"
            "    print('Materialization complete.')"
        ),
        _md("### 3b. Force a Full Refresh"),
        _code(
            "# from datetime import datetime, timedelta, timezone\n"
            "# store.materialize(\n"
            "#     start_date=datetime.now(tz=timezone.utc) - timedelta(days=7),\n"
            "#     end_date=datetime.now(tz=timezone.utc),\n"
            "# )"
        ),
    ]

    cells += [
        _md("## 4. Retrieve Online Features"),
        _code(
            "entities = store.list_entities()\n"
            "fvs = store.list_feature_views()\n"
            "\n"
            "if not entities or not fvs:\n"
            "    print('No entities or feature views — run `feast apply` first.')\n"
            "else:\n"
            "    first_fv = fvs[0]\n"
            "    feature_refs = [f'{first_fv.name}:{f.name}' for f in first_fv.features[:3]]\n"
            "\n"
            "    # Resolve the correct entity join key for the first feature view.\n"
            "    entity_name = entities[0].join_key\n"
            "    if first_fv.entities:\n"
            "        fv_entity = next(\n"
            "            (e for e in entities if e.name in set(first_fv.entities)),\n"
            "            entities[0],\n"
            "        )\n"
            "        entity_name = fv_entity.join_key\n"
            "\n"
            "    # Discover real entity IDs from the offline source.\n"
            "    from datetime import timezone\n"
            "    source = first_fv.batch_source\n"
            "    provider = store._get_provider()\n"
            "    sample_df = provider.offline_store.pull_latest_from_table_or_query(\n"
            "        config=store.config,\n"
            "        data_source=source,\n"
            "        join_key_columns=[entity_name],\n"
            "        feature_name_columns=[f.name for f in first_fv.features],\n"
            "        timestamp_field=source.timestamp_field,\n"
            "        created_timestamp_column=source.created_timestamp_column or '',\n"
            "        start_date=datetime(2000, 1, 1, tzinfo=timezone.utc),\n"
            "        end_date=datetime.now(tz=timezone.utc),\n"
            "    ).to_df()\n"
            "\n"
            "    if len(sample_df) > 0 and entity_name in sample_df.columns:\n"
            "        entity_ids = sample_df[entity_name].dropna().unique()[:5].tolist()\n"
            "    else:\n"
            "        entity_ids = [1001, 1002]\n"
            "        print('Using placeholder IDs — replace with real values.')\n"
            "\n"
            "    entity_rows = [{entity_name: eid} for eid in entity_ids]\n"
            "\n"
            "    response = store.get_online_features(\n"
            "        features=feature_refs,\n"
            "        entity_rows=entity_rows,\n"
            "    )\n"
            "    import pandas as pd\n"
            "    print(pd.DataFrame(response.to_dict()))"
        ),
        _md(
            "## 5. Online Features via FeatureService\n\nRetrieve features using a versioned FeatureService."
        ),
        _code(
            "services = store.list_feature_services()\n"
            "if not services:\n"
            "    print('No feature services defined.')\n"
            "else:\n"
            "    svc = services[0]\n"
            "\n"
            "    # Detect extra request-data fields required by ODFVs in this service.\n"
            "    odfv_map = {v.name: v for v in store.list_on_demand_feature_views()}\n"
            "    current_keys = set(entity_rows[0].keys()) if entity_rows else set()\n"
            "    missing_fields = {\n"
            "        field.name: field.dtype\n"
            "        for proj in svc.feature_view_projections\n"
            "        if proj.name in odfv_map\n"
            "        for rs in odfv_map[proj.name].source_request_sources.values()\n"
            "        for field in rs.schema\n"
            "        if field.name not in current_keys\n"
            "    }\n"
            "\n"
            "    if missing_fields:\n"
            "        print('This service requires the following extra fields in each entity row:')\n"
            "        for col, dtype in missing_fields.items():\n"
            "            print(f'  {col!r}: <your {dtype} value here>')\n"
            "        print('Add them to entity_rows above and re-run this cell.')\n"
            "    else:\n"
            "        # Check if service needs extra entity keys beyond what we have.\n"
            "        svc_entities = set()\n"
            "        for proj in svc.feature_view_projections:\n"
            "            fv_match = next((fv for fv in fvs if fv.name == proj.name), None)\n"
            "            if fv_match:\n"
            "                for ent_name in fv_match.entities:\n"
            "                    ent_obj = next((e for e in entities if e.name == ent_name), None)\n"
            "                    if ent_obj:\n"
            "                        svc_entities.add(ent_obj.join_key)\n"
            "        missing_keys = svc_entities - current_keys\n"
            "        if missing_keys:\n"
            "            print(f'This service requires additional entity keys: {missing_keys}')\n"
            "            print('Add them to entity_rows above and re-run this cell.')\n"
            "        else:\n"
            "            print(f'Using feature service: {svc.name}')\n"
            "            response = store.get_online_features(\n"
            "                features=svc,\n"
            "                entity_rows=entity_rows,\n"
            "                full_feature_names=True,\n"
            "            )\n"
            "            import pandas as pd\n"
            "            print(pd.DataFrame(response.to_dict()))"
        ),
    ]

    if auth in ("kubernetes", "oidc"):
        cells.append(_md(f"## 6. Authentication (`{auth}`)"))
        cells.append(_code("print(store.config.auth)"))

    if vector_enabled:
        dim = info.get("embedding_dim") or 384
        section = 7 if auth in ("kubernetes", "oidc") else 6
        cells.append(
            _md(
                f"## {section}. Vector / RAG Feature Retrieval\n\nSearch stored embeddings (dim: {dim})."
            )
        )
        cells.append(
            _code(
                "import numpy as np\n"
                "\n"
                "# TODO: replace with a real query embedding from your encoder model\n"
                f"query_embedding = np.random.rand({dim}).tolist()\n"
                "\n"
                "# List feature views with vector features\n"
                "fvs = store.list_feature_views()\n"
                "vec_fvs = [\n"
                "    fv for fv in fvs\n"
                "    if any(getattr(f, 'vector_index', False) for f in fv.features)\n"
                "]\n"
                "\n"
                "if vec_fvs:\n"
                "    fv = vec_fvs[0]\n"
                "    results = store.retrieve_online_documents(\n"
                "        feature=f'{fv.name}:{fv.features[0].name}',\n"
                "        query=query_embedding,\n"
                "        top_k=5,\n"
                "    )\n"
                "    import pandas as pd\n"
                "    print(pd.DataFrame(results.to_dict()))\n"
                "else:\n"
                "    print('No vector feature views found.')"
            )
        )

    cells.append(
        _md(
            "## Next Steps\n\n"
            "- Schedule `materialize_incremental` to keep the online store fresh.\n"
        )
    )

    return _notebook(cells)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def copy_demo_notebooks(
    output_dir: str = "./feast-demo-notebooks",
    repo_path: str = ".",
    overwrite: bool = False,
) -> None:
    """Generate tailored demo notebooks for each Feast project found nearby.

    The function searches *repo_path* (default: current working directory) for
    feature-store YAML files in:

    * ``<repo_path>/feature_store.yaml``
    * Every file inside ``<repo_path>/feast-config/``

    For each project discovered a sub-directory is created under *output_dir*
    and one or more notebooks are written (the exact set depends on the project
    configuration and may grow in future releases).

    Parameters
    ----------
    output_dir:
        Root directory where notebooks are written.
        Defaults to ``./feast-demo-notebooks``.
    repo_path:
        Directory to search for ``feature_store.yaml`` files.
        Defaults to the current working directory.
    overwrite:
        When *False* (default) raise :class:`FileExistsError` if *output_dir*
        already exists.  Set to *True* to update notebooks in place.
    """
    out = pathlib.Path(output_dir).resolve()

    if not overwrite and out.exists():
        raise FileExistsError(
            f"Directory '{out}' already exists. "
            "Remove it or pass overwrite=True to update notebooks in place."
        )

    root = pathlib.Path(repo_path).absolute()
    yaml_paths = _find_feature_store_yamls(root)

    if not yaml_paths:
        _logger.warning(
            "No feature_store.yaml found under '%s'. "
            "Make sure you run this from a directory that contains feature_store.yaml "
            "or a feast-config/ subdirectory.",
            root,
        )
        return

    out.mkdir(parents=True, exist_ok=True)
    print(f"Writing demo notebooks to: {out}\n")

    for yaml_path in yaml_paths:
        raw = _parse_yaml(yaml_path)
        info = _extract_store_info(raw)
        project = info["project"]

        project_dir = out / project
        project_dir.mkdir(parents=True, exist_ok=True)

        # Absolute path — use absolute() instead of resolve() to preserve
        # Kubernetes ConfigMap/Secret symlinks.
        yaml_abs_str = str(yaml_path.absolute())

        notebooks = {
            "01_feature_store_overview.ipynb": _nb_overview(info, yaml_abs_str),
            "02_historical_features_training.ipynb": _nb_historical(info, yaml_abs_str),
            "03_online_features_serving.ipynb": _nb_online(info, yaml_abs_str),
        }

        for nb_name, nb_content in notebooks.items():
            nb_path = project_dir / nb_name
            with open(nb_path, "w") as fh:
                json.dump(nb_content, fh, indent=1)

        print(
            f"  [{project}]\n"
            f"    feature_store.yaml : {yaml_abs_str}\n"
            f"    online_store       : {info['online_store_type']}\n"
            f"    offline_store      : {info['offline_store_type']}\n"
            f"    auth               : {info['auth_type']}\n"
            + ("    vector search      : enabled\n" if info["vector_enabled"] else "")
            + f"    → {project_dir}/"
        )
        for nb_name in notebooks:
            print(f"      ✓ {nb_name}")
        print()
