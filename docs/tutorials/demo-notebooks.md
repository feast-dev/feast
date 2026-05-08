# Demo Notebooks

Feast can generate tailored Jupyter notebooks for any Feast project. The notebooks adapt to your `feature_store.yaml` configuration and provide a hands-on walkthrough of core Feast functionality.

## What you get

For each project discovered, Feast creates a directory with notebooks covering:

| Notebook | Description |
|----------|-------------|
| **01 — Feature Store Overview** | Explore registered entities, feature views, feature services, and data sources. |
| **02 — Historical Feature Retrieval** | Build a training dataset with point-in-time correct joins using `get_historical_features`. |
| **03 — Online Feature Serving** | Materialize features to the online store and retrieve them at low latency with `get_online_features`. |

The content adapts automatically based on:

* **Online / offline store types** — descriptions reflect the actual backends configured.
* **Registry type** — local registries include `feast apply`; remote registries use `refresh_registry()`.
* **Authentication** — auth details from `feature_store.yaml` are surfaced when configured.
* **Vector search** — a vector/RAG retrieval section is included when embeddings are detected.

## Prerequisites

* Python 3.9+
* Feast installed (`pip install feast`)
* A feature repository with a valid `feature_store.yaml`

## Using the CLI

Run the command from (or pointing to) a directory containing `feature_store.yaml`:

```bash
feast demo-notebooks
```

This searches for `feature_store.yaml` in the current directory and every file inside the `feast-config/` directory. Each file in `feast-config/` is treated as a separate project config. For each project found, notebooks are written to `./feast-demo-notebooks/<project>/`.

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o, --output-dir` | `./feast-demo-notebooks` | Root directory for generated notebooks |
| `--overwrite` | `false` | Overwrite if the output directory already exists |

```bash
# Write to a custom directory
feast demo-notebooks -o ./my-notebooks

# Overwrite existing notebooks
feast demo-notebooks --overwrite

# Use --chdir to point at a different feature repo
feast -c /path/to/feature_repo demo-notebooks
```

## Using the Python SDK

```python
from feast import copy_demo_notebooks

copy_demo_notebooks()
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `output_dir` | `str` | `"./feast-demo-notebooks"` | Root directory for generated notebooks |
| `repo_path` | `str` | `"."` | Directory to search for `feature_store.yaml` files |
| `overwrite` | `bool` | `False` | Overwrite existing output directories |

### Examples

```python
from feast import copy_demo_notebooks

# Default — searches current directory, writes to ./feast-demo-notebooks/
copy_demo_notebooks()

# Custom paths
copy_demo_notebooks(
    output_dir="/home/user/notebooks",
    repo_path="/home/user/feast-projects/my-repo/feature_repo",
    overwrite=True,
)
```

## Multi-project repositories

If your `feast-config/` directory contains multiple files, each is treated as a separate project and a dedicated notebook directory is created:

```
feast-demo-notebooks/
├── project_alpha/
│   ├── 01_feature_store_overview.ipynb
│   ├── 02_historical_features_training.ipynb
│   └── 03_online_features_serving.ipynb
└── project_beta/
    ├── 01_feature_store_overview.ipynb
    ├── 02_historical_features_training.ipynb
    └── 03_online_features_serving.ipynb
```

## Running the notebooks

Open any generated notebook in Jupyter, JupyterLab, or VS Code and run cells from top to bottom. Each notebook:

1. Configures the path to your `feature_store.yaml` automatically (no manual editing needed).
2. Connects to the feature store using the Feast Python SDK.
3. Walks through relevant operations with real data from your project.

{% hint style="info" %}
The first notebook (**01 — Overview**) includes a prerequisites check and `feast apply` / registry sync step. Subsequent notebooks assume these have already been completed.
{% endhint %}
