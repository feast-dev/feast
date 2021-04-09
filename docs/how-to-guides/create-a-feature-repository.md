# Create a feature repository

Please see [Feature Repository](../concepts/feature-repository.md) for a detailed explanation of the purpose and structure of feature repositories.

The easiest way to create a new feature repository to use `feast init` command:

```bash
mkdir my_feature_repo && cd my_feature_repo
feast init
```

The `init` command creates a Python file with feature definitions, sample data, and a Feast configuration file for local development:

```bash
$ tree
.
├── data
│   └── driver_stats.parquet
├── example.py
└── feature_store.yaml

1 directory, 3 files
```

