# Feast SDK

## Installation

The Feast SDK can be installed directly using pip

```bash
pip install feast
```

Users should then be able to connect to a Feast deployment as follows

```python
from feast import Client

# Connect to an existing Feast Core deployment
client = Client(core_url='feast.example.com:6565')

# Ensure that your client is connected by printing out some feature sets
client.list_feature_sets()
```

 The Feast SDK also comes with a command line interface

```aspnet
$ feast

Usage: feast [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  config        View and edit Feast properties
  feature-sets  Create and manage feature sets
  ingest        Ingest feature data into a feature set
  projects      Create and manage projects
  version       Displays version and connectivity information
```



