# Python SDK

The Feast SDK can be installed directly using pip:

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

