# Python SDK

Install the [Feast Python SDK](https://api.docs.feast.dev/python/) using pip:

```bash
pip install feast==0.9.*
```

Connect to an existing Feast Core deployment:

```python
from feast import Client

# Connect to an existing Feast Core deployment
client = Client(core_url='feast.example.com:6565')

# Ensure that your client is connected by printing out some feature tables
client.list_feature_tables()
```

