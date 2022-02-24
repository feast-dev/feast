import numpy as np

import time
from feast.feature_store import FeatureStore

num_features = 50
entity_keyspace = 10**4
entity_rows = 1 

fs = FeatureStore(repo_path=".")
features = [ 
    f"feature_view_{i // 10}:feature_{i}"
    for i in range(num_features)
]
join_keys = np.random.randint(0, entity_keyspace, entity_rows).tolist()
entity_rows= [
    {"entity": join_key}
    for join_key in join_keys
]

# Force Go server to startup.
results = fs.get_online_features(
    features=features,
    entity_rows=entity_rows,
)

start_time = time.time()
results = fs.get_online_features(
    features=features,
    entity_rows=entity_rows,
)
end_time = time.time()
print(f"total time: {end_time - start_time} seconds")
