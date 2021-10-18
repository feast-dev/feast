### Instructions how to start with astra online store

1. init the feature store repo using `feast init`
2. provide your online store information:
         example: 
```project: <<project_name>>
registry: data/registry.db
provider: astra
offline_store:
    type: <<online store type (default offline store for astra is file)>>
online_store:
    type: astra
    client_id: <<client id from astra databasse>>
    secure_connect_bundle: <<path for secure bundle connect zip file>>
    secret_key: <<client secret key>>
    keyspace: astra keyspace```
