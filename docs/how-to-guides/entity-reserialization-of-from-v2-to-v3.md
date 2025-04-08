# Entity Key Re-Serialization from Version 2 to 3

Entity Key Serialization version 2 will soon be deprecated, hence we need to shift the serialization and deserilization to version 3. 

But here comes the challegnge where existing FeatuteViews on stores has written features with version 2. A version 2 serialized entity key cant be retrived using version 3 deserilization algorithm.

## Reserialize the Feature Views entity Keys to version 3

The solution is to reserialize the entity keys from version 2 to version 3.

Follow the following procedures to reserialize the entity key to version 3 in feature View in an offline / online store.

In hosrt, you need to iterate through all the feature views in your Feast repository, retrieve their serialized entity keys (if stored in version 2), reserialize them to version 3, and then update the online/offline store or wherever the serialized keys are stored.

### 1. Initialize the Feature Store

    Load the FeatureStore object to access all feature views in your repository.

### 2. Iterate Through Feature Views

    Use the list_feature_views() method to retrieve all feature views in the repository.

### 3. Retrieve Serialized Entity Keys

    For each feature view, retrieve the serialized entity keys stored in the online/offline store or other storage

### 4. Reserialize Entity Keys

    Use the reserialize_entity_v2_key_to_v3 function to convert the serialized keys from version 2 to version 3. Use [entity key encoding utils](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/key_encoding_utils.py) function `reserialize_entity_v2_key_to_v3`.

### 5. Update the Online/offline Store

    Write the reserialized keys back to the online/offline store or the appropriate storage
