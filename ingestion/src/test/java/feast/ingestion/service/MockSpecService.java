package feast.ingestion.service;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.testcontainers.shaded.com.google.common.base.Preconditions;

public class MockSpecService implements SpecService {

  public Map<String, EntitySpec> entitySpecs = new HashMap<>();
  public Map<String, FeatureSpec> featureSpecs = new HashMap<>();
  public Map<String, StorageSpec> storageSpecs = new HashMap<>();

  public <T> void checkValuesNotNull(Map<String, T> map) {
    for (Entry<String, T> entry : map.entrySet()) {
      Preconditions.checkNotNull(entry.getValue(), "not found for " + entry.getKey());
    }
  }

  @Override
  public Map<String, EntitySpec> getEntitySpecs(Iterable<String> entityIds) {
    Set<String> entityIdsSet = Sets.newHashSet(entityIds);
    Map<String, EntitySpec> map = Maps.newHashMap(
        Maps.filterEntries(entitySpecs, (entry) -> entityIdsSet.contains(entry.getKey())));
    checkValuesNotNull(map);
    return map;
  }

  @Override
  public Map<String, FeatureSpec> getFeatureSpecs(Iterable<String> featureIds) {
    Set<String> featureIdsSet = Sets.newHashSet(featureIds);
    Map<String, FeatureSpec>  map = Maps.newHashMap(
        Maps.filterEntries(featureSpecs, (entry) -> featureIdsSet.contains(entry.getKey())));
    checkValuesNotNull(map);
    return map;
  }

  @Override
  public Map<String, StorageSpec> getStorageSpecs(Iterable<String> storageIds) {
    Set<String> storageIdsSet = Sets.newHashSet(storageIds);
    Map<String, StorageSpec> map = Maps.newHashMap(
        Maps.filterEntries(storageSpecs, (entry) -> storageIdsSet.contains(entry.getKey())));
    checkValuesNotNull(map);
    return map;
  }

  public MockSpecService addStorage(StorageSpec storageSpec) {
    storageSpecs.put(storageSpec.getId(), storageSpec);
    return this;
  }

  public MockSpecService addFeature(FeatureSpec featureSpec) {
    featureSpecs.put(featureSpec.getId(), featureSpec);
    return this;
  }

  public MockSpecService addEntity(EntitySpec entitySpec) {
    entitySpecs.put(entitySpec.getName(), entitySpec);
    return this;
  }
}