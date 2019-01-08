package feast.ingestion.service;

import com.google.common.collect.Lists;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.util.HashMap;
import java.util.Map;

public class CompositeSpecService implements SpecService {

  SpecService[] specServices;

  public CompositeSpecService(SpecService... specServices) {
    this.specServices = specServices;
  }

  @Override
  public Map<String, EntitySpec> getEntitySpecs(Iterable<String> entityIds) {
    Map<String, EntitySpec> map = new HashMap<>();
    for (String entityId : entityIds) {
      RuntimeException error = null;
      for (SpecService specService : specServices) {
        try {
          map.putAll(specService.getEntitySpecs(Lists.newArrayList(entityId)));
          break;
        } catch (RuntimeException e) {
          error = e;
        }
      }
      if (error != null) {
        throw error;
      }
    }
    return map;
  }

  @Override
  public Map<String, FeatureSpec> getFeatureSpecs(Iterable<String> featureIds) {
    Map<String, FeatureSpec> map = new HashMap<>();
    for (String featureId : featureIds) {
      RuntimeException error = null;
      for (SpecService specService : specServices) {
        try {
          map.putAll(specService.getFeatureSpecs(Lists.newArrayList(featureId)));
          break;
        } catch (RuntimeException e) {
          error = e;
        }
      }
      if (error != null) {
        throw error;
      }
    }
    return map;
  }

  @Override
  public Map<String, StorageSpec> getStorageSpecs(Iterable<String> storageIds) {
    Map<String, StorageSpec> map = new HashMap<>();
    for (String storageId : storageIds) {
      RuntimeException error = null;
      for (SpecService specService : specServices) {
        try {
          map.putAll(specService.getStorageSpecs(Lists.newArrayList(storageId)));
          error = null;
          break;
        } catch (RuntimeException e) {
          error = e;
        }
      }
      if (error != null) {
        throw new RuntimeException(error);
      }
    }
    return map;
  }
}
