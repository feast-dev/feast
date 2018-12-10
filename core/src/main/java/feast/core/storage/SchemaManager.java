/*
 * Copyright 2018 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.core.storage;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SchemaManager {
  private final Map<String, StorageManager> storageRegistry = new ConcurrentHashMap<>();
  private final BigQueryViewTemplater viewTemplater;

  public SchemaManager(BigQueryViewTemplater viewTemplater) {
    this.viewTemplater = viewTemplater;
  }

  /**
   * Prepare warehouse and serving storage for the feature.
   *
   * @param featureSpec spec of the new feature.
   */
  public void registerFeature(FeatureSpec featureSpec) {
    DataStore servingDataStore = featureSpec.getDataStores().getServing();
    StorageManager servingStorageManager = storageRegistry.get(servingDataStore.getId());
    if (servingStorageManager != null) {
      servingStorageManager.registerNewFeature(featureSpec);
    }

    DataStore warehouseDataStore = featureSpec.getDataStores().getWarehouse();
    StorageManager warehouseStorageManager = storageRegistry.get(warehouseDataStore.getId());
    if (warehouseStorageManager != null) {
      warehouseStorageManager.registerNewFeature(featureSpec);
    }
  }

  /**
   * Register new storage.
   *
   * @param storageSpec new storage spec.
   */
  public void registerStorage(StorageSpec storageSpec) {
    String storageType = storageSpec.getType();
    Map<String, String> options = storageSpec.getOptionsMap();
    String id = storageSpec.getId();
    StorageManager storageManager = null;

    switch (storageSpec.getType()) {
      case BigTableStorageManager.TYPE:
        String btProjectId = options.get(BigTableStorageManager.OPT_BIGTABLE_PROJECT);
        String instanceId = options.get(BigTableStorageManager.OPT_BIGTABLE_INSTANCE);
        Connection connection = BigtableConfiguration.connect(btProjectId, instanceId);
        storageManager = new BigTableStorageManager(id, connection);
        break;
      case BigQueryStorageManager.TYPE:
        String datasetName = options.get(BigQueryStorageManager.OPT_BIGQUERY_DATASET);
        String bqProjectId = options.get(BigQueryStorageManager.OPT_BIGQUERY_PROJECT);
        BigQuery bigQuery =
            BigQueryOptions.newBuilder().setProjectId(bqProjectId).build().getService();
        storageManager =
            new BigQueryStorageManager(id, bigQuery, bqProjectId, datasetName, viewTemplater);
        break;
      case PostgresStorageManager.TYPE:
        String connectionUri = options.get(PostgresStorageManager.OPT_POSTGRES_URI);
        storageManager = new PostgresStorageManager(id, connectionUri);
        break;
      case RedisStorageManager.TYPE:
        storageManager = new RedisStorageManager(id);
        break;
      default:
        log.warn("Unknown storage type: {} \n {}", storageSpec.getType(), storageSpec);
        return;
    }

    storageRegistry.put(id, storageManager);
  }

  /**
   * Register several storage specs.
   *
   * @param storageSpecs
   */
  public void registerStorages(List<StorageSpec> storageSpecs) {
    for (StorageSpec storageSpec : storageSpecs) {
      registerStorage(storageSpec);
    }
  }
}
