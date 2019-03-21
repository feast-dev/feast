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

package feast.core.validators;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static feast.core.validators.Matchers.checkLowerSnakeCase;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import feast.core.dao.EntityInfoRepository;
import feast.core.dao.FeatureGroupInfoRepository;
import feast.core.dao.FeatureInfoRepository;
import feast.core.dao.StorageInfoRepository;
import feast.core.model.FeatureGroupInfo;
import feast.core.model.StorageInfo;
import feast.core.storage.BigQueryStorageManager;
import feast.core.storage.BigTableStorageManager;
import feast.core.storage.PostgresStorageManager;
import feast.core.storage.RedisStorageManager;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureGroupSpecProto.FeatureGroupSpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;

public class SpecValidator {

  private static final String NO_STORE = "";
  private static final String[] SUPPORTED_WAREHOUSE_STORES =
      new String[]{
          BigQueryStorageManager.TYPE, "file.json",
      };
  private static final String[] SUPPORTED_SERVING_STORES =
      new String[]{
          BigTableStorageManager.TYPE, PostgresStorageManager.TYPE, RedisStorageManager.TYPE,
      };
  private static final String[] SUPPORTED_ERRORS_STORES = new String[]{"file.json", "stderr",
      "stdout"};

  private StorageInfoRepository storageInfoRepository;
  private EntityInfoRepository entityInfoRepository;
  private FeatureGroupInfoRepository featureGroupInfoRepository;
  private FeatureInfoRepository featureInfoRepository;

  @Autowired
  public SpecValidator(
      StorageInfoRepository storageInfoRepository,
      EntityInfoRepository entityInfoRepository,
      FeatureGroupInfoRepository featureGroupInfoRepository,
      FeatureInfoRepository featureInfoRepository) {

    this.storageInfoRepository = storageInfoRepository;
    this.entityInfoRepository = entityInfoRepository;
    this.featureGroupInfoRepository = featureGroupInfoRepository;
    this.featureInfoRepository = featureInfoRepository;
  }

  /**
   * Validates a given feature spec's contents, throwing and IllegalArgumentException if the spec is
   * invalid.
   */
  public void validateFeatureSpec(FeatureSpec spec) throws IllegalArgumentException {
    try {
      // check not not null
      checkArgument(!spec.getId().equals(""), "Id field cannot be empty");
      checkArgument(!spec.getName().equals(""), "Name field cannot be empty");
      checkLowerSnakeCase(spec.getName(), "Name");
      checkArgument(!spec.getOwner().equals(""), "Owner field cannot be empty");
      checkArgument(!spec.getDescription().equals(""), "Description field cannot be empty");
      checkArgument(!spec.getEntity().equals(""), "Entity field cannot be empty");

      // check id validity
      String[] idSplit = spec.getId().split("\\.");
      checkArgument(idSplit.length == 3, "Id must contain entity, granularity, name");
      checkArgument(
          idSplit[0].equals(spec.getEntity()),
          "Id must be in format entity.granularity.name, entity in Id does not match entity provided.");
      checkArgument(
          idSplit[1].equals(spec.getGranularity().toString().toLowerCase()),
          "Id must be in format entity.granularity.name, granularity in Id does not match granularity provided.");
      checkArgument(
          idSplit[2].equals(spec.getName()),
          "Id must be in format entity.granularity.name, name in Id does not match name provided.");

      // check if referenced objects exist
      checkArgument(
          entityInfoRepository.existsById(spec.getEntity()),
          Strings.lenientFormat("Entity with name %s does not exist", spec.getEntity()));

      // TODO: clean up store validation for features
      String servingStoreId = NO_STORE;
      String warehouseStoreId = NO_STORE;
      if (spec.hasDataStores()) {
        servingStoreId =
            spec.getDataStores().hasServing() ? spec.getDataStores().getServing().getId() : "";
        warehouseStoreId =
            spec.getDataStores().hasWarehouse() ? spec.getDataStores().getWarehouse().getId() : "";
      }
      if (!spec.getGroup().equals("")) {
        Optional groupOptional = featureGroupInfoRepository.findById(spec.getGroup());
        if (!groupOptional.isPresent()) {
          throw new IllegalArgumentException(
              Strings.lenientFormat("Group with id %s does not exist", spec.getGroup()));
        }
        FeatureGroupInfo group = (FeatureGroupInfo) groupOptional.get();
        servingStoreId =
            servingStoreId.equals(NO_STORE) ? group.getServingStore().getId() : servingStoreId;
        warehouseStoreId =
            warehouseStoreId.equals(NO_STORE) ? group.getWarehouseStore().getId()
                : warehouseStoreId;
      }
      Optional<StorageInfo> servingStore = storageInfoRepository.findById(servingStoreId);
      Optional<StorageInfo> warehouseStore = storageInfoRepository.findById(warehouseStoreId);
      checkArgument(
          servingStore.isPresent(),
          Strings.lenientFormat("Serving store with id %s does not exist", servingStoreId));
      checkArgument(
          Arrays.asList(SUPPORTED_SERVING_STORES).contains(servingStore.get().getType()),
          Strings.lenientFormat("Unsupported serving store type", servingStore.get().getType()));

      if (!warehouseStoreId.equals(NO_STORE)) {
        checkArgument(
            warehouseStore.isPresent(),
            Strings.lenientFormat("Warehouse store with id %s does not exist", warehouseStoreId));

        checkArgument(
            Arrays.asList(SUPPORTED_WAREHOUSE_STORES).contains(warehouseStore.get().getType()),
            Strings.lenientFormat(
                "Unsupported warehouse store type", warehouseStore.get().getType()));
      }

    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat(
              "Validation for feature spec with id %s failed: %s", spec.getId(), e.getMessage()));
    }
  }

  public void validateFeatureGroupSpec(FeatureGroupSpec spec) throws IllegalArgumentException {
    try {
      checkArgument(!spec.getId().equals(""), "Id field cannot be empty");
      checkLowerSnakeCase(spec.getId(), "Id");
      if (spec.hasDataStores()) {
        if (spec.getDataStores().hasServing()
            && !spec.getDataStores().getServing().getId().equals("")) {
          String servingStoreId = spec.getDataStores().getServing().getId();
          checkArgument(
              storageInfoRepository.existsById(servingStoreId),
              Strings.lenientFormat("Serving store with id %s does not exist", servingStoreId));
        }
        if (spec.getDataStores().hasWarehouse()
            && !spec.getDataStores().getWarehouse().getId().equals("")) {
          String warehouseStoreId = spec.getDataStores().getWarehouse().getId();
          checkArgument(
              storageInfoRepository.existsById(warehouseStoreId),
              Strings.lenientFormat("Warehouse store with id %s does not exist", warehouseStoreId));
        }
      }
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat(
              "Validation for feature group spec with id %s failed: %s",
              spec.getId(), e.getMessage()));
    }
  }

  public void validateEntitySpec(EntitySpec spec) throws IllegalArgumentException {
    try {
      checkArgument(!spec.getName().equals(""), "Name field cannot be empty");
      checkLowerSnakeCase(spec.getName(), "Name");
      checkNotNull(spec.getDescription(), "Description field cannot be empty");
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat(
              "Validation for entity spec with name %s failed: %s",
              spec.getName(), e.getMessage()));
    }
  }

  public void validateStorageSpec(StorageSpec spec) throws IllegalArgumentException {
    try {
      checkArgument(!spec.getId().equals(""), "Id field cannot be empty");
      Matchers.checkUpperSnakeCase(spec.getId(), "Id");
      checkArgument(Streams.concat(
          Arrays.stream(SUPPORTED_SERVING_STORES),
          Arrays.stream(SUPPORTED_WAREHOUSE_STORES)).collect(Collectors.toList())
              .contains(spec.getType()),
          "Store type not supported " + spec.getType());
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat(
              "Validation for storage spec with id %s failed: %s", spec.getId(), e.getMessage()),
          e);
    }
  }

  // TODO: add validation for storage types and options
  public void validateServingStorageSpec(StorageSpec spec) throws IllegalArgumentException {
    try {
      checkArgument(!spec.getId().equals(""), "Id field cannot be empty");
      Matchers.checkUpperSnakeCase(spec.getId(), "Id");
      checkArgument(Arrays.asList(SUPPORTED_SERVING_STORES).contains(spec.getType()),
          "Serving store type not supported " + spec.getType());
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat(
              "Validation for storage spec with id %s failed: %s", spec.getId(), e.getMessage()),
          e);
    }
  }

  public void validateWarehouseStorageSpec(StorageSpec spec) throws IllegalArgumentException {
    try {
      checkArgument(!spec.getId().equals(""), "Id field cannot be empty");
      Matchers.checkUpperSnakeCase(spec.getId(), "Id");
      checkArgument(Arrays.asList(SUPPORTED_WAREHOUSE_STORES).contains(spec.getType()),
          "Warehouse store type not supported " + spec.getType());
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat(
              "Validation for storage spec with id %s failed: %s", spec.getId(), e.getMessage()),
          e);
    }
  }

  public void validateErrorsStorageSpec(StorageSpec spec) throws IllegalArgumentException {
    try {
      checkArgument(!spec.getId().equals(""), "Id field cannot be empty");
      Matchers.checkUpperSnakeCase(spec.getId(), "Id");
      checkArgument(Arrays.asList(SUPPORTED_ERRORS_STORES).contains(spec.getType()),
          "Errors store type not supported " + spec.getType());
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat(
              "Validation for storage spec with id %s failed: %s", spec.getId(), e.getMessage()),
          e);
    }
  }

  public void validateImportSpec(ImportSpec spec) throws IllegalArgumentException {
    try {
      switch (spec.getType()) {
        case "kafka":
          checkKafkaImportSpecOption(spec);
          break;
        case "pubsub":
          checkPubSubImportSpecOption(spec);
          break;
        case "file.csv":
        case "file.json":
          checkFileImportSpecOption(spec);
          checkArgument(
              !spec.getSchema().getEntityIdColumn().equals(""),
              "entityId column must be specified in schema");
          break;
        case "bigquery":
          checkBigqueryImportSpecOption(spec);
          checkArgument(
              !spec.getSchema().getEntityIdColumn().equals(""),
              "entityId column must be specified in schema");
          break;
        default:
          throw new IllegalArgumentException(
              Strings.lenientFormat("Type %s not supported", spec.getType()));
      }
      spec.getSchema()
          .getFieldsList()
          .stream()
          .map(Field::getFeatureId)
          .filter(featureId -> !featureId.equals(""))
          .forEach(
              id ->
                  checkArgument(
                      featureInfoRepository.existsById(id),
                      Strings.lenientFormat("Feature %s not registered", id)));
      for (String name : spec.getEntitiesList()) {
        checkArgument(
            entityInfoRepository.existsById(name),
            Strings.lenientFormat("Entity %s not registered", name));
      }
      Map<String, String> jobOptions = spec.getJobOptionsMap();
      if (jobOptions.size() > 0) {
        List<String> opts = Lists.newArrayList(
            "sample.limit",
            "coalesceRows.enabled",
            "coalesceRows.delaySeconds",
            "coalesceRows.timeoutSeconds"
        );
        for (String key : jobOptions.keySet()) {
          Preconditions.checkArgument(opts.contains(key),
              Strings.lenientFormat("Option %s is not a valid jobOption", key));
        }
      }
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat("Validation for import spec failed: %s", e.getMessage()));
    }
  }

  private void checkKafkaImportSpecOption(ImportSpec spec) {
    try {
      String topics = spec.getSourceOptionsOrDefault("topics", "");
      String server = spec.getSourceOptionsOrDefault("server", "");
      if (topics.equals("") && server.equals("")) {
        throw new IllegalArgumentException(
            "Kafka ingestion requires either topics or servers");
      }
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat("Invalid options: %s", e.getMessage()));
    }
  }

  private void checkFileImportSpecOption(ImportSpec spec) throws IllegalArgumentException {
    try {
      checkArgument(!spec.getSourceOptionsOrDefault("path", "").equals(""),
          "File path cannot be empty");
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat("Invalid options: %s", e.getMessage()));
    }
  }

  private void checkPubSubImportSpecOption(ImportSpec spec) throws IllegalArgumentException {
    try {
      String topic = spec.getSourceOptionsOrDefault("topic", "");
      String subscription = spec.getSourceOptionsOrDefault("subscription", "");
      if (topic.equals("") && subscription.equals("")) {
        throw new IllegalArgumentException(
            "Pubsub ingestion requires either topic or subscription");
      }
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat("Invalid options: %s", e.getMessage()));
    }
  }

  private void checkBigqueryImportSpecOption(ImportSpec spec) throws IllegalArgumentException {
    try {
      checkArgument(!spec.getSourceOptionsOrThrow("project").equals(""),
          "Bigquery project cannot be empty");
      checkArgument(!spec.getSourceOptionsOrThrow("dataset").equals(""),
          "Bigquery dataset cannot be empty");
      checkArgument(!spec.getSourceOptionsOrThrow("table").equals(""),
          "Bigquery table cannot be empty");
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Strings.lenientFormat("Invalid options: %s", e.getMessage()));
    }
  }
}