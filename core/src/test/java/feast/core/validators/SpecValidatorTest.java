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

import static org.mockito.Mockito.when;

import com.google.common.base.Strings;
import feast.core.dao.EntityInfoRepository;
import feast.core.dao.FeatureGroupInfoRepository;
import feast.core.dao.FeatureInfoRepository;
import feast.core.dao.StorageInfoRepository;
import feast.core.model.FeatureGroupInfo;
import feast.core.model.StorageInfo;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureGroupSpecProto.FeatureGroupSpec;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.GranularityProto.Granularity;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

public class SpecValidatorTest {
  private FeatureInfoRepository featureInfoRepository;
  private FeatureGroupInfoRepository featureGroupInfoRepository;
  private EntityInfoRepository entityInfoRepository;
  private StorageInfoRepository storageInfoRepository;

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() {
    featureInfoRepository = Mockito.mock(FeatureInfoRepository.class);
    featureGroupInfoRepository = Mockito.mock(FeatureGroupInfoRepository.class);
    entityInfoRepository = Mockito.mock(EntityInfoRepository.class);
    storageInfoRepository = Mockito.mock(StorageInfoRepository.class);
  }

  @Test
  public void featureSpecWithoutIdShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input = FeatureSpec.newBuilder().build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Id field cannot be empty");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithoutNameShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input = FeatureSpec.newBuilder().setId("aa").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Name field cannot be empty");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithInvalidNameShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input = FeatureSpec.newBuilder().setId("test").setName("hello there!").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for feature spec with id test failed: invalid value for "
            + "field Name: argument must be in lower snake case, and cannot include any special characters.");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithoutOwnerShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input = FeatureSpec.newBuilder().setId("id").setName("name").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Owner field cannot be empty");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithoutDescriptionShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder().setId("id").setName("name").setOwner("owner").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Description field cannot be empty");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithoutEntityShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("id")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Entity field cannot be empty");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithIdWithoutThreeWordsShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("id")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGranularity(Granularity.Enum.forNumber(1))
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Id must contain entity, granularity, name");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithIdWithoutMatchingEntityShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("notentity.granularity.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGranularity(Granularity.Enum.forNumber(1))
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Id must be in format entity.granularity.name, "
            + "entity in Id does not match entity provided.");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithIdWithoutMatchingGranularityShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.granularity.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGranularity(Granularity.Enum.forNumber(0))
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Id must be in format entity.granularity.name, "
            + "granularity in Id does not match granularity provided.");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithIdWithoutMatchingNameShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.none.notname")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGranularity(Granularity.Enum.forNumber(0))
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Id must be in format entity.granularity.name, "
            + "name in Id does not match name provided.");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithoutExistingEntityShouldThrowIllegalArgumentException() {
    when(entityInfoRepository.existsById("entity")).thenReturn(false);
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.none.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGranularity(Granularity.Enum.forNumber(0))
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Entity with name entity does not exist");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithInvalidFeatureGroupShouldThrowIllegalArgumentException() {
    when(entityInfoRepository.existsById("entity")).thenReturn(true);
    when(featureGroupInfoRepository.existsById("group")).thenReturn(false);
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.none.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGranularity(Granularity.Enum.forNumber(0))
            .setGroup("group")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Group with id group does not exist");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithoutExistingServingStoreShouldThrowIllegalArgumentException() {
    when(entityInfoRepository.existsById("entity")).thenReturn(true);
    when(storageInfoRepository.existsById("REDIS1")).thenReturn(false);
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    DataStore servingStore = DataStore.newBuilder().setId("REDIS1").build();
    DataStores dataStores = DataStores.newBuilder().setServing(servingStore).build();
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.none.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGranularity(Granularity.Enum.forNumber(0))
            .setDataStores(dataStores)
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Serving store with id REDIS1 does not exist");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithoutServingStoreShouldInheritServingStoreIdFromGroup() {
    when(entityInfoRepository.existsById("entity")).thenReturn(true);
    when(storageInfoRepository.existsById("REDIS1")).thenReturn(true);
    when(storageInfoRepository.existsById("REDIS2")).thenReturn(false);
    FeatureGroupInfo fgi = new FeatureGroupInfo();
    StorageInfo redis1 = new StorageInfo();
    redis1.setId("REDIS1");
    redis1.setType("redis");
    fgi.setServingStore(redis1);
    when(storageInfoRepository.findById("REDIS1")).thenReturn(Optional.of(redis1));
    when(featureGroupInfoRepository.existsById("group")).thenReturn(true);
    when(featureGroupInfoRepository.findById("group")).thenReturn(Optional.of(fgi));
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    DataStore warehouseStore = DataStore.newBuilder().setId("REDIS2").build();
    DataStores dataStores = DataStores.newBuilder().setWarehouse(warehouseStore).build();
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.none.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGroup("group")
            .setGranularity(Granularity.Enum.forNumber(0))
            .setDataStores(dataStores)
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Warehouse store with id REDIS2 does not exist");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithoutExistingWarehouseStoreShouldThrowIllegalArgumentException() {
    String servingStoreId = "REDIS1";
    String warehouseStoreId = "BIGQUERY";
    when(entityInfoRepository.existsById("entity")).thenReturn(true);
    when(storageInfoRepository.existsById(servingStoreId)).thenReturn(true);
    when(storageInfoRepository.existsById(warehouseStoreId)).thenReturn(false);

    StorageInfo redis1 = new StorageInfo();
    redis1.setId(servingStoreId);
    redis1.setType("redis");
    when(storageInfoRepository.findById( servingStoreId)).thenReturn(Optional.of(redis1));

    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    DataStore servingStore = DataStore.newBuilder().setId(servingStoreId).build();
    DataStore warehouseStore = DataStore.newBuilder().setId(warehouseStoreId).build();
    DataStores dataStores =
        DataStores.newBuilder().setServing(servingStore).setWarehouse(warehouseStore).build();
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.none.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGranularity(Granularity.Enum.forNumber(0))
            .setDataStores(dataStores)
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(String.format("Warehouse store with id %s does not exist", warehouseStoreId));
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithUnsupportedWarehouseStoreShouldThrowIllegalArgumentException() {
    String servingStoreId = "REDIS1";
    StorageSpec servingStoreSpec = StorageSpec.newBuilder().setId(servingStoreId).setType("redis").build();
    StorageInfo servingStoreInfo = new StorageInfo(servingStoreSpec);

    String warehouseStoreId = "REDIS2";
    StorageSpec warehouseStoreSpec = StorageSpec.newBuilder().setId(warehouseStoreId).setType("redis").build();
    StorageInfo warehouseStoreInfo = new StorageInfo(warehouseStoreSpec);

    when(entityInfoRepository.existsById("entity")).thenReturn(true);
    when(storageInfoRepository.existsById(servingStoreId)).thenReturn(true);
    when(storageInfoRepository.existsById(warehouseStoreId)).thenReturn(true);
    when(storageInfoRepository.findById(servingStoreId)).thenReturn(Optional.of(servingStoreInfo));
    when(storageInfoRepository.findById(warehouseStoreId)).thenReturn(Optional.of(warehouseStoreInfo));
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    DataStore servingStore = DataStore.newBuilder().setId(servingStoreId).build();
    DataStore warehouseStore = DataStore.newBuilder().setId(warehouseStoreId).build();
    DataStores dataStores =
        DataStores.newBuilder().setServing(servingStore).setWarehouse(warehouseStore).build();
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.none.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGranularity(Granularity.Enum.forNumber(0))
            .setDataStores(dataStores)
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(Strings.lenientFormat("Unsupported warehouse store type", "redis"));
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithUnsupportedServingStoreShouldThrowIllegalArgumentException() {
    String servingStoreName = "CASSANDRA";
    StorageSpec redis1Spec = StorageSpec.newBuilder()
        .setId(servingStoreName)
        .setType("cassandra")
        .build();
    StorageInfo redis1StorageInfo = new StorageInfo(redis1Spec);

    String warehouseStorageName = "BIGQUERY";
    StorageSpec bqSpec = StorageSpec.newBuilder()
        .setId(warehouseStorageName)
        .setType("bigquery")
        .build();
    StorageInfo bqInfo = new StorageInfo(bqSpec);

    when(entityInfoRepository.existsById("entity")).thenReturn(true);
    when(storageInfoRepository.existsById(servingStoreName)).thenReturn(true);
    when(storageInfoRepository.existsById(warehouseStorageName)).thenReturn(true);
    when(storageInfoRepository.findById(servingStoreName)).thenReturn(Optional.of(redis1StorageInfo));
    when(storageInfoRepository.findById(warehouseStorageName)).thenReturn(Optional.of(bqInfo));
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    DataStore servingStore = DataStore.newBuilder().setId(servingStoreName).build();
    DataStore warehouseStore = DataStore.newBuilder().setId(warehouseStorageName).build();
    DataStores dataStores =
        DataStores.newBuilder().setServing(servingStore).setWarehouse(warehouseStore).build();
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.none.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGranularity(Granularity.Enum.forNumber(0))
            .setDataStores(dataStores)
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(Strings.lenientFormat("Unsupported serving store type", "cassandra"));
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureGroupSpecWithoutIdShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureGroupSpec input = FeatureGroupSpec.newBuilder().build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Id field cannot be empty");
    validator.validateFeatureGroupSpec(input);
  }

  @Test
  public void featureGroupSpecWithoutValidIdShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureGroupSpec input = FeatureGroupSpec.newBuilder().setId("NOT_VALID").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "invalid value for "
            + "field Id: argument must be in lower snake case, and cannot include any special characters.");
    validator.validateFeatureGroupSpec(input);
  }

  @Test
  public void featureGroupSpecWithNonexistentServingStoreShouldThrowIllegalArgumentException() {
    when(storageInfoRepository.existsById("REDIS1")).thenReturn(false);
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    DataStore servingDataStore = DataStore.newBuilder().setId("REDIS1").build();
    DataStores dataStores = DataStores.newBuilder().setServing(servingDataStore).build();
    FeatureGroupSpec input =
        FeatureGroupSpec.newBuilder().setId("valid").setDataStores(dataStores).build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Serving store with id REDIS1 does not exist");
    validator.validateFeatureGroupSpec(input);
  }

  @Test
  public void featureGroupSpecWithNonexistentWarehouseStoreShouldThrowIllegalArgumentException() {
    when(storageInfoRepository.existsById("REDIS1")).thenReturn(true);
    when(storageInfoRepository.existsById("REDIS2")).thenReturn(false);
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    DataStore servingDataStore = DataStore.newBuilder().setId("REDIS1").build();
    DataStore warehouseDataStore = DataStore.newBuilder().setId("REDIS2").build();
    DataStores dataStores =
        DataStores.newBuilder()
            .setServing(servingDataStore)
            .setWarehouse(warehouseDataStore)
            .build();
    FeatureGroupSpec input =
        FeatureGroupSpec.newBuilder().setId("valid").setDataStores(dataStores).build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Warehouse store with id REDIS2 does not exist");
    validator.validateFeatureGroupSpec(input);
  }

  @Test
  public void entitySpecWithoutNameShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    EntitySpec input = EntitySpec.newBuilder().build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Name field cannot be empty");
    validator.validateEntitySpec(input);
  }

  @Test
  public void entitySpecWithInvalidNameShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    EntitySpec input = EntitySpec.newBuilder().setName("INVALID NAME!").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for entity spec with name INVALID NAME! failed:"
            + " invalid value for field Name: argument must be in lower snake case, and cannot include "
            + "any special characters.");
    validator.validateEntitySpec(input);
  }

  @Test
  public void storageSpecWithoutIdShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    StorageSpec input = StorageSpec.newBuilder().build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Id field cannot be empty");
    validator.validateStorageSpec(input);
  }

  @Test
  public void importSpecWithInvalidTypeShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    ImportSpec input = ImportSpec.newBuilder().setType("blah").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Validation for import spec failed: Type blah not supported");
    validator.validateImportSpec(input);
  }

  @Test
  public void pubsubImportSpecWithoutTopicOrSubscriptionShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    ImportSpec input = ImportSpec.newBuilder().setType("pubsub").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for import spec failed: Invalid options: Pubsub ingestion requires either topic or subscription");
    validator.validateImportSpec(input);
  }

  @Test
  public void fileImportSpecWithoutSupportedFileFormatShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    ImportSpec input =
        ImportSpec.newBuilder().setType("file").putOptions("format", "notSupported").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for import spec failed: Invalid options: File format must be of type 'json' or 'csv'");
    validator.validateImportSpec(input);
  }

  @Test
  public void fileImportSpecWithoutValidPathShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    ImportSpec input = ImportSpec.newBuilder().setType("file").putOptions("format", "csv").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for import spec failed: Invalid options: File path cannot be empty");
    validator.validateImportSpec(input);
  }

  @Test
  public void fileImportSpecWithoutEntityIdColumnInSchemaShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    ImportSpec input =
        ImportSpec.newBuilder()
            .setType("file")
            .putOptions("format", "csv")
            .putOptions("path", "gs://asdasd")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for import spec failed: entityId column must be specified in schema");
    validator.validateImportSpec(input);
  }

  @Test
  public void bigQueryImportSpecWithoutEntityIdColumnInSchemaShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    ImportSpec input =
        ImportSpec.newBuilder()
            .setType("bigquery")
            .putOptions("project", "my-google-project")
            .putOptions("dataset", "feast")
            .putOptions("table", "feast")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for import spec failed: entityId column must be specified in schema");
    validator.validateImportSpec(input);
  }

  @Test
  public void importSpecWithoutValidEntityShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);

    ImportSpec input =
        ImportSpec.newBuilder()
            .setType("pubsub")
            .putOptions("topic", "my/pubsub/topic")
            .addEntities("someEntity")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Validation for import spec failed: Entity someEntity not registered");
    validator.validateImportSpec(input);
  }

  @Test
  public void importSpecWithUnregisteredFeaturesShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            storageInfoRepository,
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    when(featureInfoRepository.existsById("some_existing_feature")).thenReturn(true);
    Schema schema =
        Schema.newBuilder()
            .addFields(Field.newBuilder().setFeatureId("some_existing_feature").build())
            .addFields(Field.newBuilder().setFeatureId("some_nonexistent_feature").build())
            .build();
    ImportSpec input =
        ImportSpec.newBuilder()
            .setType("pubsub")
            .putOptions("topic", "my/pubsub/topic")
            .setSchema(schema)
            .addEntities("someEntity")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for import spec failed: Feature some_nonexistent_feature not registered");
    validator.validateImportSpec(input);
  }
}
