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

import static feast.core.config.StorageConfig.DEFAULT_ERRORS_ID;
import static feast.core.config.StorageConfig.DEFAULT_SERVING_ID;
import static feast.core.config.StorageConfig.DEFAULT_WAREHOUSE_ID;
import static org.mockito.Mockito.when;

import feast.core.dao.EntityInfoRepository;
import feast.core.dao.FeatureGroupInfoRepository;
import feast.core.dao.FeatureInfoRepository;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureGroupSpecProto.FeatureGroupSpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.specs.StorageSpecProto.StorageSpec;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

public class SpecValidatorTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();
  private FeatureInfoRepository featureInfoRepository;
  private FeatureGroupInfoRepository featureGroupInfoRepository;
  private EntityInfoRepository entityInfoRepository;

  @Before
  public void setUp() {
    featureInfoRepository = Mockito.mock(FeatureInfoRepository.class);
    featureGroupInfoRepository = Mockito.mock(FeatureGroupInfoRepository.class);
    entityInfoRepository = Mockito.mock(EntityInfoRepository.class);
  }

  @Test
  public void featureSpecWithoutIdShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
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
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Id must contain entity, name");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithIdWithoutMatchingEntityShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("notentity.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Id must be in format entity.name, "
            + "entity in Id does not match entity provided.");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithIdWithoutMatchingNameShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.notname")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Id must be in format entity.name, "
            + "name in Id does not match name provided.");
    validator.validateFeatureSpec(input);
  }

  @Test
  public void featureSpecWithoutExistingEntityShouldThrowIllegalArgumentException() {
    when(entityInfoRepository.existsById("entity")).thenReturn(false);
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
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
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    FeatureSpec input =
        FeatureSpec.newBuilder()
            .setId("entity.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("dasdad")
            .setEntity("entity")
            .setGroup("group")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Group with id group does not exist");
    validator.validateFeatureSpec(input);
  }


  @Test
  public void featureGroupSpecWithoutIdShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
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
  public void entitySpecWithoutNameShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
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
  public void testServingStorageSpec_withValidTypes() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    validator.validateServingStorageSpec(StorageSpec.newBuilder().setId(DEFAULT_SERVING_ID)
        .setType("redis").build());
    validator.validateServingStorageSpec(StorageSpec.newBuilder().setId(DEFAULT_SERVING_ID)
        .setType("bigtable").build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testServingStorageSpec_withInvalidType() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    validator.validateServingStorageSpec(StorageSpec.newBuilder().setId(DEFAULT_SERVING_ID)
        .setType("invalid").build());
  }

  @Test
  public void testWarehouseStorageSpec_withValidTypes() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    validator.validateWarehouseStorageSpec(StorageSpec.newBuilder().setId(DEFAULT_WAREHOUSE_ID)
        .setType("file.json").build());
    validator.validateWarehouseStorageSpec(StorageSpec.newBuilder().setId(DEFAULT_WAREHOUSE_ID)
        .setType("bigquery").build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWarehouseStorageSpec_withInvalidType() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    validator.validateWarehouseStorageSpec(StorageSpec.newBuilder().setId(DEFAULT_WAREHOUSE_ID)
        .setType("invalid").build());
  }

  @Test
  public void testErrorsStorageSpec_withValidTypes() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    validator.validateErrorsStorageSpec(StorageSpec.newBuilder().setId(DEFAULT_ERRORS_ID)
        .setType("file.json").build());
    validator.validateErrorsStorageSpec(StorageSpec.newBuilder().setId(DEFAULT_ERRORS_ID)
        .setType("stderr").build());
    validator.validateErrorsStorageSpec(StorageSpec.newBuilder().setId(DEFAULT_ERRORS_ID)
        .setType("stderr").build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testErrorsStorageSpec_withInvalidType() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    validator.validateErrorsStorageSpec(StorageSpec.newBuilder().setId(DEFAULT_ERRORS_ID)
        .setType("invalid").build());
  }


  @Test
  public void storageSpecWithoutIdShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
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
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    ImportSpec input =
        ImportSpec.newBuilder().setType("file.wat?").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for import spec failed: Type file.wat? not supported");
    validator.validateImportSpec(input);
  }

  @Test
  public void fileImportSpecWithoutValidPathShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    ImportSpec input = ImportSpec.newBuilder().setType("file.csv").build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for import spec failed: Invalid options: File path cannot be empty");
    validator.validateImportSpec(input);
  }

  @Test
  public void fileImportSpecWithoutEntityIdColumnInSchemaShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    ImportSpec input =
        ImportSpec.newBuilder()
            .setType("file.csv")
            .putSourceOptions("path", "gs://asdasd")
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
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    ImportSpec input =
        ImportSpec.newBuilder()
            .setType("bigquery")
            .putSourceOptions("project", "my-google-project")
            .putSourceOptions("dataset", "feast")
            .putSourceOptions("table", "feast")
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
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);

    ImportSpec input =
        ImportSpec.newBuilder()
            .setType("pubsub")
            .putSourceOptions("topic", "my/pubsub/topic")
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
            .putSourceOptions("topic", "my/pubsub/topic")
            .setSchema(schema)
            .addEntities("someEntity")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for import spec failed: Feature some_nonexistent_feature not registered");
    validator.validateImportSpec(input);
  }

  @Test
  public void importSpecWithKafkaSourceAndCorrectOptionsShouldPassValidation() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    when(featureInfoRepository.existsById("some_existing_feature")).thenReturn(true);
    when(entityInfoRepository.existsById("someEntity")).thenReturn(true);
    Schema schema =
        Schema.newBuilder()
            .addFields(Field.newBuilder().setFeatureId("some_existing_feature").build())
            .build();
    ImportSpec input =
        ImportSpec.newBuilder()
            .setType("kafka")
            .putSourceOptions("topics", "my-kafka-topic")
            .putSourceOptions("server", "localhost:54321")
            .setSchema(schema)
            .addEntities("someEntity")
            .build();
    validator.validateImportSpec(input);
  }

  @Test
  public void importSpecWithCoalesceJobOptionsShouldPassValidation() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    when(featureInfoRepository.existsById("some_existing_feature")).thenReturn(true);
    when(entityInfoRepository.existsById("someEntity")).thenReturn(true);
    Schema schema =
        Schema.newBuilder()
            .addFields(Field.newBuilder().setFeatureId("some_existing_feature").build())
            .build();
    ImportSpec input =
        ImportSpec.newBuilder()
            .setType("kafka")
            .putSourceOptions("topics", "my-kafka-topic")
            .putSourceOptions("server", "localhost:54321")
            .putJobOptions("coalesceRows.enabled", "true")
            .putJobOptions("coalesceRows.delaySeconds", "10000")
            .putJobOptions("coalesceRows.timeoutSeconds", "20000")
            .putJobOptions("sample.limit", "1000")
            .setSchema(schema)
            .addEntities("someEntity")
            .build();
    validator.validateImportSpec(input);
  }

  @Test
  public void importSpecWithLimitJobOptionsShouldPassValidation() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    when(featureInfoRepository.existsById("some_existing_feature")).thenReturn(true);
    when(entityInfoRepository.existsById("someEntity")).thenReturn(true);
    Schema schema =
        Schema.newBuilder()
            .addFields(Field.newBuilder().setFeatureId("some_existing_feature").build())
            .build();
    ImportSpec input =
        ImportSpec.newBuilder()
            .setType("kafka")
            .putSourceOptions("topics", "my-kafka-topic")
            .putSourceOptions("server", "localhost:54321")
            .putJobOptions("sample.limit", "1000")
            .setSchema(schema)
            .addEntities("someEntity")
            .build();
    validator.validateImportSpec(input);
  }

  @Test
  public void importSpecWithKafkaSourceWithoutOptionsShouldThrowIllegalArgumentException() {
    SpecValidator validator =
        new SpecValidator(
            entityInfoRepository,
            featureGroupInfoRepository,
            featureInfoRepository);
    when(featureInfoRepository.existsById("some_existing_feature")).thenReturn(true);
    when(entityInfoRepository.existsById("someEntity")).thenReturn(true);
    Schema schema =
        Schema.newBuilder()
            .addFields(Field.newBuilder().setFeatureId("some_existing_feature").build())
            .build();
    ImportSpec input =
        ImportSpec.newBuilder()
            .setType("kafka")
            .setSchema(schema)
            .addEntities("someEntity")
            .build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Validation for import spec failed: Invalid options: Kafka ingestion requires either topics or servers");
    validator.validateImportSpec(input);
  }
}
