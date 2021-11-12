/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
 */
package feast.serving.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import feast.common.it.DataGenerator;
import feast.common.models.FeatureV2;
import feast.proto.core.EntityProto;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.types.ValueProto;
import io.grpc.ManagedChannel;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@ActiveProfiles("it")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "feast.core-cache-refresh-interval=1",
      "feast.active_store=cassandra",
      "spring.main.allow-bean-definition-overriding=true"
    })
@Testcontainers
public class ServingServiceCassandraIT extends BaseAuthIT {

  static final Map<String, String> options = new HashMap<>();
  static CoreSimpleAPIClient coreClient;
  static ServingServiceGrpc.ServingServiceBlockingStub servingStub;

  static CqlSession cqlSession;
  static final int FEAST_SERVING_PORT = 6570;

  static final FeatureReferenceV2 feature1Reference =
      DataGenerator.createFeatureReference("rides", "trip_cost");
  static final FeatureReferenceV2 feature2Reference =
      DataGenerator.createFeatureReference("rides", "trip_distance");
  static final FeatureReferenceV2 feature3Reference =
      DataGenerator.createFeatureReference("rides", "trip_empty");
  static final FeatureReferenceV2 feature4Reference =
      DataGenerator.createFeatureReference("rides", "trip_wrong_type");

  @ClassRule @Container
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
              new File("src/test/resources/docker-compose/docker-compose-cassandra-it.yml"))
          .withExposedService(
              CORE,
              FEAST_CORE_PORT,
              Wait.forLogMessage(".*gRPC Server started.*\\n", 1)
                  .withStartupTimeout(Duration.ofMinutes(SERVICE_START_MAX_WAIT_TIME_IN_MINUTES)))
          .withExposedService(CASSANDRA, CASSANDRA_PORT);

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) {
    registry.add("grpc.server.port", () -> FEAST_SERVING_PORT);
  }

  @BeforeAll
  static void globalSetup() throws IOException {
    coreClient = TestUtils.getApiClientForCore(FEAST_CORE_PORT);
    servingStub = TestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);

    cqlSession =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(
                    environment.getServiceHost("cassandra_1", CASSANDRA_PORT),
                    environment.getServicePort("cassandra_1", CASSANDRA_PORT)))
            .withLocalDatacenter(CASSANDRA_DATACENTER)
            .build();

    /** Feast resource creation Workflow */
    String projectName = "default";
    // Apply Entity (driver_id)
    String driverEntityName = "driver_id";
    String driverEntityDescription = "My driver id";
    ValueProto.ValueType.Enum driverEntityType = ValueProto.ValueType.Enum.INT64;
    EntityProto.EntitySpecV2 driverEntitySpec =
        EntityProto.EntitySpecV2.newBuilder()
            .setName(driverEntityName)
            .setDescription(driverEntityDescription)
            .setValueType(driverEntityType)
            .build();
    TestUtils.applyEntity(coreClient, projectName, driverEntitySpec);

    // Apply Entity (merchant_id)
    String merchantEntityName = "merchant_id";
    String merchantEntityDescription = "My driver id";
    ValueProto.ValueType.Enum merchantEntityType = ValueProto.ValueType.Enum.INT64;
    EntityProto.EntitySpecV2 merchantEntitySpec =
        EntityProto.EntitySpecV2.newBuilder()
            .setName(merchantEntityName)
            .setDescription(merchantEntityDescription)
            .setValueType(merchantEntityType)
            .build();
    TestUtils.applyEntity(coreClient, projectName, merchantEntitySpec);

    // Apply FeatureTable (rides)
    String ridesFeatureTableName = "rides";
    ImmutableList<String> ridesEntities = ImmutableList.of(driverEntityName);
    ImmutableMap<String, ValueProto.ValueType.Enum> ridesFeatures =
        ImmutableMap.of(
            "trip_cost",
            ValueProto.ValueType.Enum.INT32,
            "trip_distance",
            ValueProto.ValueType.Enum.DOUBLE,
            "trip_empty",
            ValueProto.ValueType.Enum.DOUBLE,
            "trip_wrong_type",
            ValueProto.ValueType.Enum.STRING);
    TestUtils.applyFeatureTable(
        coreClient, projectName, ridesFeatureTableName, ridesEntities, ridesFeatures, 7200);

    // Apply FeatureTable (food)
    String foodFeatureTableName = "food";
    ImmutableList<String> foodEntities = ImmutableList.of(driverEntityName);
    ImmutableMap<String, ValueProto.ValueType.Enum> foodFeatures =
        ImmutableMap.of(
            "trip_cost",
            ValueProto.ValueType.Enum.INT32,
            "trip_distance",
            ValueProto.ValueType.Enum.DOUBLE);
    TestUtils.applyFeatureTable(
        coreClient, projectName, foodFeatureTableName, foodEntities, foodFeatures, 7200);

    // Apply FeatureTable (rides_merchant)
    String rideMerchantFeatureTableName = "rides_merchant";
    ImmutableList<String> ridesMerchantEntities =
        ImmutableList.of(driverEntityName, merchantEntityName);
    TestUtils.applyFeatureTable(
        coreClient,
        projectName,
        rideMerchantFeatureTableName,
        ridesMerchantEntities,
        ridesFeatures,
        7200);

    /** Create Cassandra Tables Workflow */
    String cassandraTableName = String.format("%s__%s", projectName, driverEntityName);
    String compoundCassandraTableName =
        String.format("%s__%s", projectName, String.join("__", ridesMerchantEntities));

    cqlSession.execute(String.format("DROP KEYSPACE IF EXISTS %s", CASSANDRA_KEYSPACE));
    cqlSession.execute(
        String.format(
            "CREATE KEYSPACE %s WITH replication = \n"
                + "{'class':'SimpleStrategy','replication_factor':'1'};",
            CASSANDRA_KEYSPACE));

    // Create Cassandra Tables
    createCassandraTable(cassandraTableName);
    createCassandraTable(compoundCassandraTableName);

    // Add column families
    addCassandraTableColumn(cassandraTableName, ridesFeatureTableName);
    addCassandraTableColumn(cassandraTableName, foodFeatureTableName);
    addCassandraTableColumn(compoundCassandraTableName, rideMerchantFeatureTableName);

    /** Single Entity Ingestion Workflow */
    Schema ftSchema =
        SchemaBuilder.record("DriverData")
            .namespace(ridesFeatureTableName)
            .fields()
            .requiredInt(feature1Reference.getName())
            .requiredDouble(feature2Reference.getName())
            .nullableString(feature3Reference.getName(), "null")
            .requiredString(feature4Reference.getName())
            .endRecord();
    byte[] schemaReference =
        Hashing.murmur3_32().hashBytes(ftSchema.toString().getBytes()).asBytes();
    byte[] schemaKey = createSchemaKey(schemaReference);

    ingestBulk(ridesFeatureTableName, cassandraTableName, ftSchema, 20);

    Schema foodFtSchema =
        SchemaBuilder.record("FoodDriverData")
            .namespace(foodFeatureTableName)
            .fields()
            .requiredInt(feature1Reference.getName())
            .requiredDouble(feature2Reference.getName())
            .nullableString(feature3Reference.getName(), "null")
            .requiredString(feature4Reference.getName())
            .endRecord();
    byte[] foodSchemaReference =
        Hashing.murmur3_32().hashBytes(foodFtSchema.toString().getBytes()).asBytes();
    byte[] foodSchemaKey = createSchemaKey(foodSchemaReference);

    ingestBulk(foodFeatureTableName, cassandraTableName, foodFtSchema, 20);

    /** Compound Entity Ingestion Workflow */
    Schema compoundFtSchema =
        SchemaBuilder.record("DriverMerchantData")
            .namespace(rideMerchantFeatureTableName)
            .fields()
            .requiredLong(feature1Reference.getName())
            .requiredDouble(feature2Reference.getName())
            .nullableString(feature3Reference.getName(), "null")
            .requiredString(feature4Reference.getName())
            .endRecord();
    byte[] compoundSchemaReference =
        Hashing.murmur3_32().hashBytes(compoundFtSchema.toString().getBytes()).asBytes();

    GenericRecord compoundEntityRecord =
        new GenericRecordBuilder(compoundFtSchema)
            .set("trip_cost", 10L)
            .set("trip_distance", 5.5)
            .set("trip_empty", null)
            .set("trip_wrong_type", "wrong_type")
            .build();
    ValueProto.Value driverEntityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();
    ValueProto.Value merchantEntityValue = ValueProto.Value.newBuilder().setInt64Val(1234).build();
    ImmutableMap<String, ValueProto.Value> compoundEntityMap =
        ImmutableMap.of(
            driverEntityName, driverEntityValue, merchantEntityName, merchantEntityValue);
    GetOnlineFeaturesRequestV2.EntityRow entityRow =
        DataGenerator.createCompoundEntityRow(compoundEntityMap, 100);
    byte[] compoundEntityFeatureKey =
        ridesMerchantEntities.stream()
            .map(entity -> DataGenerator.valueToString(entityRow.getFieldsMap().get(entity)))
            .collect(Collectors.joining("#"))
            .getBytes();
    byte[] compoundEntityFeatureValue = createEntityValue(compoundFtSchema, compoundEntityRecord);
    byte[] compoundSchemaKey = createSchemaKey(compoundSchemaReference);

    ingestData(
        rideMerchantFeatureTableName,
        compoundCassandraTableName,
        compoundEntityFeatureKey,
        compoundEntityFeatureValue,
        compoundSchemaKey);

    /** Schema Ingestion Workflow */
    cqlSession.execute(
        String.format(
            "CREATE TABLE %s.%s (schema_ref BLOB PRIMARY KEY, avro_schema BLOB);",
            CASSANDRA_KEYSPACE, CASSANDRA_SCHEMA_TABLE));

    ingestSchema(schemaKey, ftSchema);
    ingestSchema(foodSchemaKey, foodFtSchema);
    ingestSchema(compoundSchemaKey, compoundFtSchema);

    // set up options for call credentials
    options.put("oauth_url", TOKEN_URL);
    options.put(CLIENT_ID, CLIENT_ID);
    options.put(CLIENT_SECRET, CLIENT_SECRET);
    options.put("jwkEndpointURI", JWK_URI);
    options.put("audience", AUDIENCE);
    options.put("grant_type", GRANT_TYPE);
  }

  private static byte[] createSchemaKey(byte[] schemaReference) throws IOException {
    ByteArrayOutputStream concatOutputStream = new ByteArrayOutputStream();
    concatOutputStream.write(schemaReference);
    byte[] schemaKey = concatOutputStream.toByteArray();

    return schemaKey;
  }

  private static byte[] createEntityValue(Schema schema, GenericRecord record) throws IOException {
    // Entity-Feature Row
    byte[] avroSerializedFeatures = recordToAvro(record, schema);

    ByteArrayOutputStream concatOutputStream = new ByteArrayOutputStream();
    concatOutputStream.write(avroSerializedFeatures);
    byte[] entityFeatureValue = concatOutputStream.toByteArray();

    return entityFeatureValue;
  }

  private static void createCassandraTable(String cassandraTableName) {
    cqlSession.execute(
        String.format(
            "CREATE TABLE %s.%s (key BLOB PRIMARY KEY);", CASSANDRA_KEYSPACE, cassandraTableName));
  }

  private static void addCassandraTableColumn(String cassandraTableName, String featureTableName) {
    cqlSession.execute(
        String.format(
            "ALTER TABLE %s.%s ADD (%s BLOB, %s__schema_ref BLOB);",
            CASSANDRA_KEYSPACE, cassandraTableName, featureTableName, featureTableName));
  }

  private static void ingestData(
      String featureTableName,
      String cassandraTableName,
      byte[] entityFeatureKey,
      byte[] entityFeatureValue,
      byte[] schemaKey) {
    PreparedStatement statement =
        cqlSession.prepare(
            String.format(
                "INSERT INTO %s.%s (%s, %s__schema_ref, %s) VALUES (?, ?, ?)",
                CASSANDRA_KEYSPACE,
                cassandraTableName,
                CASSANDRA_ENTITY_KEY,
                featureTableName,
                featureTableName));

    cqlSession.execute(
        statement.bind(
            ByteBuffer.wrap(entityFeatureKey),
            ByteBuffer.wrap(schemaKey),
            ByteBuffer.wrap(entityFeatureValue)));
  }

  private static void ingestBulk(
      String featureTableName, String cassandraTableName, Schema schema, Integer counts) {

    IntStream.range(0, counts)
        .forEach(
            i -> {
              try {
                GenericRecord record =
                    new GenericRecordBuilder(schema)
                        .set("trip_cost", i)
                        .set("trip_distance", (double) i)
                        .set("trip_empty", null)
                        .set("trip_wrong_type", "test")
                        .build();
                byte[] schemaReference =
                    Hashing.murmur3_32().hashBytes(schema.toString().getBytes()).asBytes();

                byte[] entityFeatureKey =
                    String.valueOf(DataGenerator.createInt64Value(i).getInt64Val()).getBytes();
                byte[] entityFeatureValue = createEntityValue(schema, record);

                byte[] schemaKey = createSchemaKey(schemaReference);
                ingestData(
                    featureTableName,
                    cassandraTableName,
                    entityFeatureKey,
                    entityFeatureValue,
                    schemaKey);
              } catch (IOException e) {
                e.printStackTrace();
              }
            });
  }

  private static void ingestSchema(byte[] schemaKey, Schema schema) {
    PreparedStatement schemaStatement =
        cqlSession.prepare(
            String.format(
                "INSERT INTO %s.%s (schema_ref, avro_schema) VALUES (?, ?);",
                CASSANDRA_KEYSPACE, CASSANDRA_SCHEMA_TABLE));
    cqlSession.execute(
        schemaStatement.bind(
            ByteBuffer.wrap(schemaKey), ByteBuffer.wrap(schema.toString().getBytes())));
  }

  private static byte[] recordToAvro(GenericRecord datum, Schema schema) throws IOException {
    GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
    writer.write(datum, encoder);
    encoder.flush();

    return output.toByteArray();
  }

  @AfterAll
  static void tearDown() {
    ((ManagedChannel) servingStub.getChannel()).shutdown();
  }

  @Test
  public void shouldRegisterSingleEntityAndGetOnlineFeatures() {
    String projectName = "default";
    String entityName = "driver_id";
    ValueProto.Value entityValue = DataGenerator.createInt64Value(1);

    // Instantiate EntityRows
    GetOnlineFeaturesRequestV2.EntityRow entityRow =
        DataGenerator.createEntityRow(entityName, entityValue, 100);
    ImmutableList<GetOnlineFeaturesRequestV2.EntityRow> entityRows = ImmutableList.of(entityRow);

    // Instantiate FeatureReferences
    FeatureReferenceV2 featureReference =
        DataGenerator.createFeatureReference("rides", "trip_cost");
    FeatureReferenceV2 notFoundFeatureReference =
        DataGenerator.createFeatureReference("rides", "trip_transaction");

    ImmutableList<FeatureReferenceV2> featureReferences =
        ImmutableList.of(featureReference, notFoundFeatureReference);

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    ImmutableMap<String, ValueProto.Value> expectedValueMap =
        ImmutableMap.of(
            entityName,
            entityValue,
            FeatureV2.getFeatureStringRef(featureReference),
            DataGenerator.createInt32Value(1),
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            DataGenerator.createEmptyValue());

    ImmutableMap<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        ImmutableMap.of(
            entityName,
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(featureReference),
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND);

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    ImmutableList<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        ImmutableList.of(expectedFieldValues);

    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());
  }

  @Test
  public void shouldRegisterCompoundEntityAndGetOnlineFeatures() {
    String projectName = "default";
    String driverEntityName = "driver_id";
    String merchantEntityName = "merchant_id";
    ValueProto.Value driverEntityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();
    ValueProto.Value merchantEntityValue = ValueProto.Value.newBuilder().setInt64Val(1234).build();

    ImmutableMap<String, ValueProto.Value> compoundEntityMap =
        ImmutableMap.of(
            driverEntityName, driverEntityValue, merchantEntityName, merchantEntityValue);

    // Instantiate EntityRows
    GetOnlineFeaturesRequestV2.EntityRow entityRow =
        DataGenerator.createCompoundEntityRow(compoundEntityMap, 100);
    ImmutableList<GetOnlineFeaturesRequestV2.EntityRow> entityRows = ImmutableList.of(entityRow);

    // Instantiate FeatureReferences
    FeatureReferenceV2 featureReference =
        DataGenerator.createFeatureReference("rides", "trip_cost");
    FeatureReferenceV2 notFoundFeatureReference =
        DataGenerator.createFeatureReference("rides", "trip_transaction");

    ImmutableList<FeatureReferenceV2> featureReferences =
        ImmutableList.of(featureReference, notFoundFeatureReference);

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    ImmutableMap<String, ValueProto.Value> expectedValueMap =
        ImmutableMap.of(
            driverEntityName,
            driverEntityValue,
            merchantEntityName,
            merchantEntityValue,
            FeatureV2.getFeatureStringRef(featureReference),
            DataGenerator.createInt32Value(1),
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            DataGenerator.createEmptyValue());

    ImmutableMap<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        ImmutableMap.of(
            driverEntityName,
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            merchantEntityName,
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(featureReference),
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND);

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    ImmutableList<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        ImmutableList.of(expectedFieldValues);

    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());
  }

  @Test
  public void shouldReturnCorrectRowCountAndOrder() {
    // getOnlineFeatures Information
    String projectName = "default";
    String entityName = "driver_id";
    ValueProto.Value entityValue1 = ValueProto.Value.newBuilder().setInt64Val(1).build();
    ValueProto.Value entityValue2 = ValueProto.Value.newBuilder().setInt64Val(2).build();
    ValueProto.Value entityValue3 = ValueProto.Value.newBuilder().setInt64Val(3).build();
    ValueProto.Value entityValue4 = ValueProto.Value.newBuilder().setInt64Val(4).build();

    // Instantiate EntityRows
    GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        DataGenerator.createEntityRow(entityName, entityValue1, 100);
    GetOnlineFeaturesRequestV2.EntityRow entityRow2 =
        DataGenerator.createEntityRow(entityName, entityValue2, 100);
    GetOnlineFeaturesRequestV2.EntityRow entityRow3 =
        DataGenerator.createEntityRow(entityName, entityValue3, 100);
    GetOnlineFeaturesRequestV2.EntityRow entityRow4 =
        DataGenerator.createEntityRow(entityName, entityValue4, 100);
    ImmutableList<GetOnlineFeaturesRequestV2.EntityRow> entityRows =
        ImmutableList.of(entityRow1, entityRow2, entityRow4, entityRow3);

    // Instantiate FeatureReferences
    FeatureReferenceV2 featureReference =
        DataGenerator.createFeatureReference("rides", "trip_cost");
    FeatureReferenceV2 notFoundFeatureReference =
        DataGenerator.createFeatureReference("rides", "trip_transaction");
    FeatureReferenceV2 emptyFeatureReference =
        DataGenerator.createFeatureReference("rides", "trip_empty");

    ImmutableList<FeatureReferenceV2> featureReferences =
        ImmutableList.of(featureReference, notFoundFeatureReference, emptyFeatureReference);

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    ImmutableMap<String, ValueProto.Value> expectedValueMap =
        ImmutableMap.of(
            entityName,
            entityValue1,
            FeatureV2.getFeatureStringRef(featureReference),
            DataGenerator.createInt32Value(1),
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            DataGenerator.createEmptyValue(),
            FeatureV2.getFeatureStringRef(emptyFeatureReference),
            DataGenerator.createEmptyValue());

    ImmutableMap<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        ImmutableMap.of(
            entityName,
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(featureReference),
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND,
            FeatureV2.getFeatureStringRef(emptyFeatureReference),
            GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE);

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();

    ImmutableMap<String, ValueProto.Value> expectedValueMap2 =
        ImmutableMap.of(
            entityName,
            entityValue2,
            FeatureV2.getFeatureStringRef(featureReference),
            DataGenerator.createInt32Value(2),
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            DataGenerator.createEmptyValue(),
            FeatureV2.getFeatureStringRef(emptyFeatureReference),
            DataGenerator.createEmptyValue());

    ImmutableMap<String, ValueProto.Value> expectedValueMap3 =
        ImmutableMap.of(
            entityName,
            entityValue3,
            FeatureV2.getFeatureStringRef(featureReference),
            DataGenerator.createInt32Value(3),
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            DataGenerator.createEmptyValue(),
            FeatureV2.getFeatureStringRef(emptyFeatureReference),
            DataGenerator.createEmptyValue());

    ImmutableMap<String, ValueProto.Value> expectedValueMap4 =
        ImmutableMap.of(
            entityName,
            entityValue4,
            FeatureV2.getFeatureStringRef(featureReference),
            DataGenerator.createInt32Value(4),
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            DataGenerator.createEmptyValue(),
            FeatureV2.getFeatureStringRef(emptyFeatureReference),
            DataGenerator.createEmptyValue());

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues2 =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap2)
            .putAllStatuses(expectedStatusMap)
            .build();
    GetOnlineFeaturesResponse.FieldValues expectedFieldValues3 =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap3)
            .putAllStatuses(expectedStatusMap)
            .build();
    GetOnlineFeaturesResponse.FieldValues expectedFieldValues4 =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap4)
            .putAllStatuses(expectedStatusMap)
            .build();
    ImmutableList<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        ImmutableList.of(
            expectedFieldValues, expectedFieldValues2, expectedFieldValues4, expectedFieldValues3);

    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());
  }

  @Test
  public void shouldReturnFeaturesFromDiffFeatureTable() {
    String projectName = "default";
    String entityName = "driver_id";
    ValueProto.Value entityValue = DataGenerator.createInt64Value(1);

    // Instantiate EntityRows
    GetOnlineFeaturesRequestV2.EntityRow entityRow =
        DataGenerator.createEntityRow(entityName, entityValue, 100);
    ImmutableList<GetOnlineFeaturesRequestV2.EntityRow> entityRows = ImmutableList.of(entityRow);

    // Instantiate FeatureReferences
    FeatureReferenceV2 rideFeatureReference =
        DataGenerator.createFeatureReference("rides", "trip_cost");
    FeatureReferenceV2 rideFeatureReference2 =
        DataGenerator.createFeatureReference("rides", "trip_distance");
    FeatureReferenceV2 foodFeatureReference =
        DataGenerator.createFeatureReference("food", "trip_cost");
    FeatureReferenceV2 foodFeatureReference2 =
        DataGenerator.createFeatureReference("food", "trip_distance");

    ImmutableList<FeatureReferenceV2> featureReferences =
        ImmutableList.of(
            rideFeatureReference,
            rideFeatureReference2,
            foodFeatureReference,
            foodFeatureReference2);

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    ImmutableMap<String, ValueProto.Value> expectedValueMap =
        ImmutableMap.of(
            entityName,
            entityValue,
            FeatureV2.getFeatureStringRef(rideFeatureReference),
            DataGenerator.createInt32Value(1),
            FeatureV2.getFeatureStringRef(rideFeatureReference2),
            DataGenerator.createDoubleValue(1.0),
            FeatureV2.getFeatureStringRef(foodFeatureReference),
            DataGenerator.createInt32Value(1),
            FeatureV2.getFeatureStringRef(foodFeatureReference2),
            DataGenerator.createDoubleValue(1.0));

    ImmutableMap<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        ImmutableMap.of(
            entityName,
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(rideFeatureReference),
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(rideFeatureReference2),
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(foodFeatureReference),
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(foodFeatureReference2),
            GetOnlineFeaturesResponse.FieldStatus.PRESENT);

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    ImmutableList<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        ImmutableList.of(expectedFieldValues);

    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());
  }
}
