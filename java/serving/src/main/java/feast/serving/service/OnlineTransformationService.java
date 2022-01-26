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
package feast.serving.service;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import feast.proto.core.*;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.TransformationServiceAPIProto.TransformFeaturesRequest;
import feast.proto.serving.TransformationServiceAPIProto.TransformFeaturesResponse;
import feast.proto.serving.TransformationServiceAPIProto.ValueType;
import feast.proto.serving.TransformationServiceGrpc;
import feast.proto.types.ValueProto;
import feast.serving.registry.RegistryRepository;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

public class OnlineTransformationService implements TransformationService {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(OnlineTransformationService.class);
  private final TransformationServiceGrpc.TransformationServiceBlockingStub stub;
  private final RegistryRepository registryRepository;
  static final int INT64_BITWIDTH = 64;
  static final int INT32_BITWIDTH = 32;

  public OnlineTransformationService(
      String transformationServiceEndpoint, RegistryRepository registryRepository) {
    if (transformationServiceEndpoint != null) {
      final ManagedChannel channel =
          ManagedChannelBuilder.forTarget(transformationServiceEndpoint).usePlaintext().build();
      this.stub = TransformationServiceGrpc.newBlockingStub(channel);
    } else {
      this.stub = null;
    }
    this.registryRepository = registryRepository;
  }

  /** {@inheritDoc} */
  @Override
  public TransformFeaturesResponse transformFeatures(
      TransformFeaturesRequest transformFeaturesRequest) {
    if (this.stub == null) {
      throw new RuntimeException(
          "Transformation service endpoint must be configured to enable this functionality.");
    }
    return this.stub.transformFeatures(transformFeaturesRequest);
  }

  /** {@inheritDoc} */
  @Override
  public List<ServingAPIProto.FeatureReferenceV2> extractOnDemandFeaturesDependencies(
      List<ServingAPIProto.FeatureReferenceV2> onDemandFeatureReferences) {
    List<ServingAPIProto.FeatureReferenceV2> onDemandFeatureInputs = new ArrayList<>();
    for (ServingAPIProto.FeatureReferenceV2 featureReference : onDemandFeatureReferences) {
      OnDemandFeatureViewProto.OnDemandFeatureViewSpec onDemandFeatureViewSpec =
          this.registryRepository.getOnDemandFeatureViewSpec(featureReference);
      Map<String, OnDemandFeatureViewProto.OnDemandInput> inputs =
          onDemandFeatureViewSpec.getInputsMap();

      for (OnDemandFeatureViewProto.OnDemandInput input : inputs.values()) {
        OnDemandFeatureViewProto.OnDemandInput.InputCase inputCase = input.getInputCase();
        switch (inputCase) {
          case REQUEST_DATA_SOURCE:
            // Do nothing. The value should be provided as dedicated request parameter
            break;
          case FEATURE_VIEW_PROJECTION:
            FeatureReferenceProto.FeatureViewProjection projection =
                input.getFeatureViewProjection();
            for (FeatureProto.FeatureSpecV2 featureSpec : projection.getFeatureColumnsList()) {
              String featureName = featureSpec.getName();
              ServingAPIProto.FeatureReferenceV2 onDemandFeatureInput =
                  ServingAPIProto.FeatureReferenceV2.newBuilder()
                      .setFeatureViewName(projection.getFeatureViewName())
                      .setFeatureName(featureName)
                      .build();
              onDemandFeatureInputs.add(onDemandFeatureInput);
            }
            break;
          case FEATURE_VIEW:
            FeatureViewProto.FeatureView featureView = input.getFeatureView();
            FeatureViewProto.FeatureViewSpec featureViewSpec = featureView.getSpec();
            String featureViewName = featureViewSpec.getName();
            for (FeatureProto.FeatureSpecV2 featureSpec : featureViewSpec.getFeaturesList()) {
              String featureName = featureSpec.getName();
              ServingAPIProto.FeatureReferenceV2 onDemandFeatureInput =
                  ServingAPIProto.FeatureReferenceV2.newBuilder()
                      .setFeatureViewName(featureViewName)
                      .setFeatureName(featureName)
                      .build();
              onDemandFeatureInputs.add(onDemandFeatureInput);
            }
            break;
          default:
            throw Status.INTERNAL
                .withDescription(
                    "OnDemandInput proto input field has an unexpected type: " + inputCase)
                .asRuntimeException();
        }
      }
    }
    return onDemandFeatureInputs;
  }

  /** {@inheritDoc} */
  public void processTransformFeaturesResponse(
      feast.proto.serving.TransformationServiceAPIProto.TransformFeaturesResponse
          transformFeaturesResponse,
      String onDemandFeatureViewName,
      Set<String> onDemandFeatureStringReferences,
      ServingAPIProto.GetOnlineFeaturesResponse.Builder responseBuilder) {
    try {
      BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
      ArrowFileReader reader =
          new ArrowFileReader(
              new ByteArrayReadableSeekableByteChannel(
                  transformFeaturesResponse
                      .getTransformationOutput()
                      .getArrowValue()
                      .toByteArray()),
              allocator);
      reader.loadNextBatch();
      VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
      Schema responseSchema = readBatch.getSchema();
      List<Field> responseFields = responseSchema.getFields();
      Timestamp now = Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build();

      for (Field field : responseFields) {
        String columnName = field.getName();
        String fullFeatureName = columnName.replace("__", ":");
        ArrowType columnType = field.getType();

        // The response will contain all features for the specified ODFV, so we
        // skip the features that were not requested.
        if (!onDemandFeatureStringReferences.contains(fullFeatureName)) {
          continue;
        }

        FieldVector fieldVector = readBatch.getVector(field);
        int valueCount = fieldVector.getValueCount();
        ServingAPIProto.GetOnlineFeaturesResponse.FeatureVector.Builder vectorBuilder =
            responseBuilder.addResultsBuilder();
        List<ValueProto.Value> valueList = Lists.newArrayListWithExpectedSize(valueCount);

        // TODO: support all Feast types
        // TODO: clean up the switch statement
        if (columnType instanceof ArrowType.Int) {
          int bitWidth = ((ArrowType.Int) columnType).getBitWidth();
          switch (bitWidth) {
            case INT64_BITWIDTH:
              for (int i = 0; i < valueCount; i++) {
                long int64Value = ((BigIntVector) fieldVector).get(i);
                valueList.add(ValueProto.Value.newBuilder().setInt64Val(int64Value).build());
              }
              break;
            case INT32_BITWIDTH:
              for (int i = 0; i < valueCount; i++) {
                int int32Value = ((IntVector) fieldVector).get(i);
                valueList.add(ValueProto.Value.newBuilder().setInt32Val(int32Value).build());
              }
              break;
            default:
              throw Status.INTERNAL
                  .withDescription(
                      "Column "
                          + columnName
                          + " is of type ArrowType.Int but has bitWidth "
                          + bitWidth
                          + " which cannot be handled.")
                  .asRuntimeException();
          }
        } else if (columnType instanceof ArrowType.FloatingPoint) {
          FloatingPointPrecision precision = ((ArrowType.FloatingPoint) columnType).getPrecision();
          switch (precision) {
            case DOUBLE:
              for (int i = 0; i < valueCount; i++) {
                double doubleValue = ((Float8Vector) fieldVector).get(i);
                valueList.add(ValueProto.Value.newBuilder().setDoubleVal(doubleValue).build());
              }
              break;
            case SINGLE:
              for (int i = 0; i < valueCount; i++) {
                float floatValue = ((Float4Vector) fieldVector).get(i);
                valueList.add(ValueProto.Value.newBuilder().setFloatVal(floatValue).build());
              }
              break;
            default:
              throw Status.INTERNAL
                  .withDescription(
                      "Column "
                          + columnName
                          + " is of type ArrowType.FloatingPoint but has precision "
                          + precision
                          + " which cannot be handled.")
                  .asRuntimeException();
          }
        }

        for (ValueProto.Value v : valueList) {
          vectorBuilder.addValues(v);
          vectorBuilder.addStatuses(ServingAPIProto.FieldStatus.PRESENT);
          vectorBuilder.addEventTimestamps(now);
        }

        responseBuilder.getMetadataBuilder().getFeatureNamesBuilder().addVal(fullFeatureName);
      }
    } catch (IOException e) {
      log.info(e.toString());
      throw Status.INTERNAL
          .withDescription(
              "Unable to correctly process transform features response: " + e.toString())
          .asRuntimeException();
    }
  }

  /** {@inheritDoc} */
  public ValueType serializeValuesIntoArrowIPC(List<Pair<String, List<ValueProto.Value>>> values) {
    // In order to be serialized correctly, the data must be packaged in a VectorSchemaRoot.
    // We first construct all the columns.
    Map<String, FieldVector> columnNameToColumn = new HashMap<String, FieldVector>();
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    List<Field> columnFields = new ArrayList<Field>();
    List<FieldVector> columns = new ArrayList<FieldVector>();

    for (Pair<String, List<ValueProto.Value>> columnEntry : values) {
      // The Python FTS does not expect full feature names, so we extract the feature name.
      String columnName = columnEntry.getKey();

      List<ValueProto.Value> columnValues = columnEntry.getValue();
      FieldVector column;
      ValueProto.Value.ValCase valCase = columnValues.get(0).getValCase();
      // TODO: support all Feast types
      switch (valCase) {
        case INT32_VAL:
          column = new IntVector(columnName, allocator);
          column.setValueCount(columnValues.size());
          for (int idx = 0; idx < columnValues.size(); idx++) {
            ((IntVector) column).set(idx, columnValues.get(idx).getInt32Val());
          }
          break;
        case INT64_VAL:
          column = new BigIntVector(columnName, allocator);
          column.setValueCount(columnValues.size());
          for (int idx = 0; idx < columnValues.size(); idx++) {
            ((BigIntVector) column).set(idx, columnValues.get(idx).getInt64Val());
          }

          break;
        case DOUBLE_VAL:
          column = new Float8Vector(columnName, allocator);
          column.setValueCount(columnValues.size());
          for (int idx = 0; idx < columnValues.size(); idx++) {
            ((Float8Vector) column).set(idx, columnValues.get(idx).getDoubleVal());
          }
          break;
        case FLOAT_VAL:
          column = new Float4Vector(columnName, allocator);
          column.setValueCount(columnValues.size());
          for (int idx = 0; idx < columnValues.size(); idx++) {
            ((Float4Vector) column).set(idx, columnValues.get(idx).getFloatVal());
          }
          break;
        default:
          throw Status.INTERNAL
              .withDescription(
                  "Column " + columnName + " has a type that is currently not handled: " + valCase)
              .asRuntimeException();
      }

      columns.add(column);
      columnFields.add(column.getField());
    }

    VectorSchemaRoot schemaRoot = new VectorSchemaRoot(columnFields, columns);

    // Serialize the VectorSchemaRoot into Arrow IPC format.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ArrowFileWriter writer = new ArrowFileWriter(schemaRoot, null, Channels.newChannel(out));
    try {
      writer.start();
      writer.writeBatch();
      writer.end();
    } catch (IOException e) {
      log.info(e.toString());
      throw Status.INTERNAL
          .withDescription(
              "ArrowFileWriter could not write properly; failed with error: " + e.toString())
          .asRuntimeException();
    }
    byte[] byteData = out.toByteArray();
    ByteString inputData = ByteString.copyFrom(byteData);
    ValueType transformationInput = ValueType.newBuilder().setArrowValue(inputData).build();
    return transformationInput;
  }
}
