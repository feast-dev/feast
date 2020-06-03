/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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

import com.google.protobuf.Duration;
import feast.proto.serving.ServingAPIProto.*;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.Field;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.Record;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.ValueProto.Value;
import feast.serving.specs.CachedSpecService;
import feast.serving.util.Metrics;
import feast.serving.util.RefUtil;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.OnlineRetriever;
import io.grpc.Status;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public class OnlineServingService implements ServingService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(OnlineServingService.class);
  private final CachedSpecService specService;
  private final Tracer tracer;
  private final OnlineRetriever retriever;

  public OnlineServingService(
      OnlineRetriever retriever, CachedSpecService specService, Tracer tracer) {
    this.retriever = retriever;
    this.specService = specService;
    this.tracer = tracer;
  }

  /** {@inheritDoc} */
  @Override
  public GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest) {
    return GetFeastServingInfoResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_ONLINE)
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequest request) {
    try (Scope scope = tracer.buildSpan("getOnlineFeatures").startActive(true)) {
      List<EntityRow> entityRows = request.getEntityRowsList();
      boolean includeMetadata = request.getIncludeMetadataInResponse();
      // Collect the response fields for each entity row in entityFieldsMap.
      Map<EntityRow, Map<String, Field>> entityFieldsMap =
          entityRows.stream().collect(Collectors.toMap(row -> row, row -> new HashMap<>()));

      if (!request.getOmitEntitiesInResponse()) {
        // Add entity row's fields as response fields
        entityRows.forEach(
            entityRow -> {
              entityFieldsMap.get(entityRow).putAll(this.unpackFields(entityRow, includeMetadata));
            });
      }

      List<FeatureSetRequest> featureSetRequests =
          specService.getFeatureSets(request.getFeaturesList());
      for (FeatureSetRequest featureSetRequest : featureSetRequests) {
        // Pull feature rows for given entity rows from the feature/featureset specified in feature
        // set request.
        List<FeatureRow> featureRows = retriever.getOnlineFeatures(entityRows, featureSetRequest);
        // Check that feature row returned corresponds to a given entity row.
        if (featureRows.size() != entityRows.size()) {
          throw Status.INTERNAL
              .withDescription(
                  "The no. of FeatureRow obtained from OnlineRetriever"
                      + "does not match no. of entityRow passed.")
              .asRuntimeException();
        }

        for (var i = 0; i < entityRows.size(); i++) {
          FeatureRow featureRow = featureRows.get(i);
          EntityRow entityRow = entityRows.get(i);
          // Unpack feature response fields from feature row
          Map<FeatureReference, Field> fields =
              this.unpackFields(featureRow, entityRow, featureSetRequest, includeMetadata);
          // Merge feature response fields into entityFieldsMap
          entityFieldsMap
              .get(entityRow)
              .putAll(
                  fields.entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              es -> RefUtil.generateFeatureStringRef(es.getKey()),
                              es -> es.getValue())));

          // Populate metrics
          this.populateStaleKeyCountMetrics(fields, featureSetRequest);
        }
        this.populateRequestCountMetrics(featureSetRequest);
      }

      // Build response records from entityFieldsMap.
      // Records should be in the same order as the entityRows provided by the user.
      List<Record> records =
          entityRows.stream()
              .map(
                  entityRow ->
                      Record.newBuilder().putAllFields(entityFieldsMap.get(entityRow)).build())
              .collect(Collectors.toList());
      return GetOnlineFeaturesResponse.newBuilder().addAllRecords(records).build();
    }
  }

  /**
   * Unpack response fields from the given entity row's fields.
   *
   * @param entityRow to unpack for response fields
   * @param includeMetadata whether metadata should be included in the response fields
   * @return Map mapping of name of field to response field.
   */
  private Map<String, Field> unpackFields(EntityRow entityRow, boolean includeMetadata) {
    return entityRow.getFieldsMap().entrySet().stream()
        .collect(
            Collectors.toMap(
                es -> es.getKey(),
                es -> {
                  Field.Builder field = Field.newBuilder().setValue(es.getValue());
                  if (includeMetadata) {
                    field.setStatus(FieldStatus.PRESENT);
                  }
                  return field.build();
                }));
  }

  /**
   * Unpack response fields using data from the given feature row for features specified in the
   * given feature set request.
   *
   * @param featureRow to unpack for response fields.
   * @param entityRow for which the feature row was retrieved for.
   * @param featureSetRequest for which the feature row was retrieved.
   * @param includeMetadata whether metadata should be included in the response fields
   * @return Map mapping of feature ref to response field
   */
  private Map<FeatureReference, Field> unpackFields(
      FeatureRow featureRow,
      EntityRow entityRow,
      FeatureSetRequest featureSetRequest,
      boolean includeMetadata) {
    // In order to return values containing the same feature references provided by the user,
    // we reuse the feature references in the request as the keys in field builder map
    Map<String, FeatureReference> refsByName = featureSetRequest.getFeatureRefsByName();
    Map<FeatureReference, Field.Builder> fields = new HashMap<>();

    boolean hasStaleValues = this.isStale(featureSetRequest, entityRow, featureRow);
    if (featureRow != null) {
      // unpack feature row's field's values & populate field map
      Map<FeatureReference, Field.Builder> featureFields =
          featureRow.getFieldsList().stream()
              .filter(featureRowField -> refsByName.containsKey(featureRowField.getName()))
              .collect(
                  Collectors.toMap(
                      featureRowField -> refsByName.get(featureRowField.getName()),
                      featureRowField -> {
                        // omit stale feature values.
                        if (hasStaleValues) {
                          return Field.newBuilder().setValue(Value.newBuilder().build());
                        } else {
                          return Field.newBuilder().setValue(featureRowField.getValue());
                        }
                      }));
      fields.putAll(featureFields);
    }

    // create empty response fields for features specified in request but not present in feature
    // row.
    Set<FeatureReference> missingFeatures = new HashSet<>(refsByName.values());
    missingFeatures.removeAll(fields.keySet());
    missingFeatures.forEach(
        ref -> fields.put(ref, Field.newBuilder().setValue(Value.newBuilder().build())));

    // attach metadata to the feature response fields & build response field
    return fields.entrySet().stream()
        .collect(
            Collectors.toMap(
                es -> es.getKey(),
                es -> {
                  Field.Builder field = es.getValue();
                  if (includeMetadata) {
                    field = this.attachMetadata(field, featureRow, hasStaleValues);
                  }
                  return field.build();
                }));
  }

  /**
   * Attach metadata to the given response field. Attaches field status to the response providing
   * metadata for the field.
   *
   * @param featureRow where the field was unpacked from.
   * @param hasStaleValue whether the field contains a stale value
   */
  private Field.Builder attachMetadata(
      Field.Builder field, FeatureRow featureRow, boolean hasStaleValue) {
    FieldStatus fieldStatus = FieldStatus.PRESENT;
    if (featureRow == null) {
      fieldStatus = FieldStatus.NOT_FOUND;
    } else if (hasStaleValue) {
      fieldStatus = FieldStatus.OUTSIDE_MAX_AGE;
    } else if (field.getValue().getValCase() == Value.ValCase.VAL_NOT_SET) {
      fieldStatus = FieldStatus.NULL_VALUE;
    }

    return field.setStatus(fieldStatus);
  }

  /**
   * Determine if the feature data in the given feature row is considered stale. Data is considered
   * to be stale when difference ingestion time set in feature row and the retrieval time set in
   * entity row exceeds featureset max age.
   *
   * @param featureSetRequest contains the spec where feature's max age is extracted.
   * @param entityRow contains the retrieval timing of when features are pulled.
   * @param featureRow contains the ingestion timing and feature data.
   */
  private boolean isStale(
      FeatureSetRequest featureSetRequest, EntityRow entityRow, FeatureRow featureRow) {
    Duration maxAge = featureSetRequest.getSpec().getMaxAge();
    if (featureRow == null) {
      // no data to consider: not stale
      return false;
    }
    if (maxAge.equals(Duration.getDefaultInstance())) {
      // max age is not set: stale detection disabled
      return false;
    }

    long givenTimestamp = entityRow.getEntityTimestamp().getSeconds();
    if (givenTimestamp == 0) {
      givenTimestamp = System.currentTimeMillis() / 1000;
    }
    long timeDifference = givenTimestamp - featureRow.getEventTimestamp().getSeconds();
    return timeDifference > maxAge.getSeconds();
  }

  private void populateStaleKeyCountMetrics(
      Map<FeatureReference, Field> fields, FeatureSetRequest featureSetRequest) {
    String project = featureSetRequest.getSpec().getProject();
    fields
        .entrySet()
        .forEach(
            es -> {
              FeatureReference ref = es.getKey();
              Field field = es.getValue();
              if (field.getStatus() == FieldStatus.OUTSIDE_MAX_AGE) {
                Metrics.staleKeyCount.labels(project, RefUtil.generateFeatureStringRef(ref)).inc();
              }
            });
  }

  private void populateRequestCountMetrics(FeatureSetRequest featureSetRequest) {
    String project = featureSetRequest.getSpec().getProject();
    featureSetRequest
        .getFeatureReferences()
        .parallelStream()
        .forEach(ref -> Metrics.requestCount.labels(project, ref.getName()).inc());
  }

  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetBatchFeaturesRequest getFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  @Override
  public GetJobResponse getJob(GetJobRequest getJobRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }
}
