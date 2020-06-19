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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto.Value;
import feast.serving.specs.CachedSpecService;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.connectors.redis.retriever.RedisOnlineRetriever;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

public class OnlineServingServiceTest {

  @Mock CachedSpecService specService;

  @Mock Tracer tracer;

  @Mock RedisOnlineRetriever retriever;

  private OnlineServingService onlineServingService;

  @Before
  public void setUp() {
    initMocks(this);
    onlineServingService = new OnlineServingService(retriever, specService, tracer);
  }

  @Test
  public void shouldReturnResponseWithValuesAndMetadataIfKeysPresent() {
    GetOnlineFeaturesRequest request =
        GetOnlineFeaturesRequest.newBuilder()
            .setOmitEntitiesInResponse(false)
            .addFeatures(FeatureReference.newBuilder().setName("feature1").build())
            .addFeatures(
                FeatureReference.newBuilder().setName("feature2").setProject("project").build())
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a")))
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(2))
                    .putFields("entity2", strValue("b")))
            .build();

    List<Optional<FeatureRow>> featureRows =
        Lists.newArrayList(
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .addAllFields(
                        Lists.newArrayList(
                            FieldProto.Field.newBuilder()
                                .setName("entity1")
                                .setValue(intValue(1))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("entity2")
                                .setValue(strValue("a"))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature1")
                                .setValue(intValue(1))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature2")
                                .setValue(intValue(1))
                                .build()))
                    .setFeatureSet("featureSet")
                    .build()),
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .addAllFields(
                        Lists.newArrayList(
                            FieldProto.Field.newBuilder()
                                .setName("entity1")
                                .setValue(intValue(2))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("entity2")
                                .setValue(strValue("b"))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature1")
                                .setValue(intValue(2))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature2")
                                .setValue(intValue(2))
                                .build()))
                    .setFeatureSet("featureSet")
                    .build()));

    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .addAllFeatureReferences(request.getFeaturesList())
            .setSpec(getFeatureSetSpec())
            .build();

    when(specService.getFeatureSets(request.getFeaturesList()))
        .thenReturn(Collections.singletonList(featureSetRequest));
    when(retriever.getOnlineFeatures(request.getEntityRowsList(), featureSetRequest))
        .thenReturn(featureRows);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(1))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", strValue("a"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("feature1", intValue(1))
                    .putStatuses("feature1", FieldStatus.PRESENT)
                    .putFields("project/feature2", intValue(1))
                    .putStatuses("project/feature2", FieldStatus.PRESENT)
                    .build())
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(2))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", strValue("b"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("feature1", intValue(2))
                    .putStatuses("feature1", FieldStatus.PRESENT)
                    .putFields("project/feature2", intValue(2))
                    .putStatuses("project/feature2", FieldStatus.PRESENT)
                    .build())
            .build();
    GetOnlineFeaturesResponse actual = onlineServingService.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesAndMetadataIfKeysNotPresent() {
    // some keys not present, should have empty values
    GetOnlineFeaturesRequest request =
        GetOnlineFeaturesRequest.newBuilder()
            .addFeatures(FeatureReference.newBuilder().setName("feature1").build())
            .addFeatures(
                FeatureReference.newBuilder().setName("feature2").setProject("project").build())
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a")))
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(2))
                    .putFields("entity2", strValue("b")))
            .build();

    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .addAllFeatureReferences(request.getFeaturesList())
            .setSpec(getFeatureSetSpec())
            .build();

    List<Optional<FeatureRow>> featureRows =
        Lists.newArrayList(
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .setFeatureSet("project/featureSet")
                    .addAllFields(
                        Lists.newArrayList(
                            FieldProto.Field.newBuilder()
                                .setName("feature1")
                                .setValue(intValue(1))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature2")
                                .setValue(intValue(1))
                                .build()))
                    .build()),
            Optional.empty());

    when(specService.getFeatureSets(request.getFeaturesList()))
        .thenReturn(Collections.singletonList(featureSetRequest));
    when(retriever.getOnlineFeatures(request.getEntityRowsList(), featureSetRequest))
        .thenReturn(featureRows);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(1))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", strValue("a"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("feature1", intValue(1))
                    .putStatuses("feature1", FieldStatus.PRESENT)
                    .putFields("project/feature2", intValue(1))
                    .putStatuses("project/feature2", FieldStatus.PRESENT)
                    .build())
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(2))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", strValue("b"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("feature1", Value.newBuilder().build())
                    .putStatuses("feature1", FieldStatus.NOT_FOUND)
                    .putFields("project/feature2", Value.newBuilder().build())
                    .putStatuses("project/feature2", FieldStatus.NOT_FOUND)
                    .build())
            .build();
    GetOnlineFeaturesResponse actual = onlineServingService.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesAndMetadataIfMaxAgeIsExceeded() {
    // keys present, but considered stale when compared to maxAge
    GetOnlineFeaturesRequest request =
        GetOnlineFeaturesRequest.newBuilder()
            .addFeatures(FeatureReference.newBuilder().setName("feature1").build())
            .addFeatures(
                FeatureReference.newBuilder().setName("feature2").setProject("project").build())
            .setOmitEntitiesInResponse(false)
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a")))
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(2))
                    .putFields("entity2", strValue("b")))
            .build();

    List<Optional<FeatureRow>> featureRows =
        Lists.newArrayList(
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .addAllFields(
                        Lists.newArrayList(
                            FieldProto.Field.newBuilder()
                                .setName("entity1")
                                .setValue(intValue(1))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("entity2")
                                .setValue(strValue("a"))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature1")
                                .setValue(intValue(1))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature2")
                                .setValue(intValue(1))
                                .build()))
                    .setFeatureSet("project/featureSet")
                    .build()),
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(
                        Timestamp.newBuilder().setSeconds(50)) // this value should be nulled
                    .addAllFields(
                        Lists.newArrayList(
                            FieldProto.Field.newBuilder()
                                .setName("entity1")
                                .setValue(intValue(2))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("entity2")
                                .setValue(strValue("b"))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature1")
                                .setValue(intValue(2))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("project/feature2")
                                .setValue(intValue(2))
                                .build()))
                    .setFeatureSet("project/featureSet")
                    .build()));

    FeatureSetSpec spec =
        getFeatureSetSpec().toBuilder().setMaxAge(Duration.newBuilder().setSeconds(1)).build();
    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .addAllFeatureReferences(request.getFeaturesList())
            .setSpec(spec)
            .build();

    when(specService.getFeatureSets(request.getFeaturesList()))
        .thenReturn(Collections.singletonList(featureSetRequest));
    when(retriever.getOnlineFeatures(request.getEntityRowsList(), featureSetRequest))
        .thenReturn(featureRows);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(1))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", strValue("a"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("feature1", intValue(1))
                    .putStatuses("feature1", FieldStatus.PRESENT)
                    .putFields("project/feature2", intValue(1))
                    .putStatuses("project/feature2", FieldStatus.PRESENT)
                    .build())
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(2))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", strValue("b"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("feature1", Value.newBuilder().build())
                    .putStatuses("feature1", FieldStatus.OUTSIDE_MAX_AGE)
                    .putFields("project/feature2", Value.newBuilder().build())
                    .putStatuses("project/feature2", FieldStatus.OUTSIDE_MAX_AGE)
                    .build())
            .build();
    GetOnlineFeaturesResponse actual = onlineServingService.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldFilterOutUndesiredRows() {
    // requested rows less than the rows available in the featureset
    GetOnlineFeaturesRequest request =
        GetOnlineFeaturesRequest.newBuilder()
            .setOmitEntitiesInResponse(false)
            .addFeatures(FeatureReference.newBuilder().setName("feature1").build())
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a")))
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", intValue(2))
                    .putFields("entity2", strValue("b")))
            .build();

    List<Optional<FeatureRow>> featureRows =
        Lists.newArrayList(
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .addAllFields(
                        Lists.newArrayList(
                            FieldProto.Field.newBuilder()
                                .setName("entity1")
                                .setValue(intValue(1))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("entity2")
                                .setValue(strValue("a"))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature1")
                                .setValue(intValue(1))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature2")
                                .setValue(intValue(1))
                                .build()))
                    .setFeatureSet("featureSet")
                    .build()),
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .addAllFields(
                        Lists.newArrayList(
                            FieldProto.Field.newBuilder()
                                .setName("entity1")
                                .setValue(intValue(2))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("entity2")
                                .setValue(strValue("b"))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature1")
                                .setValue(intValue(2))
                                .build(),
                            FieldProto.Field.newBuilder()
                                .setName("feature2")
                                .setValue(intValue(2))
                                .build()))
                    .setFeatureSet("featureSet")
                    .build()));

    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .addAllFeatureReferences(request.getFeaturesList())
            .setSpec(getFeatureSetSpec())
            .build();

    when(specService.getFeatureSets(request.getFeaturesList()))
        .thenReturn(Collections.singletonList(featureSetRequest));
    when(retriever.getOnlineFeatures(request.getEntityRowsList(), featureSetRequest))
        .thenReturn(featureRows);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(1))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", strValue("a"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("feature1", intValue(1))
                    .putStatuses("feature1", FieldStatus.PRESENT)
                    .build())
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(2))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", strValue("b"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("feature1", intValue(2))
                    .putStatuses("feature1", FieldStatus.PRESENT)
                    .build())
            .build();
    GetOnlineFeaturesResponse actual = onlineServingService.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  private Value intValue(int val) {
    return Value.newBuilder().setInt32Val(val).build();
  }

  private Value strValue(String val) {
    return Value.newBuilder().setStringVal(val).build();
  }

  private FeatureSetSpec getFeatureSetSpec() {
    return FeatureSetSpec.newBuilder()
        .setName("featureSet")
        .addEntities(EntitySpec.newBuilder().setName("entity1"))
        .addEntities(EntitySpec.newBuilder().setName("entity2"))
        .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
        .build();
  }
}
