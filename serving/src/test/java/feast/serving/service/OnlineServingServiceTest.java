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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.FeatureReference;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.serving.specs.CachedSpecService;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.connectors.redis.retriever.RedisOnlineRetriever;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
  public void shouldReturnResponseWithValuesIfKeysPresent() {
    GetOnlineFeaturesRequest request =
        GetOnlineFeaturesRequest.newBuilder()
            .addFeatures(
                FeatureReference.newBuilder().setName("feature1").setProject("project").build())
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

    List<FeatureRow> featureRows =
        Lists.newArrayList(
            FeatureRow.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                        Field.newBuilder().setName("entity2").setValue(strValue("a")).build(),
                        Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                        Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
                .setFeatureSet("featureSet")
                .build(),
            FeatureRow.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                        Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                        Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                        Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
                .setFeatureSet("featureSet")
                .build());

    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .addAllFeatureReferences(request.getFeaturesList())
            .setSpec(getFeatureSetSpec())
            .build();

    when(specService.getFeatureSets(request.getFeaturesList()))
        .thenReturn(Collections.singletonList(featureSetRequest));
    when(retriever.getOnlineFeatures(
            request.getEntityRowsList(), Collections.singletonList(featureSetRequest)))
        .thenReturn(Collections.singletonList(featureRows));
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a"))
                    .putFields("project/feature1", intValue(1))
                    .putFields("project/feature2", intValue(1)))
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(2))
                    .putFields("entity2", strValue("b"))
                    .putFields("project/feature1", intValue(2))
                    .putFields("project/feature2", intValue(2)))
            .build();
    GetOnlineFeaturesResponse actual = onlineServingService.getOnlineFeatures(request);
    assertThat(
        responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }

  @Test
  public void shouldReturnKeysWithoutVersionIfNotProvided() {
    GetOnlineFeaturesRequest request =
        GetOnlineFeaturesRequest.newBuilder()
            .addFeatures(
                FeatureReference.newBuilder().setName("feature1").setProject("project").build())
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

    List<FeatureRow> featureRows =
        Lists.newArrayList(
            FeatureRow.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                        Field.newBuilder().setName("entity2").setValue(strValue("a")).build(),
                        Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                        Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
                .setFeatureSet("featureSet")
                .build(),
            FeatureRow.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                        Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                        Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                        Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
                .setFeatureSet("featureSet")
                .build());

    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .addAllFeatureReferences(request.getFeaturesList())
            .setSpec(getFeatureSetSpec())
            .build();

    when(specService.getFeatureSets(request.getFeaturesList()))
        .thenReturn(Collections.singletonList(featureSetRequest));
    when(retriever.getOnlineFeatures(
            request.getEntityRowsList(), Collections.singletonList(featureSetRequest)))
        .thenReturn(Collections.singletonList(featureRows));
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a"))
                    .putFields("project/feature1", intValue(1))
                    .putFields("project/feature2", intValue(1)))
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(2))
                    .putFields("entity2", strValue("b"))
                    .putFields("project/feature1", intValue(2))
                    .putFields("project/feature2", intValue(2)))
            .build();
    GetOnlineFeaturesResponse actual = onlineServingService.getOnlineFeatures(request);
    assertThat(
        responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesIfKeysNotPresent() {
    // some keys not present, should have empty values
    GetOnlineFeaturesRequest request =
        GetOnlineFeaturesRequest.newBuilder()
            .addFeatures(
                FeatureReference.newBuilder().setName("feature1").setProject("project").build())
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

    List<FeatureRow> featureRows =
        Lists.newArrayList(
            FeatureRow.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                .setFeatureSet("project/featureSet")
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                        Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("project/featureSet")
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setName("feature1").build(),
                        Field.newBuilder().setName("feature2").build()))
                .build());

    when(specService.getFeatureSets(request.getFeaturesList()))
        .thenReturn(Collections.singletonList(featureSetRequest));
    when(retriever.getOnlineFeatures(
            request.getEntityRowsList(), Collections.singletonList(featureSetRequest)))
        .thenReturn(Collections.singletonList(featureRows));
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a"))
                    .putFields("project/feature1", intValue(1))
                    .putFields("project/feature2", intValue(1)))
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(2))
                    .putFields("entity2", strValue("b"))
                    .putFields("project/feature1", Value.newBuilder().build())
                    .putFields("project/feature2", Value.newBuilder().build()))
            .build();
    GetOnlineFeaturesResponse actual = onlineServingService.getOnlineFeatures(request);
    assertThat(
        responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesIfMaxAgeIsExceeded() {
    // keys present, but too stale comp. to maxAge
    GetOnlineFeaturesRequest request =
        GetOnlineFeaturesRequest.newBuilder()
            .addFeatures(
                FeatureReference.newBuilder().setName("feature1").setProject("project").build())
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

    List<FeatureRow> featureRows =
        Lists.newArrayList(
            FeatureRow.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                        Field.newBuilder().setName("entity2").setValue(strValue("a")).build(),
                        Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                        Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
                .setFeatureSet("featureSet")
                .build(),
            FeatureRow.newBuilder()
                .setEventTimestamp(
                    Timestamp.newBuilder().setSeconds(50)) // this value should be nulled
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                        Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                        Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                        Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
                .setFeatureSet("featureSet")
                .build());

    FeatureSetSpec spec =
        getFeatureSetSpec().toBuilder().setMaxAge(Duration.newBuilder().setSeconds(1)).build();
    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .addAllFeatureReferences(request.getFeaturesList())
            .setSpec(spec)
            .build();

    when(specService.getFeatureSets(request.getFeaturesList()))
        .thenReturn(Collections.singletonList(featureSetRequest));
    when(retriever.getOnlineFeatures(
            request.getEntityRowsList(), Collections.singletonList(featureSetRequest)))
        .thenReturn(Collections.singletonList(featureRows));
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a"))
                    .putFields("project/feature1", intValue(1))
                    .putFields("project/feature2", intValue(1)))
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(2))
                    .putFields("entity2", strValue("b"))
                    .putFields("project/feature1", Value.newBuilder().build())
                    .putFields("project/feature2", Value.newBuilder().build()))
            .build();
    GetOnlineFeaturesResponse actual = onlineServingService.getOnlineFeatures(request);
    assertThat(
        responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }

  @Test
  public void shouldFilterOutUndesiredRows() {
    // requested rows less than the rows available in the featureset
    GetOnlineFeaturesRequest request =
        GetOnlineFeaturesRequest.newBuilder()
            .addFeatures(
                FeatureReference.newBuilder().setName("feature1").setProject("project").build())
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

    List<FeatureRow> featureRows =
        Lists.newArrayList(
            FeatureRow.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                        Field.newBuilder().setName("entity2").setValue(strValue("a")).build(),
                        Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                        Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
                .setFeatureSet("featureSet")
                .build(),
            FeatureRow.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                        Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                        Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                        Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
                .setFeatureSet("featureSet")
                .build());

    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .addAllFeatureReferences(request.getFeaturesList())
            .setSpec(getFeatureSetSpec())
            .build();

    when(specService.getFeatureSets(request.getFeaturesList()))
        .thenReturn(Collections.singletonList(featureSetRequest));
    when(retriever.getOnlineFeatures(
            request.getEntityRowsList(), Collections.singletonList(featureSetRequest)))
        .thenReturn(Collections.singletonList(featureRows));
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(1))
                    .putFields("entity2", strValue("a"))
                    .putFields("project/feature1", intValue(1)))
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", intValue(2))
                    .putFields("entity2", strValue("b"))
                    .putFields("project/feature1", intValue(2)))
            .build();
    GetOnlineFeaturesResponse actual = onlineServingService.getOnlineFeatures(request);
    assertThat(
        responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }

  private List<Map<String, Value>> responseToMapList(GetOnlineFeaturesResponse response) {
    return response.getFieldValuesList().stream()
        .map(FieldValues::getFieldsMap)
        .collect(Collectors.toList());
  }

  private Value intValue(int val) {
    return Value.newBuilder().setInt64Val(val).build();
  }

  private Value strValue(String val) {
    return Value.newBuilder().setStringVal(val).build();
  }

  private FeatureSetSpec getFeatureSetSpec() {
    return FeatureSetSpec.newBuilder()
        .setProject("project")
        .setName("featureSet")
        .addEntities(EntitySpec.newBuilder().setName("entity1"))
        .addEntities(EntitySpec.newBuilder().setName("entity2"))
        .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
        .build();
  }
}
