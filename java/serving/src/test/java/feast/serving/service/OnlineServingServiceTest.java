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

import static feast.serving.util.DataGenerator.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureViewProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.FieldStatus;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.types.ValueProto;
import feast.serving.registry.Registry;
import feast.serving.registry.RegistryRepository;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.ProtoFeature;
import feast.storage.connectors.redis.retriever.OnlineRetriever;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

public class OnlineServingServiceTest {

  @Mock Registry registry;
  @Mock Tracer tracer;
  @Mock OnlineRetriever retrieverV2;
  private String transformationServiceEndpoint;

  private OnlineServingServiceV2 onlineServingServiceV2;

  List<Feature> mockedFeatureRows;
  List<FeatureProto.FeatureSpecV2> featureSpecs;

  Timestamp now = Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build();

  @Before
  public void setUp() {
    initMocks(this);

    RegistryRepository registryRepo = new RegistryRepository(registry);

    OnlineTransformationService onlineTransformationService =
        new OnlineTransformationService(transformationServiceEndpoint, registryRepo);
    onlineServingServiceV2 =
        new OnlineServingServiceV2(
            retrieverV2, tracer, registryRepo, onlineTransformationService, "feast_project");

    mockedFeatureRows = new ArrayList<>();
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_1")
                .build(),
            now,
            createStrValue("1")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_2")
                .build(),
            now,
            createStrValue("2")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_1")
                .build(),
            now,
            createStrValue("3")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_2")
                .build(),
            now,
            createStrValue("4")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_3")
                .build(),
            now,
            createStrValue("5")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureViewName("featureview_1")
                .setFeatureName("feature_1")
                .build(),
            Timestamp.newBuilder().setSeconds(1).build(),
            createStrValue("6")));

    featureSpecs = new ArrayList<>();
    featureSpecs.add(
        FeatureProto.FeatureSpecV2.newBuilder()
            .setName("feature_1")
            .setValueType(ValueProto.ValueType.Enum.STRING)
            .build());
    featureSpecs.add(
        FeatureProto.FeatureSpecV2.newBuilder()
            .setName("feature_2")
            .setValueType(ValueProto.ValueType.Enum.STRING)
            .build());
  }

  @Test
  public void shouldReturnResponseWithValuesAndMetadataIfKeysPresent() {
    String projectName = "default";
    ServingAPIProto.FeatureReferenceV2 featureReference1 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_1")
            .build();
    ServingAPIProto.FeatureReferenceV2 featureReference2 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_2")
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        List.of(featureReference1, featureReference2);
    ServingAPIProto.GetOnlineFeaturesRequest request = getOnlineFeaturesRequest(featureReferences);

    List<List<Feature>> featureRows =
        List.of(
            List.of(mockedFeatureRows.get(0), mockedFeatureRows.get(1)),
            List.of(mockedFeatureRows.get(2), mockedFeatureRows.get(3)));

    when(retrieverV2.getOnlineFeatures(any(), any(), any())).thenReturn(featureRows);
    when(registry.getFeatureViewSpec(any())).thenReturn(getFeatureViewSpec());
    when(registry.getFeatureSpec(mockedFeatureRows.get(0).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(registry.getFeatureSpec(mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));
    when(registry.getFeatureSpec(mockedFeatureRows.get(2).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(registry.getFeatureSpec(mockedFeatureRows.get(3).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));

    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("1"))
                    .addValues(createStrValue("3"))
                    .addStatuses(FieldStatus.PRESENT)
                    .addStatuses(FieldStatus.PRESENT)
                    .addEventTimestamps(now)
                    .addEventTimestamps(now))
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("2"))
                    .addValues(createStrValue("4"))
                    .addStatuses(FieldStatus.PRESENT)
                    .addStatuses(FieldStatus.PRESENT)
                    .addEventTimestamps(now)
                    .addEventTimestamps(now))
            .setMetadata(
                ServingAPIProto.GetOnlineFeaturesResponseMetadata.newBuilder()
                    .setFeatureNames(
                        ServingAPIProto.FeatureList.newBuilder()
                            .addVal("featureview_1:feature_1")
                            .addVal("featureview_1:feature_2")))
            .build();
    ServingAPIProto.GetOnlineFeaturesResponse actual =
        onlineServingServiceV2.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesAndMetadataIfKeysNotPresent() {
    String projectName = "default";
    ServingAPIProto.FeatureReferenceV2 featureReference1 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_1")
            .build();
    ServingAPIProto.FeatureReferenceV2 featureReference2 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_2")
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        List.of(featureReference1, featureReference2);
    ServingAPIProto.GetOnlineFeaturesRequest request = getOnlineFeaturesRequest(featureReferences);

    List<Feature> entityKeyList1 = new ArrayList<>();
    List<Feature> entityKeyList2 = new ArrayList<>();
    entityKeyList1.add(mockedFeatureRows.get(0));
    entityKeyList1.add(mockedFeatureRows.get(1));
    entityKeyList2.add(mockedFeatureRows.get(4));

    List<List<Feature>> featureRows =
        List.of(
            List.of(mockedFeatureRows.get(0), mockedFeatureRows.get(1)),
            Arrays.asList(null, mockedFeatureRows.get(4)));

    when(retrieverV2.getOnlineFeatures(any(), any(), any())).thenReturn(featureRows);
    when(registry.getFeatureViewSpec(any())).thenReturn(getFeatureViewSpec());
    when(registry.getFeatureSpec(mockedFeatureRows.get(0).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(registry.getFeatureSpec(mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));

    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("1"))
                    .addValues(createEmptyValue())
                    .addStatuses(FieldStatus.PRESENT)
                    .addStatuses(FieldStatus.NOT_FOUND)
                    .addEventTimestamps(now)
                    .addEventTimestamps(Timestamp.newBuilder().build()))
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("2"))
                    .addValues(createStrValue("5"))
                    .addStatuses(FieldStatus.PRESENT)
                    .addStatuses(FieldStatus.PRESENT)
                    .addEventTimestamps(now)
                    .addEventTimestamps(now))
            .setMetadata(
                ServingAPIProto.GetOnlineFeaturesResponseMetadata.newBuilder()
                    .setFeatureNames(
                        ServingAPIProto.FeatureList.newBuilder()
                            .addVal("featureview_1:feature_1")
                            .addVal("featureview_1:feature_2")))
            .build();
    GetOnlineFeaturesResponse actual = onlineServingServiceV2.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithValuesAndMetadataIfMaxAgeIsExceeded() {
    String projectName = "default";
    ServingAPIProto.FeatureReferenceV2 featureReference1 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_1")
            .build();
    ServingAPIProto.FeatureReferenceV2 featureReference2 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featureview_1")
            .setFeatureName("feature_2")
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        List.of(featureReference1, featureReference2);
    ServingAPIProto.GetOnlineFeaturesRequest request = getOnlineFeaturesRequest(featureReferences);

    List<List<Feature>> featureRows =
        List.of(
            List.of(mockedFeatureRows.get(5), mockedFeatureRows.get(1)),
            List.of(mockedFeatureRows.get(5), mockedFeatureRows.get(1)));

    when(retrieverV2.getOnlineFeatures(any(), any(), any())).thenReturn(featureRows);
    when(registry.getFeatureViewSpec(any()))
        .thenReturn(
            FeatureViewProto.FeatureViewSpec.newBuilder()
                .setName("featureview_1")
                .addEntities("entity1")
                .addEntities("entity2")
                .addFeatures(
                    FeatureProto.FeatureSpecV2.newBuilder()
                        .setName("feature_1")
                        .setValueType(ValueProto.ValueType.Enum.STRING)
                        .build())
                .addFeatures(
                    FeatureProto.FeatureSpecV2.newBuilder()
                        .setName("feature_2")
                        .setValueType(ValueProto.ValueType.Enum.STRING)
                        .build())
                .setTtl(Duration.newBuilder().setSeconds(3600))
                .build());
    when(registry.getFeatureSpec(mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));
    when(registry.getFeatureSpec(mockedFeatureRows.get(5).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));

    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("6"))
                    .addValues(createStrValue("6"))
                    .addStatuses(FieldStatus.OUTSIDE_MAX_AGE)
                    .addStatuses(FieldStatus.OUTSIDE_MAX_AGE)
                    .addEventTimestamps(Timestamp.newBuilder().setSeconds(1).build())
                    .addEventTimestamps(Timestamp.newBuilder().setSeconds(1).build()))
            .addResults(
                GetOnlineFeaturesResponse.FeatureVector.newBuilder()
                    .addValues(createStrValue("2"))
                    .addValues(createStrValue("2"))
                    .addStatuses(FieldStatus.PRESENT)
                    .addStatuses(FieldStatus.PRESENT)
                    .addEventTimestamps(now)
                    .addEventTimestamps(now))
            .setMetadata(
                ServingAPIProto.GetOnlineFeaturesResponseMetadata.newBuilder()
                    .setFeatureNames(
                        ServingAPIProto.FeatureList.newBuilder()
                            .addVal("featureview_1:feature_1")
                            .addVal("featureview_1:feature_2")))
            .build();
    GetOnlineFeaturesResponse actual = onlineServingServiceV2.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  private FeatureViewProto.FeatureViewSpec getFeatureViewSpec() {
    return FeatureViewProto.FeatureViewSpec.newBuilder()
        .setName("featureview_1")
        .addEntities("entity1")
        .addEntities("entity2")
        .addFeatures(
            FeatureProto.FeatureSpecV2.newBuilder()
                .setName("feature_1")
                .setValueType(ValueProto.ValueType.Enum.STRING)
                .build())
        .addFeatures(
            FeatureProto.FeatureSpecV2.newBuilder()
                .setName("feature_2")
                .setValueType(ValueProto.ValueType.Enum.STRING)
                .build())
        .setTtl(Duration.newBuilder().setSeconds(120))
        .build();
  }

  private ServingAPIProto.GetOnlineFeaturesRequest getOnlineFeaturesRequest(
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    return ServingAPIProto.GetOnlineFeaturesRequest.newBuilder()
        .setFeatures(
            ServingAPIProto.FeatureList.newBuilder()
                .addAllVal(
                    featureReferences.stream()
                        .map(feast.common.models.Feature::getFeatureReference)
                        .collect(Collectors.toList()))
                .build())
        .putAllEntities(
            ImmutableMap.of(
                "entity1",
                    ValueProto.RepeatedValue.newBuilder()
                        .addAllVal(List.of(createInt64Value(1), createInt64Value(2)))
                        .build(),
                "entity2",
                    ValueProto.RepeatedValue.newBuilder()
                        .addAllVal(List.of(createStrValue("a"), createStrValue("b")))
                        .build()))
        .build();
  }
}
