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

import static feast.common.it.DataGenerator.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.proto.types.ValueProto;
import feast.serving.specs.CachedSpecService;
import feast.serving.specs.CoreFeatureSpecRetriever;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.ProtoFeature;
import feast.storage.connectors.redis.retriever.OnlineRetriever;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

public class OnlineServingServiceTest {

  @Mock CachedSpecService specService;
  @Mock Tracer tracer;
  @Mock OnlineRetriever retrieverV2;
  private String transformationServiceEndpoint;

  private OnlineServingServiceV2 onlineServingServiceV2;

  List<Feature> mockedFeatureRows;
  List<FeatureProto.FeatureSpecV2> featureSpecs;

  @Before
  public void setUp() {
    initMocks(this);
    CoreFeatureSpecRetriever coreFeatureSpecRetriever = new CoreFeatureSpecRetriever(specService);
    OnlineTransformationService onlineTransformationService =
        new OnlineTransformationService(transformationServiceEndpoint, coreFeatureSpecRetriever);
    onlineServingServiceV2 =
        new OnlineServingServiceV2(
            retrieverV2, tracer, coreFeatureSpecRetriever, onlineTransformationService);

    mockedFeatureRows = new ArrayList<>();
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_1")
                .build(),
            Timestamp.newBuilder().setSeconds(100).build(),
            createStrValue("1")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_2")
                .build(),
            Timestamp.newBuilder().setSeconds(100).build(),
            createStrValue("2")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_1")
                .build(),
            Timestamp.newBuilder().setSeconds(100).build(),
            createStrValue("3")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_2")
                .build(),
            Timestamp.newBuilder().setSeconds(100).build(),
            createStrValue("4")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_3")
                .build(),
            Timestamp.newBuilder().setSeconds(100).build(),
            createStrValue("5")));
    mockedFeatureRows.add(
        new ProtoFeature(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_1")
                .build(),
            Timestamp.newBuilder().setSeconds(50).build(),
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
            .setFeatureTable("featuretable_1")
            .setName("feature_1")
            .build();
    ServingAPIProto.FeatureReferenceV2 featureReference2 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable("featuretable_1")
            .setName("feature_2")
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        List.of(featureReference1, featureReference2);
    GetOnlineFeaturesRequestV2 request = getOnlineFeaturesRequestV2(projectName, featureReferences);

    List<Feature> entityKeyList1 = new ArrayList<>();
    List<Feature> entityKeyList2 = new ArrayList<>();
    entityKeyList1.add(mockedFeatureRows.get(0));
    entityKeyList1.add(mockedFeatureRows.get(1));
    entityKeyList2.add(mockedFeatureRows.get(2));
    entityKeyList2.add(mockedFeatureRows.get(3));

    List<List<Feature>> featureRows = List.of(entityKeyList1, entityKeyList2);

    when(retrieverV2.getOnlineFeatures(any(), any(), any(), any())).thenReturn(featureRows);
    when(specService.getFeatureTableSpec(any(), any())).thenReturn(getFeatureTableSpec());
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(0).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(2).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(3).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));

    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", createInt64Value(1))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", createStrValue("a"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("featuretable_1:feature_1", createStrValue("1"))
                    .putStatuses("featuretable_1:feature_1", FieldStatus.PRESENT)
                    .putFields("featuretable_1:feature_2", createStrValue("2"))
                    .putStatuses("featuretable_1:feature_2", FieldStatus.PRESENT)
                    .build())
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", createInt64Value(2))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", createStrValue("b"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("featuretable_1:feature_1", createStrValue("3"))
                    .putStatuses("featuretable_1:feature_1", FieldStatus.PRESENT)
                    .putFields("featuretable_1:feature_2", createStrValue("4"))
                    .putStatuses("featuretable_1:feature_2", FieldStatus.PRESENT)
                    .build())
            .build();
    GetOnlineFeaturesResponse actual = onlineServingServiceV2.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesAndMetadataIfKeysNotPresent() {
    String projectName = "default";
    ServingAPIProto.FeatureReferenceV2 featureReference1 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable("featuretable_1")
            .setName("feature_1")
            .build();
    ServingAPIProto.FeatureReferenceV2 featureReference2 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable("featuretable_1")
            .setName("feature_2")
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        List.of(featureReference1, featureReference2);
    GetOnlineFeaturesRequestV2 request = getOnlineFeaturesRequestV2(projectName, featureReferences);

    List<Feature> entityKeyList1 = new ArrayList<>();
    List<Feature> entityKeyList2 = new ArrayList<>();
    entityKeyList1.add(mockedFeatureRows.get(0));
    entityKeyList1.add(mockedFeatureRows.get(1));
    entityKeyList2.add(mockedFeatureRows.get(4));

    List<List<Feature>> featureRows = List.of(entityKeyList1, entityKeyList2);

    when(retrieverV2.getOnlineFeatures(any(), any(), any(), any())).thenReturn(featureRows);
    when(specService.getFeatureTableSpec(any(), any())).thenReturn(getFeatureTableSpec());
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(0).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));

    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", createInt64Value(1))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", createStrValue("a"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("featuretable_1:feature_1", createStrValue("1"))
                    .putStatuses("featuretable_1:feature_1", FieldStatus.PRESENT)
                    .putFields("featuretable_1:feature_2", createStrValue("2"))
                    .putStatuses("featuretable_1:feature_2", FieldStatus.PRESENT)
                    .build())
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", createInt64Value(2))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", createStrValue("b"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("featuretable_1:feature_1", createEmptyValue())
                    .putStatuses("featuretable_1:feature_1", FieldStatus.NOT_FOUND)
                    .putFields("featuretable_1:feature_2", createEmptyValue())
                    .putStatuses("featuretable_1:feature_2", FieldStatus.NOT_FOUND)
                    .build())
            .build();
    GetOnlineFeaturesResponse actual = onlineServingServiceV2.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesAndMetadataIfMaxAgeIsExceeded() {
    String projectName = "default";
    ServingAPIProto.FeatureReferenceV2 featureReference1 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable("featuretable_1")
            .setName("feature_1")
            .build();
    ServingAPIProto.FeatureReferenceV2 featureReference2 =
        ServingAPIProto.FeatureReferenceV2.newBuilder()
            .setFeatureTable("featuretable_1")
            .setName("feature_2")
            .build();
    List<ServingAPIProto.FeatureReferenceV2> featureReferences =
        List.of(featureReference1, featureReference2);
    GetOnlineFeaturesRequestV2 request = getOnlineFeaturesRequestV2(projectName, featureReferences);

    List<Feature> entityKeyList1 = new ArrayList<>();
    List<Feature> entityKeyList2 = new ArrayList<>();
    entityKeyList1.add(mockedFeatureRows.get(5));
    entityKeyList1.add(mockedFeatureRows.get(1));
    entityKeyList2.add(mockedFeatureRows.get(5));
    entityKeyList2.add(mockedFeatureRows.get(1));

    List<List<Feature>> featureRows = List.of(entityKeyList1, entityKeyList2);

    when(retrieverV2.getOnlineFeatures(any(), any(), any(), any())).thenReturn(featureRows);
    when(specService.getFeatureTableSpec(any(), any()))
        .thenReturn(
            FeatureTableSpec.newBuilder()
                .setName("featuretable_1")
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
                .setMaxAge(Duration.newBuilder().setSeconds(1))
                .build());
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(5).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));

    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected =
        GetOnlineFeaturesResponse.newBuilder()
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", createInt64Value(1))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", createStrValue("a"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("featuretable_1:feature_1", createEmptyValue())
                    .putStatuses("featuretable_1:feature_1", FieldStatus.OUTSIDE_MAX_AGE)
                    .putFields("featuretable_1:feature_2", createStrValue("2"))
                    .putStatuses("featuretable_1:feature_2", FieldStatus.PRESENT)
                    .build())
            .addFieldValues(
                FieldValues.newBuilder()
                    .putFields("entity1", createInt64Value(2))
                    .putStatuses("entity1", FieldStatus.PRESENT)
                    .putFields("entity2", createStrValue("b"))
                    .putStatuses("entity2", FieldStatus.PRESENT)
                    .putFields("featuretable_1:feature_1", createEmptyValue())
                    .putStatuses("featuretable_1:feature_1", FieldStatus.OUTSIDE_MAX_AGE)
                    .putFields("featuretable_1:feature_2", createStrValue("2"))
                    .putStatuses("featuretable_1:feature_2", FieldStatus.PRESENT)
                    .build())
            .build();
    GetOnlineFeaturesResponse actual = onlineServingServiceV2.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  private FeatureTableSpec getFeatureTableSpec() {
    return FeatureTableSpec.newBuilder()
        .setName("featuretable_1")
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
        .setMaxAge(Duration.newBuilder().setSeconds(120))
        .build();
  }

  private GetOnlineFeaturesRequestV2 getOnlineFeaturesRequestV2(
      String projectName, List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    return GetOnlineFeaturesRequestV2.newBuilder()
        .setProject(projectName)
        .addAllFeatures(featureReferences)
        .addEntityRows(
            GetOnlineFeaturesRequestV2.EntityRow.newBuilder()
                .setTimestamp(Timestamp.newBuilder().setSeconds(100))
                .putFields("entity1", createInt64Value(1))
                .putFields("entity2", createStrValue("a")))
        .addEntityRows(
            GetOnlineFeaturesRequestV2.EntityRow.newBuilder()
                .setTimestamp(Timestamp.newBuilder().setSeconds(100))
                .putFields("entity1", createInt64Value(2))
                .putFields("entity2", createStrValue("b")))
        .addFeatures(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_1")
                .build())
        .addFeatures(
            ServingAPIProto.FeatureReferenceV2.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_2")
                .build())
        .build();
  }
}
