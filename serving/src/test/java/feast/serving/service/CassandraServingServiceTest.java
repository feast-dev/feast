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

import static feast.serving.test.TestUtil.intValue;
import static feast.serving.test.TestUtil.strValue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.datastax.driver.core.Session;
import feast.core.FeatureSetProto;
import feast.serving.specs.CachedSpecService;
import feast.serving.specs.FeatureSetRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

public class CassandraServingServiceTest {

  @Mock Session session;

  @Mock CachedSpecService specService;

  @Mock Tracer tracer;

  private CassandraServingService cassandraServingService;

  @Before
  public void setUp() {
    initMocks(this);

    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    cassandraServingService =
        new CassandraServingService(session, "test", "feature_store", specService, tracer);
  }

  @Test
  public void shouldConstructCassandraKeyCorrectly() {
    List<String> cassandraKeys =
        cassandraServingService.createLookupKeys(
            new ArrayList<String>() {
              {
                add("entity1");
                add("entity2");
              }
            },
            new ArrayList<EntityRow>() {
              {
                add(
                    EntityRow.newBuilder()
                        .putFields("entity1", intValue(1))
                        .putFields("entity2", strValue("a"))
                        .build());
                add(
                    EntityRow.newBuilder()
                        .putFields("entity1", intValue(2))
                        .putFields("entity2", strValue("b"))
                        .build());
              }
            },
            FeatureSetRequest.newBuilder().setSpec(FeatureSetProto.FeatureSetSpec.newBuilder().setName("featureSet").setVersion(1).build()).build()
      );
    List<String> expectedKeys =
        new ArrayList<String>() {
          {
            add("featureSet:1:entity1=1|entity2=a");
            add("featureSet:1:entity1=2|entity2=b");
          }
        };

    Assert.assertEquals(expectedKeys, cassandraKeys);
  }

  @Test(expected = Exception.class)
  public void shouldThrowExceptionWhenCannotConstructCassandraKey() {
    List<String> cassandraKeys =
        cassandraServingService.createLookupKeys(
            new ArrayList<String>() {
              {
                add("entity1");
                add("entity2");
              }
            },
            new ArrayList<EntityRow>() {
              {
                add(EntityRow.newBuilder().putFields("entity1", intValue(1)).build());
                add(
                    EntityRow.newBuilder()
                        .putFields("entity1", intValue(2))
                        .putFields("entity2", strValue("b"))
                        .build());
              }
            },
            FeatureSetRequest.newBuilder().setSpec(FeatureSetProto.FeatureSetSpec.newBuilder().setName("featureSet").setVersion(1).build()).build());
  }
}
