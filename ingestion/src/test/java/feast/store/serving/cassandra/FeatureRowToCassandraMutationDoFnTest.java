/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.store.serving.cassandra;

import com.google.protobuf.Duration;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSet;
import feast.test.TestUtil;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType.Enum;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class FeatureRowToCassandraMutationDoFnTest implements Serializable {

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void processElement_shouldCreateCassandraMutation_givenFeatureRow() {
    FeatureSetSpec featureSetSpec =
        TestUtil.createFeatureSetSpec(
            "fs",
            1,
            10,
            new HashMap<String, Enum>() {
              {
                put("entity1", Enum.INT64);
              }
            },
            new HashMap<String, Enum>() {
              {
                put("feature1", Enum.STRING);
              }
            });
    FeatureRow featureRow =
        TestUtil.createFeatureRow(
            featureSetSpec,
            10,
            new HashMap<String, Value>() {
              {
                put("entity1", TestUtil.intValue(1));
                put("feature1", TestUtil.strValue("a"));
              }
            });

    PCollection<FeatureRow> input = testPipeline.apply(Create.of(featureRow));

    PCollection<CassandraMutation> output =
        input.apply(
            ParDo.of(
                new FeatureRowToCassandraMutationDoFn(
                    new HashMap<String, FeatureSet>() {
                      {
                        put(
                            featureSetSpec.getName() + ":" + featureSetSpec.getVersion(),
                            FeatureSet.newBuilder().setSpec(featureSetSpec).build());
                      }
                    },
                    Duration.newBuilder().setSeconds(0).build())));

    CassandraMutation[] expected =
        new CassandraMutation[] {
          new CassandraMutation(
              "fs:1:entity1=1",
              "feature1",
              ByteBuffer.wrap(TestUtil.strValue("a").toByteArray()),
              10000000,
              10)
        };

    PAssert.that(output).containsInAnyOrder(expected);

    testPipeline.run();
  }

  @Test
  public void
      processElement_shouldCreateCassandraMutations_givenFeatureRowWithMultipleEntitiesAndFeatures() {
    FeatureSetSpec featureSetSpec =
        TestUtil.createFeatureSetSpec(
            "fs",
            1,
            10,
            new HashMap<String, Enum>() {
              {
                put("entity1", Enum.INT64);
                put("entity2", Enum.STRING);
              }
            },
            new HashMap<String, Enum>() {
              {
                put("feature1", Enum.STRING);
                put("feature2", Enum.INT64);
              }
            });
    FeatureRow featureRow =
        TestUtil.createFeatureRow(
            featureSetSpec,
            10,
            new HashMap<String, Value>() {
              {
                put("entity1", TestUtil.intValue(1));
                put("entity2", TestUtil.strValue("b"));
                put("feature1", TestUtil.strValue("a"));
                put("feature2", TestUtil.intValue(2));
              }
            });

    PCollection<FeatureRow> input = testPipeline.apply(Create.of(featureRow));

    PCollection<CassandraMutation> output =
        input.apply(
            ParDo.of(
                new FeatureRowToCassandraMutationDoFn(
                    new HashMap<String, FeatureSet>() {
                      {
                        put(
                            featureSetSpec.getName() + ":" + featureSetSpec.getVersion(),
                            FeatureSet.newBuilder().setSpec(featureSetSpec).build());
                      }
                    },
                    Duration.newBuilder().setSeconds(0).build())));

    CassandraMutation[] expected =
        new CassandraMutation[] {
          new CassandraMutation(
              "fs:1:entity1=1|entity2=b",
              "feature1",
              ByteBuffer.wrap(TestUtil.strValue("a").toByteArray()),
              10000000,
              10),
          new CassandraMutation(
              "fs:1:entity1=1|entity2=b",
              "feature2",
              ByteBuffer.wrap(TestUtil.intValue(2).toByteArray()),
              10000000,
              10)
        };

    PAssert.that(output).containsInAnyOrder(expected);

    testPipeline.run();
  }

  @Test
  public void processElement_shouldUseDefaultMaxAge_whenMissingMaxAge() {
    Duration defaultTtl = Duration.newBuilder().setSeconds(500).build();
    FeatureSetSpec featureSetSpec =
        TestUtil.createFeatureSetSpec(
            "fs",
            1,
            0,
            new HashMap<String, Enum>() {
              {
                put("entity1", Enum.INT64);
              }
            },
            new HashMap<String, Enum>() {
              {
                put("feature1", Enum.STRING);
              }
            });
    FeatureRow featureRow =
        TestUtil.createFeatureRow(
            featureSetSpec,
            10,
            new HashMap<String, Value>() {
              {
                put("entity1", TestUtil.intValue(1));
                put("feature1", TestUtil.strValue("a"));
              }
            });

    PCollection<FeatureRow> input = testPipeline.apply(Create.of(featureRow));

    PCollection<CassandraMutation> output =
        input.apply(
            ParDo.of(
                new FeatureRowToCassandraMutationDoFn(
                    new HashMap<String, FeatureSet>() {
                      {
                        put(
                            featureSetSpec.getName() + ":" + featureSetSpec.getVersion(),
                            FeatureSet.newBuilder().setSpec(featureSetSpec).build());
                      }
                    },
                    defaultTtl)));

    CassandraMutation[] expected =
        new CassandraMutation[] {
          new CassandraMutation(
              "fs:1:entity1=1",
              "feature1",
              ByteBuffer.wrap(TestUtil.strValue("a").toByteArray()),
              10000000,
              500)
        };

    PAssert.that(output).containsInAnyOrder(expected);

    testPipeline.run();
  }
}
