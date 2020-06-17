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
package feast.ingestion.transform.specs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class FilterRelevantTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void onlySpecsThatMatchStoreSubscriptionShouldPass() {
    SourceProto.Source source =
        SourceProto.Source.newBuilder()
            .setKafkaSourceConfig(
                SourceProto.KafkaSourceConfig.newBuilder()
                    .setBootstrapServers("localhost")
                    .setTopic("topic")
                    .build())
            .build();

    StoreProto.Store store1 =
        StoreProto.Store.newBuilder()
            .addSubscriptions(
                StoreProto.Store.Subscription.newBuilder()
                    .setProject("project")
                    .setName("fs*")
                    .build())
            .build();

    StoreProto.Store store2 =
        StoreProto.Store.newBuilder()
            .addSubscriptions(
                StoreProto.Store.Subscription.newBuilder()
                    .setProject("project_2")
                    .setName("fs_1")
                    .build())
            .build();

    SourceProto.Source irrelevantSource =
        SourceProto.Source.newBuilder()
            .setKafkaSourceConfig(
                SourceProto.KafkaSourceConfig.newBuilder()
                    .setBootstrapServers("localhost")
                    .setTopic("other_topic")
                    .build())
            .build();

    PCollection<KV<String, FeatureSetProto.FeatureSetSpec>> filtered =
        p.apply(
                Create.of(
                    ImmutableMap.of(
                        "project/fs_1", makeSpecBuilder(source).build(), // pass
                        "project/fs_2",
                            makeSpecBuilder(irrelevantSource).build(), // different source
                        "invalid/fs_3",
                            makeSpecBuilder(source)
                                .setProject("invalid")
                                .build(), // invalid project
                        "project/invalid",
                            makeSpecBuilder(source).setName("invalid").build(), // invalid name
                        "project_2/fs_1",
                            makeSpecBuilder(source).setProject("project_2").build() // pass
                        )))
            .apply(Filter.by(new FilterRelevantFunction(source, ImmutableList.of(store1, store2))));

    PAssert.that(filtered)
        .containsInAnyOrder(
            KV.of("project/fs_1", makeSpecBuilder(source).build()),
            KV.of("project_2/fs_1", makeSpecBuilder(source).setProject("project_2").build()));

    PAssert.that(filtered.apply(Count.globally())).containsInAnyOrder(2L);

    p.run();
  }

  private FeatureSetProto.FeatureSetSpec.Builder makeSpecBuilder(SourceProto.Source source) {
    return FeatureSetProto.FeatureSetSpec.newBuilder()
        .setProject("project")
        .setName("fs_1")
        .setSource(source);
  }
}
