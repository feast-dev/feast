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
package feast.ingestion.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.Subscription;
import feast.proto.types.FeatureRowProto;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;

public class FeatureRowToStoreAllocatorTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  private StoreProto.Store newStore(String s) {
    return StoreProto.Store.newBuilder()
        .addSubscriptions(
            StoreProto.Store.Subscription.newBuilder().setProject("project").setName(s).build())
        .build();
  }

  private StoreProto.Store newStore(List<StoreProto.Store.Subscription> subscriptionList) {
    return StoreProto.Store.newBuilder().addAllSubscriptions(subscriptionList).build();
  }

  @Test
  public void featureRowShouldBeAllocatedToStoreTagsAccordingToSubscription() {
    StoreProto.Store bqOnlyStore = newStore("bq*");
    StoreProto.Store redisOnlyStore = newStore("redis*");
    StoreProto.Store anyStore = newStore("*");

    Map<StoreProto.Store, TupleTag<FeatureRowProto.FeatureRow>> storeTags =
        ImmutableMap.of(
            bqOnlyStore, new TupleTag<>(),
            redisOnlyStore, new TupleTag<>(),
            anyStore, new TupleTag<>());

    PCollectionTuple allocatedRows =
        p.apply(
                Create.of(
                    FeatureRowProto.FeatureRow.newBuilder().setFeatureSet("project/bq_1").build(),
                    FeatureRowProto.FeatureRow.newBuilder().setFeatureSet("project/bq_2").build(),
                    FeatureRowProto.FeatureRow.newBuilder()
                        .setFeatureSet("project/redis_1")
                        .build(),
                    FeatureRowProto.FeatureRow.newBuilder()
                        .setFeatureSet("project/redis_2")
                        .build(),
                    FeatureRowProto.FeatureRow.newBuilder()
                        .setFeatureSet("project/redis_3")
                        .build()))
            .apply(
                FeatureRowToStoreAllocator.newBuilder()
                    .setStoreTags(storeTags)
                    .setStores(ImmutableList.of(bqOnlyStore, redisOnlyStore, anyStore))
                    .build());

    PAssert.that(
            allocatedRows
                .get(storeTags.get(bqOnlyStore))
                .setCoder(ProtoCoder.of(FeatureRowProto.FeatureRow.class))
                .apply("CountBq", Count.globally()))
        .containsInAnyOrder(2L);

    PAssert.that(
            allocatedRows
                .get(storeTags.get(redisOnlyStore))
                .setCoder(ProtoCoder.of(FeatureRowProto.FeatureRow.class))
                .apply("CountRedis", Count.globally()))
        .containsInAnyOrder(3L);

    PAssert.that(
            allocatedRows
                .get(storeTags.get(anyStore))
                .setCoder(ProtoCoder.of(FeatureRowProto.FeatureRow.class))
                .apply("CountAll", Count.globally()))
        .containsInAnyOrder(5L);

    p.run();
  }

  @Test
  public void featureRowShouldBeAllocatedToStoreTagsAccordingToSubscriptionBlacklist() {
    Subscription subscription1 = Subscription.newBuilder().setProject("*").setName("*").build();
    Subscription subscription2 =
        Subscription.newBuilder().setProject("project1").setName("fs_2").build();
    Subscription subscription3 =
        Subscription.newBuilder().setProject("project1").setName("fs_1").setExclude(true).build();
    Subscription subscription4 =
        Subscription.newBuilder().setProject("project2").setName("*").setExclude(true).build();

    List<Subscription> testStoreSubscriptions1 =
        Arrays.asList(subscription1, subscription2, subscription3);
    StoreProto.Store testStore1 = newStore(testStoreSubscriptions1);

    List<Subscription> testStoreSubscriptions2 = Arrays.asList(subscription1, subscription4);
    StoreProto.Store testStore2 = newStore(testStoreSubscriptions2);

    Map<StoreProto.Store, TupleTag<FeatureRowProto.FeatureRow>> storeTags =
        ImmutableMap.of(
            testStore1, new TupleTag<>(),
            testStore2, new TupleTag<>());

    PCollectionTuple allocatedRows =
        p.apply(
                Create.of(
                    FeatureRowProto.FeatureRow.newBuilder().setFeatureSet("project1/fs_1").build(),
                    FeatureRowProto.FeatureRow.newBuilder().setFeatureSet("project2/fs_1").build(),
                    FeatureRowProto.FeatureRow.newBuilder().setFeatureSet("project2/fs_2").build()))
            .apply(
                FeatureRowToStoreAllocator.newBuilder()
                    .setStoreTags(storeTags)
                    .setStores(ImmutableList.of(testStore1, testStore2))
                    .build());

    PAssert.that(
            allocatedRows
                .get(storeTags.get(testStore1))
                .setCoder(ProtoCoder.of(FeatureRowProto.FeatureRow.class))
                .apply("CountStore1", Count.globally()))
        .containsInAnyOrder(2L);

    PAssert.that(
            allocatedRows
                .get(storeTags.get(testStore2))
                .setCoder(ProtoCoder.of(FeatureRowProto.FeatureRow.class))
                .apply("CountStore2", Count.globally()))
        .containsInAnyOrder(1L);

    p.run();
  }
}
