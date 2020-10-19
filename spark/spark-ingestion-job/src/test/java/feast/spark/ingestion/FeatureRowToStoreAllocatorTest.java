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
package feast.spark.ingestion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import feast.proto.core.StoreProto;
import feast.proto.types.FeatureRowProto;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class FeatureRowToStoreAllocatorTest {
  @Test
  public void shouldAllocateRowsWhenStoreSubscribed() {

    List<StoreProto.Store.Subscription> subscriptionList = new ArrayList<>();

    subscriptionList.add(
        StoreProto.Store.Subscription.newBuilder()
            .setProject("project")
            .setName("featureset")
            .build());
    StoreProto.Store store =
        StoreProto.Store.newBuilder().addAllSubscriptions(subscriptionList).build();

    FeatureRowProto.FeatureRow featureRow =
        FeatureRowProto.FeatureRow.newBuilder().setFeatureSet("project/featureset").build();

    FeatureRowToStoreAllocator featureRowToStoreAllocator = new FeatureRowToStoreAllocator();
    boolean actual = featureRowToStoreAllocator.allocateRowToStore(featureRow, store);

    assertTrue(actual);
  }

  @Test
  public void shouldNotAllocateRowsWhenStoresNotSubscribed() {

    List<StoreProto.Store.Subscription> subscriptionList = new ArrayList<>();

    subscriptionList.add(
        StoreProto.Store.Subscription.newBuilder()
            .setProject("incorrect-project")
            .setName("incorrect-featureset")
            .build());
    StoreProto.Store store =
        StoreProto.Store.newBuilder().addAllSubscriptions(subscriptionList).build();

    FeatureRowProto.FeatureRow featureRow =
        FeatureRowProto.FeatureRow.newBuilder().setFeatureSet("project/featureset").build();

    FeatureRowToStoreAllocator featureRowToStoreAllocator = new FeatureRowToStoreAllocator();
    boolean actual = featureRowToStoreAllocator.allocateRowToStore(featureRow, store);

    assertFalse(actual);
  }
}
