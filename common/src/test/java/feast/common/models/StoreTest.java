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
package feast.common.models;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertTrue;

import feast.proto.core.StoreProto.Store.Subscription;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class StoreTest {

  private List<Subscription> allSubscriptions;

  @Before
  public void setUp() {

    Subscription emptySubscription = Subscription.newBuilder().build();
    Subscription subscription1 = Subscription.newBuilder().setProject("*").setName("*").build();
    Subscription subscription2 =
        Subscription.newBuilder().setProject("project1").setName("fs_2").build();
    Subscription subscription3 =
        Subscription.newBuilder().setProject("project1").setName("fs_1").setExclude(true).build();
    allSubscriptions =
        Arrays.asList(emptySubscription, subscription1, subscription2, subscription3);
  }

  @Test
  public void shouldReturnSubscriptionsBasedOnStr() {
    String subscriptions = "project1:fs_1:true,project1:fs_2";
    List<Subscription> actual1 = Store.parseSubFromStr(subscriptions);
    List<Subscription> expected1 = Arrays.asList(allSubscriptions.get(2), allSubscriptions.get(3));

    List<Subscription> actual2 = Store.parseSubFromStrWithoutExclusions(subscriptions);
    List<Subscription> expected2 = Arrays.asList(allSubscriptions.get(2));

    assertTrue(actual1.containsAll(expected1) && expected1.containsAll(actual1));
    assertTrue(actual2.containsAll(expected2) && expected2.containsAll(actual2));
  }

  @Test
  public void shouldReturnStringBasedOnSubscription() {
    // Case: default exclude should be false
    String actual1 = Store.parseSubscriptionFrom(allSubscriptions.get(2));
    Subscription sub1 = allSubscriptions.get(2);
    String expected1 = sub1.getProject() + ":" + sub1.getName() + ":" + sub1.getExclude();

    // Case: explicit setting of exclude to true
    String actual2 = Store.parseSubscriptionFrom(allSubscriptions.get(3));
    Subscription sub2 = allSubscriptions.get(3);
    String expected2 = sub2.getProject() + ":" + sub2.getName() + ":" + sub2.getExclude();

    assertThat(actual1, equalTo(expected1));
    assertThat(actual2, equalTo(expected2));
  }

  @Test
  public void shouldSubscribeToFeatureSet() {
    allSubscriptions = allSubscriptions.subList(2, 4);
    // Case: excluded flag = true
    boolean actual1 = Store.isSubscribedToFeatureSet(allSubscriptions, "project1", "fs_1");
    boolean expected1 = false;

    // Case: excluded flag = false
    boolean actual2 = Store.isSubscribedToFeatureSet(allSubscriptions, "project1", "fs_2");
    boolean expected2 = true;

    // Case: featureset does not exist
    boolean actual3 =
        Store.isSubscribedToFeatureSet(allSubscriptions, "project1", "fs_nonexistent");
    boolean expected3 = false;

    assertThat(actual1, equalTo(expected1));
    assertThat(actual2, equalTo(expected2));
    assertThat(actual3, equalTo(expected3));
  }
}
