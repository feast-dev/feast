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

import feast.proto.core.StoreProto.Store.Subscription;
import java.util.List;
import java.util.regex.Pattern;

public class Store {

  /**
   * Accepts a Subscription class object and returns it in string format
   *
   * @param subscription Subscription class to be converted to string format
   * @return String formatted Subscription class
   */
  public static String parseSubscriptionFrom(Subscription subscription) {
    if (subscription.getName().isEmpty() || subscription.getProject().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Missing arguments in subscription string: %s", subscription.toString()));
    }

    return String.format("%s:%s", subscription.getProject(), subscription.getName());
  }

  /**
   * Accepts a exclude parameter to determine whether to return subscriptions that are excluded.
   *
   * @param subscription String formatted Subscription to be converted to Subscription class
   * @return Subscription class with its respective attributes
   */
  public static Subscription convertStringToSubscription(String subscription) {
    if (subscription.equals("")) {
      return Subscription.newBuilder().build();
    }
    String[] split = subscription.split(":");
    return Subscription.newBuilder().setProject(split[0]).setName(split[1]).build();
  }

  /**
   * The current use of this function is to determine whether a FeatureRow is subscribed to a
   * Featureset.
   *
   * @param subscriptions List of Subscriptions available in Store
   * @param projectName Project name used for matching Subscription's Project
   * @param featureSetName Featureset name used for matching Subscription's Featureset
   * @return boolean flag to signify if FeatureRow is subscribed to Featureset
   */
  public static boolean isSubscribedToFeatureSet(
      List<Subscription> subscriptions, String projectName, String featureSetName) {
    for (Subscription sub : subscriptions) {
      // If configuration missing, fail
      if (sub.getProject().isEmpty() || sub.getName().isEmpty()) {
        throw new IllegalArgumentException(
            String.format("Subscription is missing arguments: %s", sub.toString()));
      }

      // If all wildcards, subscribe to everything
      if (sub.getProject().equals("*") || sub.getName().equals("*")) {
        return true;
      }

      // Match project name
      if (!projectName.equals(sub.getProject())) {
        continue;
      }

      // Convert wildcard to regex
      String subName = sub.getName();
      if (!sub.getName().contains(".*")) {
        subName = subName.replace("*", ".*");
      }

      // Match feature set name to pattern
      Pattern pattern = Pattern.compile(subName);
      if (!pattern.matcher(featureSetName).matches()) {
        continue;
      }
      return true;
    }

    return false;
  }
}
