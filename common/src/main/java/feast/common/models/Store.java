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

import feast.proto.core.StoreProto;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Store {

  /**
   * Accepts a comma-delimited Subscriptions that is string-formatted and converts it to a list of
   * Subscription class objects.
   *
   * @param subscriptions String formatted Subscriptions, comma delimited.
   * @param exclude flag to determine if subscriptions with exclusion flag should be returned
   * @return List of Subscription class objects
   */
  public static List<StoreProto.Store.Subscription> parseSubscriptionFrom(
      String subscriptions, boolean exclude) {
    List<StoreProto.Store.Subscription> allSubscriptions =
        Arrays.stream(subscriptions.split(","))
            .map(subscriptionStr -> convertStringToSubscription(subscriptionStr))
            .collect(Collectors.toList());

    if (exclude) {
      allSubscriptions =
          allSubscriptions.stream().filter(sub -> !sub.getExclude()).collect(Collectors.toList());
    }

    return allSubscriptions;
  }

  /**
   * Accepts a Subscription class object and returns it in string format
   *
   * @param subscription Subscription class to be converted to string format
   * @return String formatted Subscription class
   */
  public static String parseSubscriptionFrom(StoreProto.Store.Subscription subscription) {
    if (subscription.getName().isEmpty() || subscription.getProject().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Missing arguments in subscription string: %s", subscription.toString()));
    }

    return String.format(
        "%s:%s:%s", subscription.getProject(), subscription.getName(), subscription.getExclude());
  }

  /**
   * Accepts a exclude parameter to determine whether to return subscriptions that are excluded.
   *
   * @param subscription String formatted Subscription to be converted to Subscription class
   * @return Subscription class with its respective attributes
   */
  public static StoreProto.Store.Subscription convertStringToSubscription(String subscription) {
    if (subscription.equals("")) {
      return StoreProto.Store.Subscription.newBuilder().build();
    }
    String[] split = subscription.split(":");
    if (split.length == 2) {
      // Backward compatibility check
      return StoreProto.Store.Subscription.newBuilder()
          .setProject(split[0])
          .setName(split[1])
          .build();
    }
    return StoreProto.Store.Subscription.newBuilder()
        .setProject(split[0])
        .setName(split[1])
        .setExclude(Boolean.parseBoolean(split[2]))
        .build();
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
      List<StoreProto.Store.Subscription> subscriptions,
      String projectName,
      String featureSetName) {
    // Case 1: Highest priority check, to exclude all matching subscriptions with excluded flag =
    // true
    for (StoreProto.Store.Subscription sub : subscriptions) {
      // If configuration missing, fail
      if (sub.getProject().isEmpty() || sub.getName().isEmpty()) {
        throw new IllegalArgumentException(
            String.format("Subscription is missing arguments: %s", sub.toString()));
      }

      String subName = sub.getName();
      String subProject = sub.getProject();
      if (!sub.getName().contains(".*")) {
        subName = subName.replace("*", ".*");
      }
      if (!sub.getProject().contains(".*")) {
        subProject = subProject.replace("*", ".*");
      }

      // Match feature set name to pattern
      Pattern patternName = Pattern.compile(subName);
      Pattern patternProject = Pattern.compile(subProject);

      // SubCase: Project name and feature set name matches and excluded flag is true
      if (patternProject.matcher(projectName).matches()
          && patternName.matcher(featureSetName).matches()
          && sub.getExclude()) {
        return false;
      }
    }

    // Case 2: Featureset is not excluded, check if it is included in the current subscriptions
    // filteredSubscriptions only contain subscriptions with excluded flag = false
    List<StoreProto.Store.Subscription> filteredSubscriptions =
        subscriptions.stream().filter(sub -> !sub.getExclude()).collect(Collectors.toList());

    for (StoreProto.Store.Subscription filteredSub : filteredSubscriptions) {
      // Convert wildcard to regex
      String subName = filteredSub.getName();
      String subProject = filteredSub.getProject();
      if (!filteredSub.getName().contains(".*")) {
        subName = subName.replace("*", ".*");
      }
      if (!filteredSub.getProject().contains(".*")) {
        subProject = subProject.replace("*", ".*");
      }

      // Match feature set name to pattern
      Pattern patternName = Pattern.compile(subName);
      Pattern patternProject = Pattern.compile(subProject);

      // SubCase: Project name and feature set name matches
      if (patternProject.matcher(projectName).matches()
          && patternName.matcher(featureSetName).matches()) {
        return true;
      }
    }
    return false;
  }
}
