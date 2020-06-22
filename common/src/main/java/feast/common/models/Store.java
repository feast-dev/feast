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
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Store {

  /**
   * Accepts a comma-delimited string and converts it to a list of Subscription class objects.
   *
   * @param subscriptions String formatted Subscriptions, comma delimited.
   * @return List of Subscription class objects
   */
  public static List<Subscription> parseSubFromStr(String subscriptions) {
    List<Subscription> allSubscriptions =
        Arrays.stream(subscriptions.split(","))
            .map(subscriptionStr -> convertStringToSubscription(subscriptionStr))
            .collect(Collectors.toList());

    return allSubscriptions;
  }

  /**
   * Accepts a comma-delimited string and converts it to a list of Subscription class objects, with
   * exclusions filtered out.
   *
   * @param subscriptions String formatted Subscriptions, comma delimited.
   * @return List of Subscription class objects
   */
  public static List<Subscription> parseSubFromStrWithoutExclusions(String subscriptions) {
    List<Subscription> allSubscriptions =
        Arrays.stream(subscriptions.split(","))
            .map(subscriptionStr -> convertStringToSubscription(subscriptionStr))
            .collect(Collectors.toList());

    allSubscriptions =
        allSubscriptions.stream().filter(sub -> !sub.getExclude()).collect(Collectors.toList());

    return allSubscriptions;
  }

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

    return String.format(
        "%s:%s:%s", subscription.getProject(), subscription.getName(), subscription.getExclude());
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
    if (split.length == 2) {
      // Backward compatibility check
      return Subscription.newBuilder().setProject(split[0]).setName(split[1]).build();
    }
    return Subscription.newBuilder()
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
      List<Subscription> subscriptions, String projectName, String featureSetName) {
    // Case 1: Highest priority check, to exclude all matching subscriptions with excluded flag =
    // true
    for (Subscription sub : subscriptions) {
      // If configuration missing, fail
      if (sub.getProject().isEmpty() || sub.getName().isEmpty()) {
        throw new IllegalArgumentException(
            String.format("Subscription is missing arguments: %s", sub.toString()));
      }
      // Match feature set name to pattern
      Pattern patternName = getNamePattern(sub);
      Pattern patternProject = getProjectPattern(sub);
      // SubCase: Project name and feature set name matches and excluded flag is true
      if (patternProject.matcher(projectName).matches()
          && patternName.matcher(featureSetName).matches()
          && sub.getExclude()) {
        return false;
      }
    }
    // Case 2: Featureset is not excluded, check if it is included in the current subscriptions
    // filteredSubscriptions only contain subscriptions with excluded flag = false
    List<Subscription> filteredSubscriptions =
        subscriptions.stream().filter(sub -> !sub.getExclude()).collect(Collectors.toList());

    for (Subscription filteredSub : filteredSubscriptions) {
      // Match feature set name to pattern
      Pattern patternName = getNamePattern(filteredSub);
      Pattern patternProject = getProjectPattern(filteredSub);
      // SubCase: Project name and feature set name matches
      if (patternProject.matcher(projectName).matches()
          && patternName.matcher(featureSetName).matches()) {
        return true;
      }
    }
    return false;
  }

  private static Pattern getProjectPattern(Subscription subscription) {
    String subProject = subscription.getProject();
    if (!subscription.getProject().contains(".*")) {
      subProject = subProject.replace("*", ".*");
    }

    return Pattern.compile(subProject);
  }

  private static Pattern getNamePattern(Subscription subscription) {
    String subName = subscription.getName();
    if (!subscription.getProject().contains(".*")) {
      subName = subName.replace("*", ".*");
    }

    return Pattern.compile(subName);
  }
}
