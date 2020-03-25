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
package feast.ingestion.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import feast.ingestion.values.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SpecUtil {

  public static String getFeatureSetReference(FeatureSetSpec featureSetSpec) {
    return String.format(
        "%s/%s:%d",
        featureSetSpec.getProject(), featureSetSpec.getName(), featureSetSpec.getVersion());
  }

  /** Get only feature set specs that matches the subscription */
  public static List<FeatureSet> getSubscribedFeatureSets(
      List<Subscription> subscriptions, List<FeatureSet> featureSets) {
    List<FeatureSet> subscribed = new ArrayList<>();
    for (FeatureSet featureSet : featureSets) {
      for (Subscription sub : subscriptions) {
        // If configuration missing, fail
        if (sub.getProject().isEmpty() || sub.getName().isEmpty() || sub.getVersion().isEmpty()) {
          throw new IllegalArgumentException(
              String.format("Subscription is missing arguments: %s", sub.toString()));
        }

        // If all wildcards, subscribe to everything
        if (sub.getProject().equals("*")
            || sub.getName().equals("*")
            || sub.getVersion().equals("*")) {
          subscribed.add(featureSet);
          break;
        }

        // If all wildcards, subscribe to everything
        if (sub.getProject().equals("*")
            && (!sub.getName().equals("*") || !sub.getVersion().equals("*"))) {
          throw new IllegalArgumentException(
              String.format(
                  "Subscription cannot have feature set name and/or version set if project is not defined: %s",
                  sub.toString()));
        }

        // Match project name
        if (!featureSet.getSpec().getProject().equals(sub.getProject())) {
          continue;
        }

        // Convert wildcard to regex
        String subName = sub.getName();
        if (!sub.getName().contains(".*")) {
          subName = subName.replace("*", ".*");
        }

        // Match feature set name to pattern
        Pattern pattern = Pattern.compile(subName);
        if (!pattern.matcher(featureSet.getSpec().getName()).matches()) {
          continue;
        }

        // If version is '*', match all
        if (sub.getVersion().equals("*")) {
          subscribed.add(featureSet);
          break;
        } else if (sub.getVersion().equals("latest")) {
          // if version is "latest"
          throw new RuntimeException(
              String.format(
                  "Support for latest feature set subscription has not been implemented yet: %s",
                  sub.toString()));

        } else {
          // If a specific version, match that version alone
          int version = Integer.parseInt(sub.getVersion());
          if (featureSet.getSpec().getVersion() == version) {
            subscribed.add(featureSet);
            break;
          }
        }
      }
    }
    return subscribed;
  }

  public static List<FeatureSet> parseFeatureSetSpecJsonList(List<String> jsonList)
      throws InvalidProtocolBufferException {
    List<FeatureSet> featureSets = new ArrayList<>();
    for (String json : jsonList) {
      FeatureSetSpec.Builder builder = FeatureSetSpec.newBuilder();
      JsonFormat.parser().merge(json, builder);
      featureSets.add(FeatureSet.newBuilder().setSpec(builder.build()).build());
    }
    return featureSets;
  }

  public static List<Store> parseStoreJsonList(List<String> jsonList)
      throws InvalidProtocolBufferException {
    List<Store> stores = new ArrayList<>();
    for (String json : jsonList) {
      Store.Builder builder = Store.newBuilder();
      JsonFormat.parser().merge(json, builder);
      stores.add(builder.build());
    }
    return stores;
  }

  public static Map<String, Field> getFieldsByName(FeatureSetSpec featureSetSpec) {
    Map<String, Field> fieldByName = new HashMap<>();
    for (EntitySpec entitySpec : featureSetSpec.getEntitiesList()) {
      fieldByName.put(
          entitySpec.getName(), new Field(entitySpec.getName(), entitySpec.getValueType()));
    }
    for (FeatureSpec featureSpec : featureSetSpec.getFeaturesList()) {
      fieldByName.put(
          featureSpec.getName(), new Field(featureSpec.getName(), featureSpec.getValueType()));
    }
    return fieldByName;
  }
}
