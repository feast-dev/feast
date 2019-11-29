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

  /** Get only feature set specs that matches the subscription */
  public static List<FeatureSetSpec> getSubscribedFeatureSets(
      List<Subscription> subscriptions, List<FeatureSetSpec> featureSetSpecs) {
    List<FeatureSetSpec> subscribed = new ArrayList<>();
    for (FeatureSetSpec featureSet : featureSetSpecs) {
      for (Subscription sub : subscriptions) {
        // Convert wildcard to regex
        String subName = sub.getName();
        if (!sub.getName().contains(".*")) {
          subName = subName.replace("*", ".*");
        }

        // Match feature set name to pattern
        Pattern pattern = Pattern.compile(subName);
        if (!pattern.matcher(featureSet.getName()).matches()) {
          continue;
        }

        // If version is empty, match all
        if (sub.getVersion().isEmpty()) {
          subscribed.add(featureSet);
          break;
        } else if (sub.getVersion().startsWith(">") && sub.getVersion().length() > 1) {
          // if version starts with >, match only those greater than the version number
          int lowerBoundIncl = Integer.parseInt(sub.getVersion().substring(1));
          if (featureSet.getVersion() >= lowerBoundIncl) {
            subscribed.add(featureSet);
            break;
          }
        } else {
          // If a specific version, match that version alone
          int version = Integer.parseInt(sub.getVersion());
          if (featureSet.getVersion() == version) {
            subscribed.add(featureSet);
            break;
          }
        }
      }
    }
    return subscribed;
  }

  public static List<FeatureSetSpec> parseFeatureSetSpecJsonList(List<String> jsonList)
      throws InvalidProtocolBufferException {
    List<FeatureSetSpec> featureSetSpecs = new ArrayList<>();
    for (String json : jsonList) {
      FeatureSetSpec.Builder builder = FeatureSetSpec.newBuilder();
      JsonFormat.parser().merge(json, builder);
      featureSetSpecs.add(builder.build());
    }
    return featureSetSpecs;
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
