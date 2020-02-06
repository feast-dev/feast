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
package com.gojek.feast;

import feast.serving.ServingAPIProto.FeatureReference;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("WeakerAccess")
public class RequestUtil {

  public static List<FeatureReference> createFeatureRefs(
      List<String> featureRefStrings, String defaultProject) {
    if (featureRefStrings == null) {
      throw new IllegalArgumentException("featureRefs cannot be null");
    }

    List<FeatureReference> featureRefs = new ArrayList<>();

    for (String featureRefString : featureRefStrings) {
      String project;
      String name;
      int version = 0;
      String[] featureSplit;
      String[] projectSplit = featureRefString.split("/");

      if (projectSplit.length == 2) {
        project = projectSplit[0];
        featureSplit = projectSplit[1].split(":");
      } else if (projectSplit.length == 1) {
        project = defaultProject;
        featureSplit = projectSplit[0].split(":");
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Feature id '%s' has invalid format. Expected format: <project>:<feature-name>:<feature-version>.",
                featureRefString));
      }

      if (featureSplit.length == 2) {
        name = featureSplit[0];
        try {
          version = Integer.parseInt(featureSplit[1]);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              String.format(
                  "Feature id '%s' contains invalid version. Expected format: <project>/<feature-name>:<feature-version>.",
                  featureRefString));
        }
      } else if (featureSplit.length == 1) {
        name = featureSplit[0];
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Feature id '%s' has invalid format. Expected format: <project>/<feature-name>:<feature-version>.",
                featureRefString));
      }

      if (project.isEmpty() || name.isEmpty() || version < 0) {
        throw new IllegalArgumentException(
            String.format(
                "Feature id '%s' has invalid format. Expected format: <project>/<feature-name>:<feature-version>.",
                featureRefString));
      }

      featureRefs.add(
          FeatureReference.newBuilder()
              .setName(name)
              .setProject(project)
              .setVersion(version)
              .build());
    }

    ;
    return featureRefs;
  }
}
