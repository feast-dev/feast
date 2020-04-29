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
package feast.core.validators;

import static feast.core.validators.Matchers.checkValidCharacters;

import com.google.common.collect.Sets;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSpec;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class FeatureSetValidator {

  public static void validateSpec(FeatureSet featureSet) {
    if (featureSet.getSpec().getProject().isEmpty()) {
      throw new IllegalArgumentException("Project name must be provided");
    }
    if (featureSet.getSpec().getName().isEmpty()) {
      throw new IllegalArgumentException("Feature set name must be provided");
    }
    if (featureSet.getSpec().getLabelsMap().containsKey("")) {
      throw new IllegalArgumentException("Feature set label keys must not be empty");
    }

    checkValidCharacters(featureSet.getSpec().getProject(), "project");
    checkValidCharacters(featureSet.getSpec().getName(), "name");
    checkUniqueColumns(
        featureSet.getSpec().getEntitiesList(), featureSet.getSpec().getFeaturesList());
    for (EntitySpec entitySpec : featureSet.getSpec().getEntitiesList()) {
      checkValidCharacters(entitySpec.getName(), "entities::name");
    }
    for (FeatureSpec featureSpec : featureSet.getSpec().getFeaturesList()) {
      checkValidCharacters(featureSpec.getName(), "features::name");
      if (featureSpec.getLabelsMap().containsKey("")) {
        throw new IllegalArgumentException("Feature label keys must not be empty");
      }
    }
  }

  private static void checkUniqueColumns(
      List<EntitySpec> entitySpecs, List<FeatureSpec> featureSpecs) {
    List<String> names = entitySpecs.stream().map(EntitySpec::getName).collect(Collectors.toList());
    featureSpecs.stream().map(f -> names.add(f.getName()));
    HashSet<String> nameSet = Sets.newHashSet(names);
    if (nameSet.size() != names.size()) {
      throw new IllegalArgumentException(
          String.format("fields within a featureset must be unique."));
    }
  }
}
