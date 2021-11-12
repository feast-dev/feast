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

import static feast.core.validators.Matchers.*;

import feast.proto.core.DataSourceProto.DataSource.SourceType;
import feast.proto.core.FeatureProto.FeatureSpecV2;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class FeatureTableValidator {
  protected static final Set<String> RESERVED_NAMES =
      Set.of("created_timestamp", "event_timestamp");

  public static void validateSpec(FeatureTableSpec spec) {
    if (spec.getName().isEmpty()) {
      throw new IllegalArgumentException("FeatureTable name must be provided");
    }
    if (spec.getLabelsMap().containsKey("")) {
      throw new IllegalArgumentException("FeatureTable cannot have labels with empty key.");
    }
    if (spec.getEntitiesCount() == 0) {
      throw new IllegalArgumentException("FeatureTable entities list cannot be empty.");
    }
    if (spec.getFeaturesCount() == 0) {
      throw new IllegalArgumentException("FeatureTable features list cannot be empty.");
    }
    if (!spec.hasBatchSource()) {
      throw new IllegalArgumentException("FeatureTable batch source cannot be empty.");
    }

    checkValidCharacters(spec.getName(), "FeatureTable");
    spec.getFeaturesList().forEach(FeatureTableValidator::validateFeatureSpec);

    // Check that features and entities defined in FeatureTable do not use reserved names
    ArrayList<String> fieldNames = new ArrayList<>(spec.getEntitiesList());
    fieldNames.addAll(
        spec.getFeaturesList().stream().map(FeatureSpecV2::getName).collect(Collectors.toList()));
    if (!Collections.disjoint(fieldNames, RESERVED_NAMES)) {
      throw new IllegalArgumentException(
          String.format(
              "Reserved names has been used as Feature(s) names. Reserved: %s", RESERVED_NAMES));
    }

    // Check that Feature and Entity names in FeatureTable do not collide with each other
    if (hasDuplicates(fieldNames)) {
      throw new IllegalArgumentException(
          String.format("Entity and Feature names within a Feature Table should be unique."));
    }

    // Check that the data sources defined in the feature table are valid
    if (!spec.getBatchSource().getType().equals(SourceType.INVALID)) {
      DataSourceValidator.validate(spec.getBatchSource());
    }
    if (!spec.getStreamSource().getType().equals(SourceType.INVALID)) {
      DataSourceValidator.validate(spec.getStreamSource());
    }
  }

  private static void validateFeatureSpec(FeatureSpecV2 spec) {
    checkValidCharacters(spec.getName(), "Feature");
    if (spec.getLabelsMap().containsKey("")) {
      throw new IllegalArgumentException("Features cannot have labels with empty key.");
    }
  }
}
