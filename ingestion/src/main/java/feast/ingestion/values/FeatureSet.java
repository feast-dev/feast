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
package feast.ingestion.values;

import static feast.ingestion.utils.SpecUtil.getFeatureSetReference;
import static feast.ingestion.utils.SpecUtil.getFieldsByName;

import feast.core.FeatureSetProto;
import java.io.Serializable;
import java.util.Map;

/**
 * This class represents {@link feast.core.FeatureSetProto.FeatureSetSpec} but contains fields
 * directly accessible by name for feature validation purposes.
 *
 * <p>The use for this class is mainly for validating the Fields in FeatureRow.
 */
public class FeatureSet implements Serializable {
  private final String reference;

  private final Map<String, Field> fields;

  public FeatureSet(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    this.reference = getFeatureSetReference(featureSetSpec);
    this.fields = getFieldsByName(featureSetSpec);
  }

  public String getReference() {
    return reference;
  }

  public Field getField(String fieldName) {
    return fields.getOrDefault(fieldName, null);
  }
}
