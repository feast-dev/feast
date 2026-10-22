/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.serving.connectors;

import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;

public class ProtoFeature implements Feature {
  private final ServingAPIProto.FeatureReferenceV2 featureReference;

  private final Timestamp eventTimestamp;

  private final ValueProto.Value featureValue;

  public ProtoFeature(
      ServingAPIProto.FeatureReferenceV2 featureReference,
      Timestamp eventTimestamp,
      ValueProto.Value featureValue) {
    this.featureReference = featureReference;
    this.eventTimestamp = eventTimestamp;
    this.featureValue = featureValue;
  }

  /**
   * Returns Feast valueType if type matches, otherwise null.
   *
   * @param valueType Feast valueType of feature as specified in FeatureSpec
   * @return ValueProto.Value representation of feature
   */
  @Override
  public ValueProto.Value getFeatureValue(ValueProto.ValueType.Enum valueType) {
    if (TYPE_TO_VAL_CASE.get(valueType) != this.featureValue.getValCase()) {
      return null;
    }

    return this.featureValue;
  }

  @Override
  public ServingAPIProto.FeatureReferenceV2 getFeatureReference() {
    return this.featureReference;
  }

  @Override
  public Timestamp getEventTimestamp() {
    return this.eventTimestamp;
  }
}
