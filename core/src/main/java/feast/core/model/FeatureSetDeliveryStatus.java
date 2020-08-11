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
package feast.core.model;

import com.google.common.base.Objects;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto.FeatureSetJobDeliveryStatus;
import lombok.Getter;
import lombok.Setter;

/**
 * Data class that represents connection between {@link Job} and FeatureSet. For all FeatureSets
 * allocated to Job FeatureSetDeliveryStatus must be created and added to Job's
 * featureSetDeliveryStatuses map. FeatureSet is determined by {@link FeatureSetReference}. Stores
 * delivery status and latest delivered version.
 */
@Getter
@Setter
public class FeatureSetDeliveryStatus {
  private FeatureSetReference featureSetReference;
  private FeatureSetJobDeliveryStatus deliveryStatus;
  private int deliveredVersion;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FeatureSetDeliveryStatus that = (FeatureSetDeliveryStatus) o;
    return deliveredVersion == that.deliveredVersion
        && Objects.equal(this.featureSetReference, that.featureSetReference)
        && deliveryStatus == that.deliveryStatus;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.featureSetReference);
  }
}
