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
package feast.jobcontroller.model;

import com.google.common.base.Objects;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto.FeatureSetJobDeliveryStatus;

/**
 * Data class that represents connection between {@link Job} and FeatureSet. For all FeatureSets
 * allocated to Job FeatureSetDeliveryStatus must be created and added to Job's
 * featureSetDeliveryStatuses map. FeatureSet is determined by {@link FeatureSetReference}. Stores
 * delivery status and latest delivered version.
 */
public class FeatureSetDeliveryStatus {
  private final FeatureSetReference featureSetReference;
  private FeatureSetJobDeliveryStatus deliveryStatus;
  private int deliveredVersion;

  public FeatureSetDeliveryStatus(FeatureSetReference featureSetReference) {
    this.featureSetReference = featureSetReference;
  }

  public FeatureSetDeliveryStatus setDeliveryStatus(FeatureSetJobDeliveryStatus deliveryStatus) {
    this.deliveryStatus = deliveryStatus;
    return this;
  }

  public FeatureSetDeliveryStatus setDeliveredVersion(int deliveredVersion) {
    this.deliveredVersion = deliveredVersion;
    return this;
  }

  public FeatureSetJobDeliveryStatus getDeliveryStatus() {
    return deliveryStatus;
  }

  public FeatureSetReference getFeatureSetReference() {
    return featureSetReference;
  }

  public int getDeliveredVersion() {
    return deliveredVersion;
  }

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
