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
import feast.proto.core.FeatureSetProto.FeatureSetJobDeliveryStatus;
import java.io.Serializable;
import javax.persistence.*;
import javax.persistence.Entity;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(
    name = "jobs_feature_sets",
    indexes = {
      @Index(name = "idx_jobs_feature_sets_job_id", columnList = "job_id"),
      @Index(name = "idx_jobs_feature_sets_feature_sets_id", columnList = "feature_sets_id")
    })
public class FeatureSetJobStatus {
  @Embeddable
  @EqualsAndHashCode
  @AllArgsConstructor
  public static class FeatureSetJobStatusKey implements Serializable {
    public FeatureSetJobStatusKey() {}

    @Column(name = "job_id")
    String jobId;

    @Column(name = "feature_sets_id")
    long featureSetId;
  }

  @EmbeddedId private FeatureSetJobStatusKey id = new FeatureSetJobStatusKey();

  @ManyToOne
  @MapsId("jobId")
  @JoinColumn(name = "job_id")
  private Job job;

  @ManyToOne
  @MapsId("featureSetId")
  @JoinColumn(name = "feature_sets_id")
  private FeatureSet featureSet;

  @Enumerated(EnumType.STRING)
  @Column(name = "delivery_status")
  private FeatureSetJobDeliveryStatus deliveryStatus;

  @Column(name = "version", columnDefinition = "integer default 0")
  private int version;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FeatureSetJobStatus that = (FeatureSetJobStatus) o;
    return version == that.version
        && Objects.equal(job.getId(), that.job.getId())
        && Objects.equal(featureSet.getReference(), that.featureSet.getReference())
        && deliveryStatus == that.deliveryStatus;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(job, featureSet);
  }
}
