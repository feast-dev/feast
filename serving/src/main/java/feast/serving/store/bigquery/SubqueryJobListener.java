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
package feast.serving.store.bigquery;

import com.google.auto.value.AutoValue;
import feast.serving.ServingAPIProto;
import feast.serving.ServingAPIProto.JobStatus;
import feast.serving.ServingAPIProto.JobType;
import feast.serving.service.JobService;
import feast.serving.store.bigquery.model.FeatureSetInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@AutoValue
public abstract class SubqueryJobListener {

  public abstract JobService jobService();

  public abstract String feastJobId();

  public abstract List<FeatureSetInfo> updatedFeatureSetInfos();

  public static Builder builder() {
    return new AutoValue_SubqueryJobListener.Builder()
        .setUpdatedFeatureSetInfos(Collections.synchronizedList(new ArrayList<FeatureSetInfo>()));
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setJobService(JobService jobService);

    public abstract Builder setFeastJobId(String feastJobId);

    public abstract Builder setUpdatedFeatureSetInfos(List<FeatureSetInfo> updatedFeatureSetInfos);

    public abstract SubqueryJobListener build();
  }

  public int subqueriesComplete() {
    List<FeatureSetInfo> featureSetInfos = updatedFeatureSetInfos();
    synchronized (featureSetInfos) {
      return featureSetInfos.size();
    }
  }

  public void callbackJobSuccess(FeatureSetInfo updatedFeatureSetInfo) {
    updatedFeatureSetInfos().add(updatedFeatureSetInfo);
  }

  public void callbackError(Throwable e) {
    jobService()
        .upsert(
            ServingAPIProto.Job.newBuilder()
                .setId(feastJobId())
                .setType(JobType.JOB_TYPE_DOWNLOAD)
                .setStatus(JobStatus.JOB_STATUS_DONE)
                .setError(e.getMessage())
                .build());
  }
}
