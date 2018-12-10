/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.job.dataflow;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricUpdate;
import feast.core.job.JobMonitor;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.model.Metrics;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataflowJobMonitor implements JobMonitor {

  private final String projectId;
  private final String location;
  private final Dataflow dataflow;
  private final DataflowJobStateMapper jobStateMaper;

  private static final String METRICS_NAMESPACE_KEY = "namespace";
  private static final String FEAST_METRICS_NAMESPACE = "feast";

  public DataflowJobMonitor(Dataflow dataflow, String projectId, String location) {
    checkNotNull(projectId);
    checkNotNull(location);
    this.projectId = projectId;
    this.location = location;
    this.dataflow = dataflow;
    this.jobStateMaper = new DataflowJobStateMapper();
  }

  /**
   * Get status of a dataflow job with given id and try to map it into Feast's JobStatus.
   *
   * @param dataflowJobId dataflow job id.
   * @return status of the job, or return {@link JobStatus#UNKNOWN} if error happens.
   */
  public JobStatus getJobStatus(String dataflowJobId) {
    try {
      Job job = dataflow.projects().locations().jobs().get(projectId, location, dataflowJobId)
          .execute();
      return jobStateMaper.map(job.getCurrentState());
    } catch (Exception e) {
      log.error("Unable to retrieve status of a dataflow job with id : {}\ncause: {}",
          dataflowJobId, e.getMessage());
    }
    return JobStatus.UNKNOWN;
  }


  /**
   * Get list of feast-related metrics of a dataflow job given the {@code dataflowJobId}.
   *
   * @param job job instance.
   * @return list of feast-related metrics. Or return an empty list if error happens.
   */
  public List<Metrics> getJobMetrics(JobInfo job) {
    String dataflowJobId = job.getExtId();
    try {
      JobMetrics jobMetrics = dataflow.projects().locations().jobs()
          .getMetrics(projectId, location, dataflowJobId).execute();
      return jobMetrics.getMetrics().stream()
          // only get feast metrics
          .filter(m -> FEAST_METRICS_NAMESPACE
              .equals(m.getName().getContext().get(METRICS_NAMESPACE_KEY)))
          // convert to internal feast metric structure.
          .map(m -> mapDataflowMetricsToFeastMetrics(job, m))
          .filter(m -> m != null)
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Unable to retrieve metrics for job with id: {}\ncause: {}", dataflowJobId, e);
    }
    return Collections.emptyList();
  }

  private Metrics mapDataflowMetricsToFeastMetrics(JobInfo job, MetricUpdate dfMetrics) {
    String name = dfMetrics.getName().getName();
    if (dfMetrics.getScalar() == null) {
      return null;
    }
    double value = ((Number) dfMetrics.getScalar()).doubleValue();
    Metrics feastMetric = new Metrics(job, name, value);
    return feastMetric;
  }
}
