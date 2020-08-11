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
package feast.core.service;

import com.google.common.collect.Lists;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import java.util.*;

public class FakeJobManager implements JobManager {
  private final Map<String, Job> state;

  public FakeJobManager() {
    state = new HashMap<>();
  }

  @Override
  public Runner getRunnerType() {
    return Runner.DIRECT;
  }

  @Override
  public Job startJob(Job job) {
    String extId = UUID.randomUUID().toString();
    job.setExtId(extId);
    job.setStatus(JobStatus.RUNNING);
    state.put(extId, job);
    return job;
  }

  @Override
  public Job updateJob(Job job) {
    return job;
  }

  @Override
  public Job abortJob(Job job) {
    job.setStatus(JobStatus.ABORTING);
    state.remove(job.getExtId());
    return job;
  }

  @Override
  public Job restartJob(Job job) {
    return abortJob(job);
  }

  @Override
  public JobStatus getJobStatus(Job job) {
    if (state.containsKey(job.getExtId())) {
      return JobStatus.RUNNING;
    }

    return JobStatus.ABORTED;
  }

  @Override
  public List<Job> listRunningJobs() {
    return Collections.emptyList();
  }

  public List<Job> getAllJobs() {
    return Lists.newArrayList(state.values());
  }

  public void cleanAll() {
    state.clear();
  }
}
