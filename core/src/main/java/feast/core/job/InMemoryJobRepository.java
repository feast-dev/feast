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
package feast.core.job;

import com.google.common.collect.Lists;
import feast.common.models.FeatureSetReference;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.proto.core.SourceProto;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Keeps state of all jobs managed by current application in memory. On start loads persistent state
 * through JobManager from JobManager backend.
 *
 * <p>E.g., with Dataflow runner all jobs with their state stored on Google's side and accessible
 * via API. So we don't need to persist this state ourselves. Instead we just fetch it once and then
 * we apply same changes to dataflow (via JobManager) and to InMemoryRepository to keep them in
 * sync.
 *
 * <p>Provides flexible access to objects via JPA-like filtering API.
 */
@Component
public class InMemoryJobRepository implements JobRepository {
  private final JobManager jobManager;

  /** Internal storage for all jobs mapped by their Id */
  private Map<String, Job> storage;

  @Autowired
  public InMemoryJobRepository(JobManager jobManager) {
    this.jobManager = jobManager;
    this.storage = new HashMap<>();

    this.storage =
        this.jobManager.listRunningJobs().stream().collect(Collectors.toMap(Job::getId, j -> j));
  }

  /**
   * Returns single job that has given source, store with given name ans its status is not in given
   * statuses. We expect this parameters to specify only one RUNNING job (most of the time). But in
   * case there're many - we return latest updated one.
   *
   * @return job that matches given parameters if it's present
   */
  @Override
  public Optional<Job> findFirstBySourceAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
      SourceProto.Source source, String storeName, Collection<JobStatus> statuses) {
    return this.storage.values().stream()
        .filter(
            j ->
                j.getSource().equals(source)
                    && (storeName == null || j.getStores().containsKey(storeName))
                    && (!statuses.contains(j.getStatus())))
        .max(Comparator.comparing(Job::getLastUpdated));
  }

  private List<Job> findWithFilter(Predicate<Job> p) {
    return this.storage.values().stream().filter(p).collect(Collectors.toList());
  }

  /** Find Jobs that have given status */
  @Override
  public List<Job> findByStatus(JobStatus status) {
    return this.findWithFilter(j -> j.getStatus().equals(status));
  }

  /**
   * Find Jobs that have given FeatureSet (specified by {@link FeatureSetReference} allocated to it.
   */
  @Override
  public List<Job> findByFeatureSetReference(FeatureSetReference reference) {
    return this.findWithFilter(j -> j.getFeatureSetDeliveryStatuses().containsKey(reference));
  }

  /** Find Jobs that have one of the stores with given name */
  @Override
  public List<Job> findByJobStoreName(String storeName) {
    return this.findWithFilter(j -> j.getStores().containsKey(storeName));
  }

  /** Find by Job's Id */
  @Override
  public Optional<Job> findById(String jobId) {
    return Optional.ofNullable(this.storage.get(jobId));
  }

  @Override
  public List<Job> findAll() {
    return Lists.newArrayList(this.storage.values());
  }

  @Override
  public void add(Job job) {
    job.preSave();

    this.storage.put(job.getId(), job);
  }

  @Override
  public void deleteAll() {
    this.storage.clear();
  }
}
