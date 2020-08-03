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
 * <p>Provides flexible access to objects via JPA-like filtering API.
 */
@Component
public class InMemoryJobRepository implements JobRepository {
  private final JobManager jobManager;

  private Map<String, Job> storage;

  @Autowired
  public InMemoryJobRepository(JobManager jobManager) {
    this.jobManager = jobManager;
    this.storage = new HashMap<>();

    this.storage =
        this.jobManager.listJobs().stream().collect(Collectors.toMap(Job::getId, j -> j));
  }

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

  @Override
  public List<Job> findByStatus(JobStatus status) {
    return this.findWithFilter(j -> j.getStatus().equals(status));
  }

  @Override
  public List<Job> findByFeatureSetReference(FeatureSetReference reference) {
    return this.findWithFilter(j -> j.getFeatureSetDeliveryStatuses().containsKey(reference));
  }

  @Override
  public List<Job> findByJobStoreName(String storeName) {
    return this.findWithFilter(j -> j.getStores().containsKey(storeName));
  }

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
