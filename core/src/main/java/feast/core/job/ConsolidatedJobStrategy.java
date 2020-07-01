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

import feast.core.dao.JobRepository;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;

/**
 * In this strategy one Ingestion Job per source is created. All stores that subscribed to
 * FeatureSets from this source will be included as sinks in this consolidated Job.
 *
 * <p>JobId will contain only source parameters (type + config). StoreName will remain empty in Job
 * table.
 */
public class ConsolidatedJobStrategy implements JobGroupingStrategy {
  private final JobRepository jobRepository;

  public ConsolidatedJobStrategy(JobRepository jobRepository) {
    this.jobRepository = jobRepository;
  }

  @Override
  public Job getOrCreateJob(Source source, Set<Store> stores) {
    return jobRepository
        .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
            source.getType(), source.getConfig(), null, JobStatus.getTerminalStates())
        .orElseGet(
            () -> {
              Job job =
                  Job.builder().setSource(source).setFeatureSetJobStatuses(new HashSet<>()).build();
              job.setStores(stores);
              return job;
            });
  }

  @Override
  public String createJobId(Job job) {
    String dateSuffix = String.valueOf(Instant.now().toEpochMilli());
    String jobId =
        String.format(
            "%s-%d-%s",
            job.getSource().getTypeString(),
            Objects.hashCode(job.getSource().getConfig()),
            dateSuffix);
    return jobId.replaceAll("_store", "-").toLowerCase();
  }

  @Override
  public Iterable<Pair<Source, Set<Store>>> collectSingleJobInput(
      Stream<Pair<Source, Store>> stream) {
    Map<Source, Set<Store>> map =
        stream.collect(
            Collectors.groupingBy(
                Pair::getLeft, Collectors.mapping(Pair::getRight, Collectors.toSet())));

    return map.entrySet().stream()
        .map(e -> Pair.of(e.getKey(), e.getValue()))
        .collect(Collectors.toList());
  }
}
