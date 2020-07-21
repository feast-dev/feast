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
import feast.core.dao.JobRepository;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;

/**
 * In this strategy one job per Source-Store pair is created.
 *
 * <p>JobId is generated accordingly from Source (type+config) and StoreName.
 */
public class JobPerStoreStrategy implements JobGroupingStrategy {
  private final JobRepository jobRepository;

  public JobPerStoreStrategy(JobRepository jobRepository) {
    this.jobRepository = jobRepository;
  }

  @Override
  public Job getOrCreateJob(Source source, Set<Store> stores) {
    ArrayList<Store> storesList = Lists.newArrayList(stores);
    if (storesList.size() != 1) {
      throw new RuntimeException("Only one store is acceptable in JobPerStore Strategy");
    }
    Store store = storesList.get(0);

    return jobRepository
        .findFirstBySourceTypeAndSourceConfigAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
            source.getType(), source.getConfig(), store.getName(), JobStatus.getTerminalStates())
        .orElseGet(
            () -> {
              Job job =
                  Job.builder()
                      .setSource(source)
                      .setStoreName(store.getName())
                      .setFeatureSetJobStatuses(new HashSet<>())
                      .build();
              job.setStores(stores);
              return job;
            });
  }

  @Override
  public String createJobId(Job job) {
    String dateSuffix = String.valueOf(Instant.now().toEpochMilli());
    String jobId =
        String.format(
            "%s-%d-to-%s-%s",
            job.getSource().getTypeString(),
            Objects.hashCode(job.getSource().getConfig()),
            job.getStoreName(),
            dateSuffix);
    return jobId.replaceAll("_store", "-").toLowerCase();
  }

  @Override
  public Iterable<Pair<Source, Set<Store>>> collectSingleJobInput(
      Stream<Pair<Source, Store>> stream) {
    return stream.map(p -> Pair.of(p.getLeft(), Set.of(p.getRight()))).collect(Collectors.toList());
  }
}
