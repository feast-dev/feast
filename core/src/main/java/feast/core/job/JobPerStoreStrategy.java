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
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
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
  public Job getOrCreateJob(
      SourceProto.Source source, Set<StoreProto.Store> stores, Map<String, String> labels) {
    ArrayList<StoreProto.Store> storesList = Lists.newArrayList(stores);
    if (storesList.size() != 1) {
      throw new RuntimeException("Only one store is acceptable in JobPerStore Strategy");
    }
    StoreProto.Store store = storesList.get(0);

    return jobRepository
        .findFirstBySourceAndStoreNameAndStatusNotInOrderByLastUpdatedDesc(
            source, store.getName(), JobStatus.getTerminalStates())
        .orElseGet(
            () ->
                Job.builder()
                    .setId(createJobId(source, stores))
                    .setSource(source)
                    .setStores(
                        stores.stream()
                            .collect(Collectors.toMap(StoreProto.Store::getName, s -> s)))
                    .setLabels(labels)
                    .build());
  }

  private String createJobId(SourceProto.Source source, Iterable<StoreProto.Store> stores) {
    String dateSuffix = String.valueOf(Instant.now().toEpochMilli());
    String jobId =
        String.format(
            "%s-%d-to-%s-%s",
            source.getType().getValueDescriptor().getName(),
            Objects.hash(
                source.getKafkaSourceConfig().getBootstrapServers(),
                source.getKafkaSourceConfig().getTopic()),
            Lists.newArrayList(stores).get(0).getName(),
            dateSuffix);
    return jobId.replaceAll("_store", "-").toLowerCase();
  }

  @Override
  public String createJobId(Job job) {
    return createJobId(job.getSource(), job.getStores().values());
  }

  @Override
  public Iterable<Pair<SourceProto.Source, Set<StoreProto.Store>>> collectSingleJobInput(
      Stream<Pair<SourceProto.Source, StoreProto.Store>> stream) {
    return stream.map(p -> Pair.of(p.getLeft(), Set.of(p.getRight()))).collect(Collectors.toList());
  }
}
