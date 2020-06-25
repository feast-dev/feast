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

import feast.core.model.Job;
import feast.core.model.Source;
import feast.core.model.Store;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Strategy interface that defines how responsibility for sources and stores will be distributed
 * across Ingestion Jobs.
 */
public interface JobGroupingStrategy {
  /** Get the non terminated ingestion job ingesting for given source and stores. */
  public Job getOrCreateJob(Source source, Set<Store> stores);
  /** Create unique JobId that would be used as key in communications with JobRunner */
  public String createJobId(Job job);
  /* Distribute given sources and stores across jobs. One yielded Pair - one created Job **/
  public Iterable<Pair<Source, Set<Store>>> collectSingleJobInput(
      Stream<Pair<Source, Store>> stream);
}
