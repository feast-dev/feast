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
package feast.serving.util;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

// TODO: send these metrics either via Prometheus push gateway or StatsD
public class Metrics {

  public static final Histogram requestLatency =
      Histogram.build()
          .name("request_latency_seconds")
          .subsystem("feast_serving")
          .help("Request latency in seconds")
          .labelNames("method", "project")
          .register();

  public static final Histogram requestEntityCountDistribution =
      Histogram.build()
          .buckets(1, 2, 5, 10, 20, 50, 100, 200)
          .name("request_entity_count_distribution")
          .subsystem("feast_serving")
          .help("Number of entity rows per request")
          .labelNames("project")
          .register();

  public static final Histogram requestFeatureCountDistribution =
      Histogram.build()
          .buckets(1, 2, 5, 10, 15, 20, 30, 50)
          .name("request_feature_count_distribution")
          .subsystem("feast_serving")
          .help("Number of feature rows per request")
          .labelNames("project")
          .register();

  public static final Histogram requestFeatureTableCountDistribution =
      Histogram.build()
          .buckets(1, 2, 5, 10, 20)
          .name("request_feature_table_count_distribution")
          .subsystem("feast_serving")
          .help("Number of feature tables per request")
          .labelNames("project")
          .register();

  public static final Counter requestFeatureCount =
      Counter.build()
          .name("request_feature_count")
          .subsystem("feast_serving")
          .help("number of feature rows requested")
          .labelNames("project", "feature_name")
          .register();

  public static final Counter notFoundKeyCount =
      Counter.build()
          .name("not_found_feature_count")
          .subsystem("feast_serving")
          .help("number requested feature rows that were not found")
          .labelNames("project", "feature_name")
          .register();

  public static final Counter staleKeyCount =
      Counter.build()
          .name("stale_feature_count")
          .subsystem("feast_serving")
          .help("number requested feature rows that were stale")
          .labelNames("project", "feature_name")
          .register();

  public static final Counter grpcRequestCount =
      Counter.build()
          .name("grpc_request_count")
          .subsystem("feast_serving")
          .help("number of grpc requests served")
          .labelNames("method", "status_code")
          .register();
}
