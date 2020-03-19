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

public class Metrics {

  public static final Histogram requestLatency =
      Histogram.build()
          .buckets(0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.05)
          .name("request_latency_seconds")
          .subsystem("feast_serving")
          .help("Request latency in seconds")
          .labelNames("method")
          .register();

  public static final Counter requestCount =
      Counter.build()
          .name("request_feature_count")
          .subsystem("feast_serving")
          .help("number of feature rows requested")
          .labelNames("project", "feature_name")
          .register();

  public static final Counter missingKeyCount =
      Counter.build()
          .name("missing_feature_count")
          .subsystem("feast_serving")
          .help("number requested feature rows that were not found")
          .labelNames("project", "feature_name")
          .register();

  public static final Counter invalidEncodingCount =
      Counter.build()
          .name("invalid_encoding_feature_count")
          .subsystem("feast_serving")
          .help("number requested feature rows that were stored with the wrong encoding")
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
