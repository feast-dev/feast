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
package feast.core.config;

import feast.core.dao.FeatureSetRepository;
import feast.core.dao.StoreRepository;
import feast.core.metrics.collector.FeastResourceCollector;
import feast.core.metrics.collector.JVMResourceCollector;
import io.prometheus.client.exporter.MetricsServlet;
import javax.servlet.http.HttpServlet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MonitoringConfig {

  private static final String PROMETHEUS_METRICS_PATH = "/metrics";

  /**
   * Add Prometheus exposition to an existing HTTP server using servlets.
   *
   * <p>https://github.com/prometheus/client_java/tree/b61dd232a504e20dad404a2bf3e2c0b8661c212a#http
   *
   * @return HTTP servlet for returning metrics data
   */
  @Bean
  public ServletRegistrationBean<HttpServlet> metricsServlet() {
    return new ServletRegistrationBean<>(new MetricsServlet(), PROMETHEUS_METRICS_PATH);
  }

  /**
   * Register custom Prometheus collector that exports metrics about Feast Resources.
   *
   * <p>For example: total number of registered feature sets and stores.
   *
   * @param featureSetRepository {@link FeatureSetRepository}
   * @param storeRepository {@link StoreRepository}
   * @return {@link FeastResourceCollector}
   */
  @Bean
  @Autowired
  public FeastResourceCollector feastResourceCollector(
      FeatureSetRepository featureSetRepository, StoreRepository storeRepository) {
    FeastResourceCollector collector =
        new FeastResourceCollector(featureSetRepository, storeRepository);
    collector.register();
    return collector;
  }

  /**
   * Register custom Prometheus collector that exports metrics about JVM resource usage.
   *
   * @return {@link JVMResourceCollector}
   */
  @Bean
  public JVMResourceCollector jvmResourceCollector() {
    JVMResourceCollector jvmResourceCollector = new JVMResourceCollector();
    jvmResourceCollector.register();
    return jvmResourceCollector;
  }
}
