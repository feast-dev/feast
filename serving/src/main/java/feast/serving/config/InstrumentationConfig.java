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
package feast.serving.config;

import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InstrumentationConfig {

  private FeastProperties feastProperties;

  @Autowired
  public InstrumentationConfig(FeastProperties feastProperties) {
    this.feastProperties = feastProperties;
  }

  @Bean
  public ServletRegistrationBean servletRegistrationBean() {
    DefaultExports.initialize();
    return new ServletRegistrationBean(new MetricsServlet(), "/metrics");
  }

  @Bean
  public Tracer tracer() {
    if (!feastProperties.getTracing().isEnabled()) {
      return NoopTracerFactory.create();
    }

    if (!feastProperties.getTracing().getTracerName().equalsIgnoreCase("jaeger")) {
      throw new IllegalArgumentException("Only 'jaeger' tracer is supported for now.");
    }

    return io.jaegertracing.Configuration.fromEnv(feastProperties.getTracing().getServiceName())
        .getTracer();
  }
}
