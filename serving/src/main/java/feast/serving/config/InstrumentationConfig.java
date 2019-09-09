/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.serving.config;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InstrumentationConfig {
  private static final String APP_NAME = "feast_serving";

  @Bean
  public StatsDClient getStatsDClient(@Value("${statsd.host}") String host,
      @Value("${statsd.port}") int port) {
    return new NonBlockingStatsDClient(APP_NAME, host, port);
  }

  @Bean
  public Tracer getTracer() {
    io.jaegertracing.Configuration tracingConfig =
        io.jaegertracing.Configuration.fromEnv(APP_NAME);
    Tracer tracer = tracingConfig.getTracer();
    GlobalTracer.registerIfAbsent(tracer);
    return tracer;
  }

  @Bean
  MeterRegistryCustomizer<MeterRegistry> metricsCommonTags() {
    return registry -> {
      try {
        registry.config().commonTags("hostname", InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e) {
        registry.config().commonTags("hostname", APP_NAME);
      }
    };
  }
}