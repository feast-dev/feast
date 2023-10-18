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
package feast.serving.service.config;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.TracingServerInterceptor;
import io.opentracing.noop.NoopTracerFactory;

public class InstrumentationConfigModule extends AbstractModule {

  @Provides
  public Tracer tracer(ApplicationProperties applicationProperties) {
    if (!applicationProperties.getFeast().getTracing().isEnabled()) {
      return NoopTracerFactory.create();
    }

    if (!applicationProperties.getFeast().getTracing().getTracerName().equalsIgnoreCase("jaeger")) {
      throw new IllegalArgumentException("Only 'jaeger' tracer is supported for now.");
    }

    return io.jaegertracing.Configuration.fromEnv(
            applicationProperties.getFeast().getTracing().getServiceName())
        .getTracer();
  }

  @Provides
  public TracingServerInterceptor tracingInterceptor(Tracer tracer) {
    return TracingServerInterceptor.newBuilder().withTracer(tracer).build();
  }
}
