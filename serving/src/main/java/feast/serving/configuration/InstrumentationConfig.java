package feast.serving.configuration;

import feast.serving.FeastProperties;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
  public Tracer getTracer() {
    if (!feastProperties.isTracingEnabled()) {
      return NoopTracerFactory.create();
    }

    if (!feastProperties.getTracingTracerName().equalsIgnoreCase("jaeger")) {
      throw new IllegalArgumentException("Only 'jaeger' tracer is supported for now.");
    }

    return io.jaegertracing.Configuration.fromEnv(feastProperties.getTracingServiceName())
        .getTracer();
  }
}
