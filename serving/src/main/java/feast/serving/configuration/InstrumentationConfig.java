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
