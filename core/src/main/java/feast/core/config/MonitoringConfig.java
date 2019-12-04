package feast.core.config;

import io.prometheus.client.exporter.MetricsServlet;
import javax.servlet.http.HttpServlet;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MonitoringConfig {
  private static final String PROMETHEUS_METRICS_PATH = "/metrics";

  @Bean
  public ServletRegistrationBean<HttpServlet> metricsServlet() {
    return new ServletRegistrationBean<>(new MetricsServlet(), PROMETHEUS_METRICS_PATH);
  }
}
