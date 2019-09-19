package feast.core.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ImportJobMetricsConfig {
  private final boolean ingestionMetricsEnabled;
  private final String influxDbUrl;
  private final String influxDbName;
  private final String influxDbMeasurementName;
}
