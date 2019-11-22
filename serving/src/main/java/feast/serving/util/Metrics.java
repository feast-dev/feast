package feast.serving.util;

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

public class Metrics {

  public static final Summary requestLatency = Summary.build()
      .quantile(0.5, 0.01)
      .quantile(0.9, 0.01)
      .quantile(0.95, 0.01)
      .quantile(0.99, 0.01)
      .name("request_latency_ms")
      .subsystem("feast_serving")
      .help("Request latency in milliseconds.")
      .labelNames("method")
      .register();

  public static final Counter requestCount = Counter.build()
      .name("request_feature_count")
      .subsystem("feast_serving")
      .help("number of feature rows requested")
      .labelNames("feature_set_name")
      .register();

  public static final Counter missingKeyCount = Counter.build()
      .name("missing_feature_count")
      .subsystem("feast_serving")
      .help("number requested feature rows that were not found")
      .labelNames("feature_set_name")
      .register();

  public static final Counter staleKeyCount = Counter.build()
      .name("stale_feature_count")
      .subsystem("feast_serving")
      .help("number requested feature rows that were stale")
      .labelNames("feature_set_name")
      .register();
}
