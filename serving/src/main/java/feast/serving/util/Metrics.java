package feast.serving.util;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

public class Metrics {

  public static final Histogram requestLatency =
      Histogram.build()
          .buckets(2, 4, 6, 8, 10, 15, 20, 25, 30, 35, 50)
          .name("request_latency_ms")
          .subsystem("feast_serving")
          .help("Request latency in milliseconds.")
          .labelNames("method")
          .register();

  public static final Counter requestCount =
      Counter.build()
          .name("request_feature_count")
          .subsystem("feast_serving")
          .help("number of feature rows requested")
          .labelNames("feature_set_name")
          .register();

  public static final Counter missingKeyCount =
      Counter.build()
          .name("missing_feature_count")
          .subsystem("feast_serving")
          .help("number requested feature rows that were not found")
          .labelNames("feature_set_name")
          .register();

  public static final Counter staleKeyCount =
      Counter.build()
          .name("stale_feature_count")
          .subsystem("feast_serving")
          .help("number requested feature rows that were stale")
          .labelNames("feature_set_name")
          .register();
}
