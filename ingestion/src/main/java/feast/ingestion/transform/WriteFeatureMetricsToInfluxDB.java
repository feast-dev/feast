package feast.ingestion.transform;

import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import java.util.DoubleSummaryStatistics;
import java.util.LongSummaryStatistics;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.joda.time.Duration;

public class WriteFeatureMetricsToInfluxDB
    extends PTransform<PCollection<FeatureRowExtended>, PDone> {

  private static final int DEFAULT_WINDOW_DURATION_IN_SEC = 30;
  private static final int DEFAULT_INFLUX_DB_JITTER_DURATION_IN_MS = 100;
  private String influxDbUrl;
  private String influxDbDatabase;
  private String influxDbMeasurement;

  public WriteFeatureMetricsToInfluxDB(
      String influxDbUrl, String influxDbDatabase, String influxDbMeasurement) {
    this.influxDbUrl = influxDbUrl;
    this.influxDbDatabase = influxDbDatabase;
    this.influxDbMeasurement = influxDbMeasurement;
  }

  @DefaultCoder(AvroCoder.class)
  static class FeatureMetric {
    // value is the numeric value of the feature, FeatureMetric only supports double type
    // if the value is a timestamp type, value corresponds to epoch seconds
    // if the value is a boolean type, value of 1 corresponds to "true" and 0 otherwise
    // if the value type is other non number format, value will be set to 0
    double value;
    // lagInSeconds is the delta between processing time in the Dataflow job
    // and the event time of the FeatureRow containing this feature
    long lagInSeconds;
    String entityName;

    public FeatureMetric() {}

    FeatureMetric(double value, long lagInSeconds, String entityName) {
      this.value = value;
      this.lagInSeconds = lagInSeconds;
      this.entityName = entityName;
    }
  }

  @Override
  public PDone expand(PCollection<FeatureRowExtended> input) {
    input
        .apply(
            Window.into(FixedWindows.of(Duration.standardSeconds(DEFAULT_WINDOW_DURATION_IN_SEC))))
        .apply(
            "Create feature metric keyed by feature id",
            ParDo.of(
                new DoFn<FeatureRowExtended, KV<String, FeatureMetric>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    FeatureRow featureRow = c.element().getRow();
                    long lagInSeconds =
                        System.currentTimeMillis() / 1000L
                            - featureRow.getEventTimestamp().getSeconds();
                    for (Feature feature : featureRow.getFeaturesList()) {
                      c.output(
                          KV.of(
                              feature.getId(),
                              new FeatureMetric(
                                  getValue(feature), lagInSeconds, featureRow.getEntityName())));
                    }
                  }
                }))
        .apply(GroupByKey.create())
        .apply(
            ParDo.of(
                new DoFn<KV<String, Iterable<FeatureMetric>>, Void>() {
                  InfluxDB influxDB;

                  @Setup
                  public void setup() {
                    try {
                      influxDB = InfluxDBFactory.connect(influxDbUrl);
                      influxDB.setDatabase(influxDbDatabase);
                      influxDB.enableBatch(
                          BatchOptions.DEFAULTS.jitterDuration(
                              DEFAULT_INFLUX_DB_JITTER_DURATION_IN_MS));
                    } catch (Exception e) {
                      // Ignored because writing metrics is not a critical component of Feast
                      // and we do not want to get overwhelmed with connection error logs
                      // due to timeouts and downtime in upstream Influx DB server
                    }
                  }

                  @Teardown
                  public void tearDown() {
                    if (influxDB != null) {
                      influxDB.close();
                    }
                  }

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    if (influxDB == null) {
                      // Influx DB client is not setup properly, skip writing metrics
                      return;
                    }

                    DoubleSummaryStatistics statsForValue = new DoubleSummaryStatistics();
                    LongSummaryStatistics statsForLagInSeconds = new LongSummaryStatistics();

                    String entityName = null;
                    String featureId = c.element().getKey();

                    for (FeatureMetric featureMetric : c.element().getValue()) {
                      statsForValue.accept(featureMetric.value);
                      statsForLagInSeconds.accept(featureMetric.lagInSeconds);
                      if (entityName == null) {
                        entityName = featureMetric.entityName;
                      }
                    }

                    if (featureId == null || entityName == null) {
                      return;
                    }

                    try {
                      influxDB.write(
                          Point.measurement(influxDbMeasurement)
                              .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                              .addField("lag_in_seconds_mean", statsForLagInSeconds.getAverage())
                              .addField("lag_in_seconds_min", statsForLagInSeconds.getMin())
                              .addField("lag_in_seconds_max", statsForLagInSeconds.getMax())
                              .addField("value_mean", statsForValue.getAverage())
                              .addField("value_min", statsForValue.getMin())
                              .addField("value_max", statsForValue.getMax())
                              .addField("value_count", statsForValue.getCount())
                              .tag("feature_id", featureId)
                              .tag("entity_name", entityName)
                              .build());
                    } catch (Exception e) {
                      // Ignored because writing metrics is not a critical component of Feast
                      // and we do not want to get overwhelmed with failed metric write logs
                      // due to timeouts and downtime in upstream Influx DB server
                    }
                  }
                }));

    return PDone.in(input.getPipeline());
  }

  /**
   * This method returns the numeric value of the feature in double type
   *
   * <ul>
   *   <li>if the value is a numeric type, it will be cast to double type
   *   <li>f the value is a timestamp type, value corresponds to epoch seconds
   *   <li>if the value is a boolean type, value of 1 corresponds to "true" and value of 0
   *       corresponds to "false"
   *   <li>if the value type is other non number format, value will be set to 0
   * </ul>
   *
   * @param feature Feast feature instance of a feature row
   * @return numeric value of the feature in double type
   */
  private double getValue(Feature feature) {
    double value;
    Value featureValue = feature.getValue();

    switch (featureValue.getValCase()) {
      case INT32VAL:
        value = featureValue.getInt32Val();
        break;
      case INT64VAL:
        value = featureValue.getInt64Val();
        break;
      case DOUBLEVAL:
        value = featureValue.getDoubleVal();
        break;
      case FLOATVAL:
        value = featureValue.getFloatVal();
        break;
      case BOOLVAL:
        value = featureValue.getBoolVal() ? 1.0 : 0.0;
        break;
      case TIMESTAMPVAL:
        value = featureValue.getTimestampVal().getSeconds();
        break;
      default:
        value = 0.0;
        break;
    }

    return value;
  }
}
