package feast.ingestion.transform;

import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

public class WriteFeatureMetricsToInfluxDB
    extends PTransform<PCollection<FeatureRowExtended>, PDone> {

  private String influxDbUrl;
  private String influxDbDatabase;
  private String influxDbMeasurement;

  public WriteFeatureMetricsToInfluxDB(
      String influxDbUrl, String influxDbDatabase, String influxDbMeasurement) {
    this.influxDbUrl = influxDbUrl;
    this.influxDbDatabase = influxDbDatabase;
    this.influxDbMeasurement = influxDbMeasurement;
  }

  @Override
  public PDone expand(PCollection<FeatureRowExtended> input) {
    input.apply(
        ParDo.of(
            new DoFn<FeatureRowExtended, Void>() {
              InfluxDB influxDB;

              @Setup
              public void setup() {
                influxDB = InfluxDBFactory.connect(influxDbUrl);
                influxDB.setDatabase(influxDbDatabase);
                influxDB.enableBatch(BatchOptions.DEFAULTS);
              }

              @FinishBundle
              public void finishBundle() {
                if (influxDB != null) {
                  influxDB.close();
                }
              }

              @ProcessElement
              public void processElement(
                  ProcessContext c, @Element FeatureRowExtended featureRowExtended) {
                FeatureRow featureRow = featureRowExtended.getRow();
                for (Feature feature : featureRow.getFeaturesList()) {
                  String featureId = feature.getId();
                  long lagInSeconds =
                      System.currentTimeMillis() / 1000L
                          - featureRow.getEventTimestamp().getSeconds();
                  double value = getValue(feature);
                  influxDB.write(
                      Point.measurement(influxDbMeasurement)
                          .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                          .addField("lag_in_seconds", lagInSeconds)
                          .addField("value", value)
                          .tag("feature_id", featureId)
                          .build());
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
