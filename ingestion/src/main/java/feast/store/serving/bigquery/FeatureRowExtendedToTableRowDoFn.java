package feast.store.serving.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.protobuf.util.Timestamps;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.types.FeatureProto.Field;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

public class FeatureRowExtendedToTableRowDoFn extends DoFn<FeatureRowExtended, TableRow> {
  private static final String EVENT_TIMESTAMP_COLUMN = "event_timestamp";
  private static final String CREATED_TIMESTAMP_COLUMN = "created_timestamp";
  private static final String JOB_ID_COLUMN = "job_id";
  private final String jobId;

  public FeatureRowExtendedToTableRowDoFn(String jobId) {
    this.jobId = jobId;
  }

  @ProcessElement
  public void processElement(
      @Element FeatureRowExtended featureRowExtended, OutputReceiver<TableRow> out) {
    FeatureRow featureRow = featureRowExtended.getRow();

  }

  private static TableRow createTableRow(FeatureRow featureRow, String jobId) {
    TableRow tableRow = new TableRow();
    tableRow.set(EVENT_TIMESTAMP_COLUMN, Timestamps.toString(featureRow.getEventTimestamp()));
    tableRow.set(CREATED_TIMESTAMP_COLUMN, Instant.now().toString());
    tableRow.set(JOB_ID_COLUMN, jobId);
    // TODO
    for (Field field : featureRow.getFieldsList()) {
      tableRow.set(field, )
    }
  }
}
