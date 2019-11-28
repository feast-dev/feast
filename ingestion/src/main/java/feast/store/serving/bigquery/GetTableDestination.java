package feast.store.serving.bigquery;

import com.google.api.services.bigquery.model.TimePartitioning;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public class GetTableDestination implements
    SerializableFunction<ValueInSingleWindow<FeatureRow>, TableDestination> {

  private String projectId;
  private String datasetId;

  public GetTableDestination(String projectId, String datasetId) {
    this.projectId = projectId;
    this.datasetId = datasetId;
  }

  @Override
  public TableDestination apply(ValueInSingleWindow<FeatureRow> input) {
    String[] split = input.getValue().getFeatureSet().split(":");

    TimePartitioning timePartitioning =
        new TimePartitioning()
            .setType("DAY")
            .setField(FeatureRowToTableRow.getEventTimestampColumn());

    return new TableDestination(
        String.format("%s:%s.%s_v%s", projectId, datasetId, split[0], split[1]),
        String
            .format("Feast table for %s", input.getValue().getFeatureSet()),
        timePartitioning);
  }
}