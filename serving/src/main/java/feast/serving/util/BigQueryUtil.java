package feast.serving.util;

import com.google.protobuf.Duration;
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.GetBatchFeaturesRequest.FeatureSet;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQueryUtil {

  private static final PebbleEngine engine = new PebbleEngine.Builder().build();
  private static final String FEATURESET_TEMPLATE_NAME = "templates/bq_featureset_query.sql";

  public static String getTimestampLimitQuery(String projectId, String datasetId,
      String leftTableName) {
    return String.format(
        "SELECT DATETIME(MAX(event_timestamp)) as max, DATETIME(MIN(event_timestamp)) as min FROM `%s.%s.%s`",
        projectId, datasetId, leftTableName);
  }

  public static String createQuery(
      List<FeatureSet> featureSets,
      List<FeatureSetSpec> featureSetSpecs,
      List<String> entityNames,
      String projectId,
      String bigqueryDataset,
      String leftTableName,
      String minTimestamp,
      String maxTimestamp) throws IOException {
    if (featureSets == null
        || featureSetSpecs == null
        || entityNames == null
        || bigqueryDataset.isEmpty()) {
      return "";
    }

    if (featureSets.size() != featureSetSpecs.size()) {
      return "";
    }

    List<String> featureSetQueries = new ArrayList<>();
    for (int i = 0; i < featureSets.size(); i++) {
      featureSetQueries.add(
          createQueryForFeatureSet(
              featureSets.get(i),
              featureSetSpecs.get(i),
              entityNames,
              projectId,
              bigqueryDataset,
              leftTableName,
              minTimestamp,
              maxTimestamp));
    }

    // TODO: templatize this as well
    if (featureSetQueries.size() > 1) {
      StringBuilder selectColumns = new StringBuilder("SELECT ");
      for (int i = 0; i < featureSets.size(); i++) {
        FeatureSet featureSet = featureSets.get(i);
        String selectAs = String
            .format("(%s) AS %s ", featureSetQueries.get(i),
                getTableName(featureSet));
        String tableName = getTableName(featureSet);
        if (i != 0) {
          selectAs = selectAs + createJoinCondition(getTableName(featureSets.get(0)),
              tableName, entityNames);
        } else {
          selectColumns.append(String.format("%s.event_timestamp", tableName));
          for (String entityName : entityNames) {
            selectColumns.append(String.format(", %s.%s", tableName, entityName));
          }
        }
        for (String featureName : featureSet.getFeatureNamesList()) {
          selectColumns.append(String.format(", %s.%s_%s", tableName, tableName, featureName));
        }
        featureSetQueries.set(i, selectAs);
      }
      selectColumns.append(" FROM ");
      return selectColumns.toString() + String.join("LEFT JOIN", featureSetQueries);
    }

    return featureSetQueries.get(0);
  }

  private static String createQueryForFeatureSet(
      FeatureSet featureSet,
      FeatureSetSpec featureSetSpec,
      List<String> entityNames,
      String projectId,
      String datasetName,
      String leftTableName,
      String minTimestamp,
      String maxTimestamp) throws IOException {

    PebbleTemplate template = engine.getTemplate(FEATURESET_TEMPLATE_NAME);
    Map<String, Object> context = new HashMap<>();
    context.put("entityNames", entityNames);
    context.put("featureSet", featureSet);
    context.put("featureSetSpec", featureSetSpec);
    context.put("maxAge", getMaxAge(featureSet, featureSetSpec).getSeconds());
    context.put("minTimestamp", minTimestamp);
    context.put("maxTimestamp", maxTimestamp);
    context.put("leftTableName", String.format("%s.%s.%s", projectId, datasetName, leftTableName));
    context.put("rightTableName",
        String.format("%s.%s.%s", projectId, datasetName, getTableName(featureSet)));

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  private static Duration getMaxAge(FeatureSet featureSet, FeatureSetSpec featureSetSpec) {
    if (featureSet.getMaxAge() == Duration.getDefaultInstance()) {
      return featureSetSpec.getMaxAge();
    }
    return featureSet.getMaxAge();
  }

  private static String createJoinCondition(String featureSet1, String featureSet2,
      List<String> entityNames) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(
        String.format("ON %s.event_timestamp = %s.event_timestamp ", featureSet1, featureSet2));
    for (String entityName : entityNames) {
      stringBuilder.append(
          String.format("AND %s.%s = %s.%s ", featureSet1, entityName, featureSet2, entityName));
    }
    return stringBuilder.toString();
  }

  private static String getTableName(FeatureSet featureSet) {
    return String.format("%s_v%s", featureSet.getName(), featureSet.getVersion());
  }

}
