package feast.serving.util;

import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDatasetRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import java.util.ArrayList;
import java.util.List;
import jdk.net.SocketFlow.Status;

public class BigQueryUtil {
  public static String createQuery(
      List<FeatureSet> featureSets,
      List<String> entityNames,
      List<EntityDatasetRow> entityDatasetRows,
      String datasetName) {

    // FeatureSet featureSet = featureSets.get(0);
    EntityDatasetRow entityDatasetRow = entityDatasetRows.get(0);

    // TODO: Arguments validation

    StringBuilder queryBuilder = new StringBuilder();

    for (int i = 0; i < featureSets.size(); i++) {
      FeatureSet featureSet = featureSets.get(i);
      String query =
          createQueryFromFeatureSet(featureSet, entityNames, entityDatasetRow, datasetName);

      if (i == 0) {
        queryBuilder.append(query);
        continue;
      }

      String tableName = getTableName(featureSet);
      String prevTableName = getTableName(featureSets.get(i - 1));
      List<String> joinOnBoolExprAsList = new ArrayList<>();
      for (String keyColumn : entityNames) {
        joinOnBoolExprAsList.add(
            String.format("%s.%s = %s.%s", prevTableName, keyColumn, tableName, keyColumn));
      }
      String joinOnBoolExpr = String.join(" AND ", joinOnBoolExprAsList);
      String joinQuery = String.format("LEFT JOIN (%s) %s ON %s", query, tableName, joinOnBoolExpr);
      queryBuilder.append(" ").append(joinQuery);
    }



    return queryBuilder.toString();
  }

  private static String createQueryFromFeatureSet(
      FeatureSet featureSet,
      List<String> entityNames,
      EntityDatasetRow entityDatasetRow,
      String datasetName) {
    String tableName = getTableName(featureSet);
    String groupedTableName =
        String.format("%s_v%s_grouped", featureSet.getName(), featureSet.getVersion());
    String keyColumns = String.join(", ", entityNames);

    List<String> columnsAsList = new ArrayList<>();
    for (String entityName : entityNames) {
      columnsAsList.add(String.format("%s.%s", tableName, entityName));
    }
    for (String featureName : featureSet.getFeatureNamesList()) {
      columnsAsList.add(String.format("%s.%s", tableName, featureName));
    }
    columnsAsList.add(String.format("%s.event_timestamp", tableName));
    String columns = String.join(", ", columnsAsList);

    List<String> joinOnBoolExprAsList = new ArrayList<>();
    joinOnBoolExprAsList.add(
        String.format("%s.event_timestamp = %s.event_timestamp", tableName, groupedTableName));
    for (String keyColumn : entityNames) {
      joinOnBoolExprAsList.add(
          String.format("%s.%s = %s.%s", tableName, keyColumn, groupedTableName, keyColumn));
    }
    String joinOnBoolExpr = String.join(" AND ", joinOnBoolExprAsList);

    String timestampLowerBound =
        Timestamps.toString(
            Timestamps.subtract(entityDatasetRow.getEntityTimestamp(), featureSet.getMaxAge()));
    String timestampUpperBound = Timestamps.toString(entityDatasetRow.getEntityTimestamp());

    return String.format(
        "SELECT %s FROM %s "
            + "INNER JOIN (SELECT %s, MAX(event_timestamp) event_timestamp FROM %s GROUP BY %s) %s ON %s "
            + "WHERE %s.event_timestamp BETWEEN '%s' AND '%s' ",
        columns,
        datasetName + "." + tableName,
        keyColumns,
        datasetName + "." + tableName,
        keyColumns,
        groupedTableName,
        joinOnBoolExpr,
        tableName,
        timestampLowerBound,
        timestampUpperBound);
  }

  private static String getTableName(FeatureSet featureSet) {
    return String.format("%s_v%s", featureSet.getName(), featureSet.getVersion());
  }
}
