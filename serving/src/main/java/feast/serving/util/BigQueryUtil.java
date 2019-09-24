package feast.serving.util;

import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDatasetRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import java.util.ArrayList;
import java.util.List;

public class BigQueryUtil {
  public static String createQuery(
      String bigqueryDataset,
      List<FeatureSet> featureSets,
      List<String> entityNames,
      List<EntityDatasetRow> entityDatasetRows) {
    assert featureSets.size() > 0;
    assert entityDatasetRows.size() > 0;

    FeatureSet featureSet = featureSets.get(0);
    EntityDatasetRow entityDatasetRow = entityDatasetRows.get(0);

    if (entityDatasetRow.getEntityIdsCount() > entityNames.size()) {
      // Throw exception
    }

    String tableName = String.format("%s_v%s", featureSet.getName(), featureSet.getVersion());
    String fullTableName =
        String.format("%s.%s_v%s", bigqueryDataset, featureSet.getName(), featureSet.getVersion());
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

    String query =
        String.format(
            "SELECT %s FROM %s "
                + "INNER JOIN (SELECT %s, MAX(event_timestamp) event_timestamp FROM %s GROUP BY %s) %s ON %s"
                + "WHERE %s.event_timestamp BETWEEN %s AND %s",
            columns,
            fullTableName,
            keyColumns,
            fullTableName,
            keyColumns,
            groupedTableName,
            joinOnBoolExpr,
            tableName,
            );

    return query;
  }
}
