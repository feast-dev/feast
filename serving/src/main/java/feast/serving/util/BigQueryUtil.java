package feast.serving.util;

import com.google.protobuf.util.Timestamps;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDatasetRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import feast.types.ValueProto.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

public class BigQueryUtil {
  public static String createQuery(
      List<FeatureSet> featureSets,
      List<FeatureSetSpec> featureSetSpecs,
      List<String> entityNames,
      List<EntityDatasetRow> entityDatasetRows,
      String bigqueryDataset) {
    // TODO: Arguments validation
    //       - size of featureSetsRequested == size of featureSetSpecs

    StringBuilder queryBuilder = new StringBuilder();

    for (int i = 0; i < entityDatasetRows.size(); i++) {
      EntityDatasetRow entityDatasetRow = entityDatasetRows.get(i);
      if (i > 0) {
        queryBuilder.append(" UNION ALL ");
      }

      String whereExprForLeftMostTable = "";

      for (int j = 0; j < featureSets.size(); j++) {
        List<String> columnsSelected = new ArrayList<>();
        String tableName = getTableName(featureSets.get(j));

        if (j == 0) {
          for (String entityName : entityNames) {
            columnsSelected.add(
                String.format(
                    "%s.%s %s_%s", tableName, entityName, featureSets.get(0).getName(), entityName));
          }
          for (FeatureSet featureSet : featureSets) {
            tableName = getTableName(featureSet);
            for (String featureName : featureSet.getFeatureNamesList()) {
              columnsSelected.add(
                  String.format(
                      "%s.%s %s_%s", tableName, featureName, featureSet.getName(), featureName));
            }
          }
          columnsSelected.add(String.format("%s.event_timestamp", getTableName(featureSets.get(0))));
          String query =
              createQueryForFeatureSet(
                  featureSets.get(0),
                  featureSetSpecs.get(0),
                  entityNames,
                  entityDatasetRow,
                  bigqueryDataset,
                  columnsSelected);
          whereExprForLeftMostTable = query.substring(query.lastIndexOf("WHERE"));
          String queryWithoutWhereExpr = query.substring(0, query.lastIndexOf("WHERE"));

          queryBuilder.append(queryWithoutWhereExpr);
          continue;
        }

        List<String> entityNamesInFeatureSetSpec =
            featureSetSpecs.get(j).getEntitiesList().stream()
                .map(EntitySpec::getName)
                .collect(Collectors.toList());
        for (String featureName : featureSets.get(j).getFeatureNamesList()) {
          columnsSelected.add(String.format("%s.%s", tableName, featureName));
        }
        for (String entityName : entityNames) {
          if (entityNamesInFeatureSetSpec.contains(entityName)) {
            columnsSelected.add(String.format("%s.%s", tableName, entityName));
          }
        }

        String query =
            createQueryForFeatureSet(
                featureSets.get(j),
                featureSetSpecs.get(j),
                entityNames,
                entityDatasetRow,
                bigqueryDataset,
                columnsSelected);

        List<String> joinOnBoolExprAsList = new ArrayList<>();
        for (String entityName : entityNames) {
          if (entityNamesInFeatureSetSpec.contains(entityName)) {
            String tableNameLeftMost = getTableName(featureSets.get(0));
            joinOnBoolExprAsList.add(
                String.format("%s.%s = %s.%s", tableNameLeftMost, entityName, tableName, entityName));
          }
        }
        String joinOnBoolExpr = String.join(" AND ", joinOnBoolExprAsList);
        String joinQuery = String.format("LEFT JOIN (%s) %s ON %s", query, tableName, joinOnBoolExpr);
        queryBuilder.append(" ").append(joinQuery);
      }

      queryBuilder.append(" ").append(whereExprForLeftMostTable);
    }

    return queryBuilder.toString();
  }

  private static String createQueryForFeatureSet(
      FeatureSet featureSet,
      FeatureSetSpec featureSetSpec,
      List<String> entityNames,
      EntityDatasetRow entityDatasetRow,
      String datasetName,
      List<String> columnsSelectedAsList) {
    String columnsSelected = String.join(", ", columnsSelectedAsList);
    String tableName = getTableName(featureSet);
    String groupedTableName =
        String.format("%s_v%s_grouped", featureSet.getName(), featureSet.getVersion());
    List<String> entityNamesInFeatureSetSpec =
        featureSetSpec.getEntitiesList().stream()
            .map(EntitySpec::getName)
            .collect(Collectors.toList());
    String keyColumns =
        String.join(", ", CollectionUtils.intersection(entityNamesInFeatureSetSpec, entityNames));

    List<String> joinOnBoolExprAsList = new ArrayList<>();
    joinOnBoolExprAsList.add(
        String.format("%s.event_timestamp = %s.event_timestamp", tableName, groupedTableName));
    for (String entityName : entityNames) {
      if (entityNamesInFeatureSetSpec.contains(entityName)) {
        joinOnBoolExprAsList.add(
            String.format("%s.%s = %s.%s", tableName, entityName, groupedTableName, entityName));
      }
    }
    String joinOnBoolExpr = String.join(" AND ", joinOnBoolExprAsList);

    List<String> whereBoolExprAsList = new ArrayList<>();
    String timestampLowerBound =
        Timestamps.toString(
            Timestamps.subtract(entityDatasetRow.getEntityTimestamp(), featureSet.getMaxAge()));
    String timestampUpperBound = Timestamps.toString(entityDatasetRow.getEntityTimestamp());
    whereBoolExprAsList.add(
        String.format(
            "%s.event_timestamp BETWEEN '%s' AND '%s'",
            tableName, timestampLowerBound, timestampUpperBound));
    List<Value> entityIdsList = entityDatasetRow.getEntityIdsList();
    for (int i = 0; i < entityIdsList.size(); i++) {
      if (entityNamesInFeatureSetSpec.contains(entityNames.get(i))) {
        whereBoolExprAsList.add(
            String.format(
                "%s.%s = %s",
                tableName, entityNames.get(i), getFormattedFeastValue(entityIdsList.get(i))));
      }
    }
    String whereBoolExpr = String.join(" AND ", whereBoolExprAsList);

    return String.format(
        "SELECT %s FROM %s "
            + "INNER JOIN (SELECT %s, MAX(event_timestamp) event_timestamp FROM %s WHERE %s GROUP BY %s) %s ON %s "
            + "WHERE %s",
        columnsSelected,
        datasetName + "." + tableName,
        keyColumns,
        datasetName + "." + tableName,
        whereBoolExpr,
        keyColumns,
        groupedTableName,
        joinOnBoolExpr,
        whereBoolExpr);
  }

  private static String getTableName(FeatureSet featureSet) {
    return String.format("%s_v%s", featureSet.getName(), featureSet.getVersion());
  }

  private static String getFormattedFeastValue(Value value) {
    String returnValue = "";
    switch (value.getValCase()) {
      case STRING_VAL:
        returnValue = String.format("'%s'", value.getStringVal());
        break;
      case INT32_VAL:
        returnValue = String.valueOf(value.getInt32Val());
        break;
      case INT64_VAL:
        returnValue = String.valueOf(value.getInt64Val());
        break;
      case DOUBLE_VAL:
        returnValue = String.valueOf(value.getDoubleVal());
        break;
      case FLOAT_VAL:
        returnValue = String.valueOf(value.getFloatVal());
        break;
      case BOOL_VAL:
        returnValue = String.valueOf(value.getBoolVal());
        break;
      case BYTES_VAL:
      case BYTES_LIST_VAL:
      case STRING_LIST_VAL:
      case INT32_LIST_VAL:
      case INT64_LIST_VAL:
      case DOUBLE_LIST_VAL:
      case FLOAT_LIST_VAL:
      case BOOL_LIST_VAL:
      case VAL_NOT_SET:
        throw new UnsupportedOperationException();
    }
    return returnValue;
  }
}
