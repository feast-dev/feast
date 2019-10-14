package feast.serving.util;

import com.google.protobuf.Duration;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.GetBatchFeaturesRequest.FeatureSet;
import java.util.ArrayList;
import java.util.List;

public class BigQueryUtil {

  public static String createQuery(
      List<FeatureSet> featureSets,
      List<FeatureSetSpec> featureSetSpecs,
      List<String> entityNames,
      String bigqueryDataset,
      String leftTableName) {
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
              bigqueryDataset,
              leftTableName));
    }

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
      String datasetName,
      String leftTableName) {

    String joinedEntities = entityNames.stream().map(s -> String.format("joined.%s, ", s))
        .reduce(String::concat).get();
    String selectEntities = entityNames.stream().map(s -> String.format("l.%s, ", s))
        .reduce(String::concat).get();
    String joinedFeatures = featureSet.getFeatureNamesList().stream()
        .map(s -> String.format("joined.%s_%s, ", getTableName(featureSet), s))
        .reduce(String::concat).get();
    String selectFeatures = featureSet.getFeatureNamesList().stream()
        .map(s -> String.format("r.%s as %s_%s, ", s, getTableName(featureSet), s))
        .reduce(String::concat).get();
    String joinConditions = featureSetSpec.getEntitiesList().stream()
        .map(EntitySpec::getName)
        .map(s -> String.format("AND l.%s = r.%s ", s, s))
        .reduce(String::concat).get();

    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append(String.format(
        "SELECT * FROM (SELECT joined.event_timestamp, %s %s ROW_NUMBER() ",
        joinedEntities, joinedFeatures));
    queryBuilder.append(String.format(
        "OVER ( PARTITION BY %s joined.event_timestamp ORDER BY joined.r_event_timestamp DESC) rank ",
        joinedEntities));
    queryBuilder.append(String.format(
        "FROM (SELECT %s l.event_timestamp, %s r.event_timestamp AS r_event_timestamp ",
        selectEntities, selectFeatures));
    queryBuilder.append(String.format(
        "FROM %s.%s AS l LEFT OUTER JOIN %s.%s AS r ",
        datasetName, leftTableName, datasetName, getTableName(featureSet)));
    queryBuilder.append(String.format("ON l.event_timestamp >= r.event_timestamp \n"
            + "AND Timestamp_sub(l.event_timestamp, interval %d second) < r.event_timestamp ",
        getMaxAge(featureSet, featureSetSpec).getSeconds()));
    queryBuilder
        .append(String.format("%s) AS joined) AS reduce WHERE  reduce.rank = 1",
            joinConditions));
    return queryBuilder.toString();
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
