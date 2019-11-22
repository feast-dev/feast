package feast.serving.util;

import com.google.protobuf.Duration;
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.FeatureSetRequest;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Value;

public class BigQueryUtil {

  private static final PebbleEngine engine = new PebbleEngine.Builder().build();
  private static final String FEATURESET_TEMPLATE_NAME = "templates/bq_featureset_query.sql";

  @Value
  public static class FeatureSetInfo {
    String id;
    String name;
    int version;
    long maxAge;
    List<String> entities;
    List<String> features;
  }

  public static String getTimestampLimitQuery(
      String projectId, String datasetId, String leftTableName) {
    return String.format(
        "SELECT DATETIME(MAX(event_timestamp)) as max, DATETIME(MIN(event_timestamp)) as min FROM `%s.%s.%s`",
        projectId, datasetId, leftTableName);
  }

  public static String createQuery(
      List<FeatureSetRequest> featureSets,
      List<FeatureSetSpec> featureSetSpecs,
      List<String> entities,
      String projectId,
      String bigqueryDataset,
      String leftTableName,
      String minTimestamp,
      String maxTimestamp)
      throws IOException {

    if (featureSets == null || featureSetSpecs == null || bigqueryDataset.isEmpty()) {
      return "";
    }

    if (featureSets.size() != featureSetSpecs.size()) {
      return "";
    }

    List<FeatureSetInfo> featureSetInfos = new ArrayList<>();
    for (int i = 0; i < featureSets.size(); i++) {
      FeatureSetSpec spec = featureSetSpecs.get(i);
      FeatureSetRequest request = featureSets.get(i);
      Duration maxAge = getMaxAge(request, spec);
      List<String> fsEntities =
          spec.getEntitiesList().stream().map(EntitySpec::getName).collect(Collectors.toList());
      String id = String.format("%s:%s", spec.getName(), spec.getVersion());
      featureSetInfos.add(
          new FeatureSetInfo(
              id,
              spec.getName(),
              spec.getVersion(),
              maxAge.getSeconds(),
              fsEntities,
              request.getFeatureNamesList()));
    }
    return createQueryForFeatureSets(
        featureSetInfos,
        entities,
        projectId,
        bigqueryDataset,
        leftTableName,
        minTimestamp,
        maxTimestamp);
  }

  public static String createQueryForFeatureSets(
      List<FeatureSetInfo> featureSetInfos,
      List<String> entities,
      String projectId,
      String datasetId,
      String leftTableName,
      String minTimestamp,
      String maxTimestamp)
      throws IOException {

    PebbleTemplate template = engine.getTemplate(FEATURESET_TEMPLATE_NAME);
    Map<String, Object> context = new HashMap<>();
    context.put("featureSets", featureSetInfos);
    context.put("fullEntitiesList", entities);
    context.put("projectId", projectId);
    context.put("datasetId", datasetId);
    context.put("minTimestamp", minTimestamp);
    context.put("maxTimestamp", maxTimestamp);
    context.put("leftTableName", leftTableName);

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  private static Duration getMaxAge(FeatureSetRequest featureSet, FeatureSetSpec featureSetSpec) {
    if (featureSet.getMaxAge() == Duration.getDefaultInstance()) {
      return featureSetSpec.getMaxAge();
    }
    return featureSet.getMaxAge();
  }
}
