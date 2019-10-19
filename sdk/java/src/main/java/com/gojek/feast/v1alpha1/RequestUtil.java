package com.gojek.feast.v1alpha1;

import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.FeatureSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javafx.util.Pair;

@SuppressWarnings("WeakerAccess")
public class RequestUtil {
  public static List<FeatureSet> createFeatureSets(List<String> featureIds) {
    if (featureIds == null) {
      throw new IllegalArgumentException("featureIds cannot be null");
    }

    // featureSetMap is a map of pair of feature set name and version -> a list of feature names
    Map<Pair<String, Integer>, List<String>> featureSetMap = new HashMap<>();

    for (String featureId : featureIds) {
      String[] parts = featureId.split(":");
      if (parts.length < 3) {
        throw new IllegalArgumentException(
            String.format(
                "Feature id '%s' has invalid format. Expected format: <feature_set_name>:<version>:<feature_name>.",
                featureId));
      }
      String featureSetName = parts[0];
      int featureSetVersion;
      try {
        featureSetVersion = Integer.parseInt(parts[1]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format(
                "Feature id '%s' contains invalid version. Expected format: <feature_set_name>:<version>:<feature_name>.",
                parts[1]));
      }

      Pair<String, Integer> key = new Pair<>(featureSetName, featureSetVersion);
      if (!featureSetMap.containsKey(key)) {
        featureSetMap.put(key, new ArrayList<>());
      }
      String featureName = parts[2];
      featureSetMap.get(key).add(featureName);
    }

    return featureSetMap.entrySet().stream()
        .map(
            entry ->
                FeatureSet.newBuilder()
                    .setName(entry.getKey().getKey())
                    .setVersion(entry.getKey().getValue())
                    .addAllFeatureNames(entry.getValue())
                    .build())
        .collect(Collectors.toList());
  }
}
