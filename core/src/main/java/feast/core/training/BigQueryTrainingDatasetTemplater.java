/*
 * Copyright 2018 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package feast.core.training;

import static java.util.stream.Collectors.groupingBy;

import com.google.protobuf.Timestamp;
import com.hubspot.jinjava.Jinjava;
import feast.core.TrainingServiceProto.FeatureSet;
import feast.core.dao.FeatureInfoRepository;
import feast.core.model.FeatureInfo;
import feast.core.model.StorageInfo;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.GranularityProto.Granularity;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;

public class BigQueryTrainingDatasetTemplater {
  private final FeatureInfoRepository featureInfoRepository;
  private final Jinjava jinjava;
  private final String template;
  private final DateTimeFormatter formatter;
  private Comparator<? super FeatureGroup> featureGroupComparator =
      new FeatureGroupTemplateComparator().reversed();

  public BigQueryTrainingDatasetTemplater(
      Jinjava jinjava, String templateString, FeatureInfoRepository featureInfoRepository) {
    this.featureInfoRepository = featureInfoRepository;
    this.jinjava = jinjava;
    this.template = templateString;
    this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));
  }

  /**
   * Create query from a template.
   *
   * @param featureSet feature set
   * @param startDate start date
   * @param endDate end date
   * @param limit limit
   * @return SQL query for creating training table.
   */
  public String createQuery(
      FeatureSet featureSet, Timestamp startDate, Timestamp endDate, long limit) {
    List<String> featureIds = featureSet.getFeatureIdsList();
    List<FeatureInfo> featureInfos = featureInfoRepository.findAllById(featureIds);
    if (featureInfos.size() < featureIds.size()) {
      Set<String> foundFeatureIds =
          featureInfos.stream().map(FeatureInfo::getId).collect(Collectors.toSet());
      featureIds.removeAll(foundFeatureIds);
      throw new NoSuchElementException("features not found: " + featureIds);
    }

    List<Feature> features = toFeatureTemplates(featureInfos);
    List<FeatureGroup> featureGroups = groupFeatureTemplate(features);

    String startDateStr = formatDateString(startDate);
    String endDateStr = formatDateString(endDate);
    String limitStr = (limit != 0) ? String.valueOf(limit) : null;
    return renderTemplate(featureGroups, startDateStr, endDateStr, limitStr);
  }

  private String renderTemplate(
      List<FeatureGroup> featureGroups, String startDateStr, String endDateStr, String limitStr) {
    Map<String, Object> context = new HashMap<>();
    featureGroups.sort(featureGroupComparator);

    context.put("feature_groups", featureGroups);
    context.put("start_date", startDateStr);
    context.put("end_date", endDateStr);
    context.put("limit", limitStr);
    return jinjava.render(template, context);
  }

  private List<Feature> toFeatureTemplates(List<FeatureInfo> featureInfos) {
    return featureInfos
        .stream()
        .map(
            fi -> {
              StorageInfo whStorage = fi.getWarehouseStore();
              String tableId = getBqTableId(fi.getFeatureSpec(), whStorage);
              return new Feature(fi.getId(), fi.getGranularity(), tableId);
            })
        .collect(Collectors.toList());
  }

  private String getBqTableId(FeatureSpec featureSpec, StorageInfo whStorage) {
    String type = whStorage.getType();
    if (!"bigquery".equals(type)) {
      throw new IllegalArgumentException(
          "One of the feature has warehouse storage other than bigquery");
    }

    StorageSpec storageSpec = whStorage.getStorageSpec();
    Map<String, String> options = storageSpec.getOptionsMap();
    String projectId = options.get("project");
    String dataset = options.get("dataset");
    String entityName = featureSpec.getEntity().toLowerCase();
    String granularity = featureSpec.getGranularity().toString().toLowerCase();
    return String.format("%s.%s.%s_%s", projectId, dataset, entityName, granularity);
  }

  private List<FeatureGroup> groupFeatureTemplate(List<Feature> features) {
    Map<String, List<Feature>> groupedFeature =
        features.stream().collect(groupingBy(Feature::getTableId));
    List<FeatureGroup> featureGroups = new ArrayList<>();
    for (Map.Entry<String, List<Feature>> entry : groupedFeature.entrySet()) {
      String tableId = entry.getKey();
      Granularity.Enum granularity = entry.getValue().get(0).granularity;
      FeatureGroup group = new FeatureGroup(tableId, granularity, entry.getValue());

      featureGroups.add(group);
    }
    return featureGroups;
  }

  private String formatDateString(Timestamp timestamp) {
    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds()).truncatedTo(ChronoUnit.DAYS);
    return formatter.format(instant);
  }

  @Getter
  static final class FeatureGroup {
    final String tableId;
    final Granularity.Enum granularity;
    final List<Feature> features;

    public FeatureGroup(String tableId, Granularity.Enum granularity, List<Feature> features) {
      this.tableId = tableId;
      this.granularity = granularity;
      this.features = features;
    }

    public String getTempTable() {
      return tableId.replaceAll("[^a-zA-Z0-9]", "_");
    }

    public String getGranularityStr() {
      return granularity.toString().toLowerCase();
    }
  }

  @Getter
  static final class Feature {
    final String featureId;
    final String tableId;
    final Granularity.Enum granularity;

    public Feature(String featureId, Granularity.Enum granularity, String tableId) {
      this.featureId = featureId;
      this.tableId = tableId;
      this.granularity = granularity;
    }

    public String getName() {
      return featureId.split("\\.")[2];
    }

    public String getColumn() {
      return featureId.replace(".", "_");
    }
  }

  private static final class FeatureGroupTemplateComparator implements Comparator<FeatureGroup> {
    @Override
    public int compare(FeatureGroup o1, FeatureGroup o2) {
      if (o1.granularity != o2.granularity) {
        return o1.granularity.getNumber() - o2.granularity.getNumber();
      }

      return o1.tableId.compareTo(o2.tableId);
    }
  }
}
