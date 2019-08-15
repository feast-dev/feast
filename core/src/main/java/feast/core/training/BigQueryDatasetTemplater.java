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

import com.google.protobuf.Timestamp;
import com.hubspot.jinjava.Jinjava;
import feast.core.DatasetServiceProto.FeatureSet;
import feast.core.dao.FeatureInfoRepository;
import feast.core.model.FeatureInfo;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.ValueProto.ValueType.Enum;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

public class BigQueryDatasetTemplater {

  private final FeatureInfoRepository featureInfoRepository;
  private final Jinjava jinjava;
  private final String template;
  private final StorageSpec storageSpec;
  private final DateTimeFormatter formatter;

  public BigQueryDatasetTemplater(
      Jinjava jinjava,
      String templateString,
      StorageSpec storageSpec,
      FeatureInfoRepository featureInfoRepository) {
    this.storageSpec = storageSpec;
    this.featureInfoRepository = featureInfoRepository;
    this.jinjava = jinjava;
    this.template = templateString;
    this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));
  }

  protected StorageSpec getStorageSpec() {
    return storageSpec;
  }

  /**
   * Create query from a template.
   *
   * @param featureSet feature set
   * @param startDate start date
   * @param endDate end date
   * @param limit limit
   * @param filters additional WHERE clause
   * @return SQL query for creating training table.
   */
  String createQuery(
      FeatureSet featureSet,
      Timestamp startDate,
      Timestamp endDate,
      long limit,
      Map<String, String> filters) {
    List<String> featureIds = featureSet.getFeatureIdsList();
    List<FeatureInfo> featureInfos = getFeatureInfosOrThrow(featureIds);

    // split filter based on ValueType of the feature
    Map<String, String> tmpFilter = new HashMap<>(filters);
    Map<String, String> numberFilters = new HashMap<>();
    Map<String, String> stringFilters = new HashMap<>();
    if (filters.containsKey("job_id")) {
      stringFilters.put("job_id", tmpFilter.get("job_id"));
      tmpFilter.remove("job_id");
    }

    List<FeatureInfo> featureFilterInfos =  getFeatureInfosOrThrow(new ArrayList<>(tmpFilter.keySet()));
    Map<String, FeatureInfo> featureInfoMap = new HashMap<>();
    for (FeatureInfo featureInfo: featureFilterInfos) {
      featureInfoMap.put(featureInfo.getId(), featureInfo);
    }


    for (Map.Entry<String, String> filter : tmpFilter.entrySet()) {
      FeatureInfo featureInfo = featureInfoMap.get(filter.getKey());
      if (isMappableToString(featureInfo.getValueType())) {
        stringFilters.put(featureInfo.getName(), filter.getValue());
      } else {
        numberFilters.put(featureInfo.getName(), filter.getValue());
      }
    }

    List<String> featureNames = getFeatureNames(featureInfos);
    String tableId = getBqTableId(featureInfos.get(0));
    String startDateStr = formatDateString(startDate);
    String endDateStr = formatDateString(endDate);
    String limitStr = (limit != 0) ? String.valueOf(limit) : null;
    return renderTemplate(tableId, featureNames, startDateStr, endDateStr, limitStr,
        numberFilters, stringFilters);
  }

  private boolean isMappableToString(Enum valueType) {
    return valueType.equals(Enum.STRING);
  }

  private List<String> getFeatureNames(List<FeatureInfo> featureInfos) {
    return featureInfos.stream().map(FeatureInfo::getName).collect(Collectors.toList());
  }

  private List<FeatureInfo> getFeatureInfosOrThrow(List<String> featureIds) {
    List<FeatureInfo> featureInfos = featureInfoRepository.findAllById(featureIds);
    if (featureInfos.size() < featureIds.size()) {
      Set<String> foundFeatureIds =
          featureInfos.stream().map(FeatureInfo::getId).collect(Collectors.toSet());
      featureIds.removeAll(foundFeatureIds);
      throw new NoSuchElementException("features not found: " + featureIds);
    }
    return featureInfos;
  }

  private String renderTemplate(
      String tableId, List<String> features, String startDateStr, String endDateStr, String limitStr,
      Map<String, String> numberFilters,
      Map<String, String> stringFilters) {
    Map<String, Object> context = new HashMap<>();

    context.put("table_id", tableId);
    context.put("features", features);
    context.put("start_date", startDateStr);
    context.put("end_date", endDateStr);
    context.put("limit", limitStr);
    context.put("number_filters", numberFilters);
    context.put("string_filters", stringFilters);
    return jinjava.render(template, context);
  }

  private String getBqTableId(FeatureInfo featureInfo) {
    String type = storageSpec.getType();

    if (!type.equals("BIGQUERY")) {
      throw new IllegalArgumentException(
          "One of the feature has warehouse storage other than bigquery");
    }

    StorageSpec storageSpec = getStorageSpec();
    Map<String, String> options = storageSpec.getOptionsMap();
    String projectId = options.get("project");
    String dataset = options.get("dataset");
    String entityName = featureInfo.getFeatureSpec().getEntity().toLowerCase();
    return String.format("%s.%s.%s", projectId, dataset, entityName);
  }

  private String formatDateString(Timestamp timestamp) {
    Instant instant = Instant.ofEpochSecond(timestamp.getSeconds()).truncatedTo(ChronoUnit.DAYS);
    return formatter.format(instant);
  }
}
