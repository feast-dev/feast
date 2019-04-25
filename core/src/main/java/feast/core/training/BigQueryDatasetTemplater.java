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
import feast.core.model.StorageInfo;
import feast.specs.StorageSpecProto.StorageSpec;
import lombok.Getter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class BigQueryDatasetTemplater {
  private final FeatureInfoRepository featureInfoRepository;
  private final Jinjava jinjava;
  private final String template;
  private final DateTimeFormatter formatter;

  public BigQueryDatasetTemplater(
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
  String createQuery(FeatureSet featureSet, Timestamp startDate, Timestamp endDate, long limit) {
    List<String> featureIds = featureSet.getFeatureIdsList();
    List<FeatureInfo> featureInfos = featureInfoRepository.findAllById(featureIds);
    Features features = new Features(featureInfos);

    if (featureInfos.size() < featureIds.size()) {
      Set<String> foundFeatureIds =
          featureInfos.stream().map(FeatureInfo::getId).collect(Collectors.toSet());
      featureIds.removeAll(foundFeatureIds);
      throw new NoSuchElementException("features not found: " + featureIds);
    }

    String startDateStr = formatDateString(startDate);
    String endDateStr = formatDateString(endDate);
    String limitStr = (limit != 0) ? String.valueOf(limit) : null;
    return renderTemplate(features, startDateStr, endDateStr, limitStr);
  }

  private String renderTemplate(
      Features features, String startDateStr, String endDateStr, String limitStr) {
    Map<String, Object> context = new HashMap<>();

    context.put("feature_set", features);
    context.put("start_date", startDateStr);
    context.put("end_date", endDateStr);
    context.put("limit", limitStr);
    return jinjava.render(template, context);
  }

  private static String getBqTableId(FeatureInfo featureInfo) {
    StorageInfo whStorage = featureInfo.getWarehouseStore();

    String type = whStorage.getType();
    if (!"bigquery".equals(type)) {
      throw new IllegalArgumentException(
          "One of the feature has warehouse storage other than bigquery");
    }

    StorageSpec storageSpec = whStorage.getStorageSpec();
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

  @Getter
  static final class Features {
    final List<String> columns;
    final String tableId;

    Features(List<FeatureInfo> featureInfos) {
      columns = featureInfos.stream().map(FeatureInfo::getName).collect(Collectors.toList());
      tableId = featureInfos.size() > 0 ? getBqTableId(featureInfos.get(0)) : "";
    }
  }
}
