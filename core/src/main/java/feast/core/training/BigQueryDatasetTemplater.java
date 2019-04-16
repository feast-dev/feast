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
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;

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

    assert featureInfos.size() > 0;
    String tableId = getBqTableId(featureInfos.get(0));
    String entityName = featureInfos.get(0).getEntity().getName();
    Features features = new Features(featureIds, tableId, entityName);

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

  private String getBqTableId(FeatureInfo featureInfo) {
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

    Features(List<String> featureIds, String tableId, String entityName) {
      // columns represent the column name in BigQuery
      // feature with id "myentity.myfeature" will be represented as column "myfeature" in BigQuery
      columns = featureIds.stream()
          .map(f -> f.replace(".", "_").replace(entityName + "_", ""))
          .collect(Collectors.toList());
      this.tableId = tableId;
    }
  }

}
