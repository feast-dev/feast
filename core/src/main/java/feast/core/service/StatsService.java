/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
 */
package feast.core.service;

import static java.lang.Math.*;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.core.CoreServiceProto.GetFeatureSetRequest;
import feast.core.CoreServiceProto.GetFeatureStatisticsRequest;
import feast.core.CoreServiceProto.GetFeatureStatisticsResponse;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.StoreType;
import feast.core.dao.FeatureStatisticsRepository;
import feast.core.dao.StoreRepository;
import feast.core.model.Feature;
import feast.core.model.FeatureStatistics;
import feast.core.model.FieldId;
import feast.storage.api.statistics.FeatureSetStatistics;
import feast.storage.api.statistics.StatisticsRetriever;
import feast.storage.connectors.bigquery.stats.BigQueryStatisticsRetriever;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.tensorflow.metadata.v0.*;
import org.tensorflow.metadata.v0.FeatureNameStatistics.Builder;

@Slf4j
@Service
public class StatsService {

  private StoreRepository storeRepository;
  private SpecService specService;
  private FeatureStatisticsRepository featureStatisticsRepository;

  @Autowired
  public StatsService(
      StoreRepository storeRepository,
      SpecService specService,
      FeatureStatisticsRepository featureStatisticsRepository) {
    this.storeRepository = storeRepository;
    this.specService = specService;
    this.featureStatisticsRepository = featureStatisticsRepository;
  }

  @Transactional
  public GetFeatureStatisticsResponse getFeatureStatistics(GetFeatureStatisticsRequest request)
      throws IOException {
    StatisticsRetriever statisticsRetriever = getStatisticsRetriever(request.getStore());
    FeatureSetSpec featureSetSpec = getFeatureSetSpec(request.getFeatureSetId());
    List<String> features = request.getFeatureIdsList();
    if (features.size() == 0) {
      features =
          featureSetSpec.getFeaturesList().stream()
              .map(FeatureSpec::getName)
              .collect(Collectors.toList());
    }
    List<List<FeatureNameStatistics>> featureNameStatisticsList = new ArrayList<>();
    if (request.getDatasetIdsCount() == 0) {
      // retrieve by date
      long timestamp = request.getStartDate().getSeconds();
      while (timestamp < request.getEndDate().getSeconds()) {
        List<FeatureNameStatistics> featureNameStatistics =
            getFeatureNameStatisticsByDate(
                statisticsRetriever, featureSetSpec, features, timestamp);
        featureNameStatisticsList.add(featureNameStatistics);
        timestamp += 86400; // advance by a day
      }
    } else {
      // retrieve by dataset
      for (String datasetId : request.getDatasetIdsList()) {
        List<FeatureNameStatistics> featureNameStatistics =
            getFeatureNameStatisticsByDataset(
                statisticsRetriever, featureSetSpec, features, datasetId);
        featureNameStatisticsList.add(featureNameStatistics);
      }
    }
    List<FeatureNameStatistics> featureNameStatistics = mergeStatistics(featureNameStatisticsList);
    return GetFeatureStatisticsResponse.newBuilder()
        .setDatasetFeatureStatisticsList(
            DatasetFeatureStatisticsList.newBuilder()
                .addDatasets(
                    DatasetFeatureStatistics.newBuilder().addAllFeatures(featureNameStatistics)))
        .build();
  }

  private List<FeatureNameStatistics> getFeatureNameStatisticsByDataset(
      StatisticsRetriever statisticsRetriever,
      FeatureSetSpec featureSetSpec,
      List<String> features,
      String datasetId)
      throws IOException {
    List<FeatureNameStatistics> featureNameStatistics = new ArrayList<>();
    List<String> featuresMissingStats = new ArrayList<>();
    for (String featureName : features) {
      Feature feature =
          new Feature(
              new FieldId(
                  featureSetSpec.getProject(),
                  featureSetSpec.getName(),
                  featureSetSpec.getVersion(),
                  featureName));
      Optional<FeatureStatistics> cachedFeatureStatistics =
          featureStatisticsRepository.findFeatureStatisticsByFeatureAndDatasetId(
              feature, datasetId);
      if (cachedFeatureStatistics.isPresent()) {
        featureNameStatistics.add(cachedFeatureStatistics.get().toProto());
      } else {
        featuresMissingStats.add(featureName);
      }
    }
    if (featuresMissingStats.size() > 0) {
      FeatureSetStatistics featureStatistics =
          statisticsRetriever.getFeatureStatistics(featureSetSpec, featuresMissingStats, datasetId);
      for (FeatureNameStatistics stat : featureStatistics.getFeatureNameStatistics()) {
        featureStatisticsRepository.save(
            FeatureStatistics.fromProto(
                featureSetSpec.getProject(),
                featureSetSpec.getName(),
                featureSetSpec.getVersion(),
                stat,
                datasetId));
      }
      featureNameStatistics.addAll(featureStatistics.getFeatureNameStatistics());
    }
    return featureNameStatistics;
  }

  private List<FeatureNameStatistics> getFeatureNameStatisticsByDate(
      StatisticsRetriever statisticsRetriever,
      FeatureSetSpec featureSetSpec,
      List<String> features,
      long timestamp)
      throws IOException {
    Date date = Date.from(Instant.ofEpochSecond(timestamp));
    List<FeatureNameStatistics> featureNameStatistics = new ArrayList<>();
    List<String> featuresMissingStats = new ArrayList<>();
    for (String featureName : features) {
      Feature feature =
          new Feature(
              new FieldId(
                  featureSetSpec.getProject(),
                  featureSetSpec.getName(),
                  featureSetSpec.getVersion(),
                  featureName));
      Optional<FeatureStatistics> cachedFeatureStatistics =
          featureStatisticsRepository.findFeatureStatisticsByFeatureAndDate(feature, date);
      if (cachedFeatureStatistics.isPresent()) {
        featureNameStatistics.add(cachedFeatureStatistics.get().toProto());
      } else {
        featuresMissingStats.add(featureName);
      }
    }
    if (featuresMissingStats.size() > 0) {
      FeatureSetStatistics featureStatistics =
          statisticsRetriever.getFeatureStatistics(
              featureSetSpec,
              featuresMissingStats,
              Timestamp.newBuilder().setSeconds(timestamp).build());
      for (FeatureNameStatistics stat : featureStatistics.getFeatureNameStatistics()) {
        featureStatisticsRepository.save(
            FeatureStatistics.fromProto(
                featureSetSpec.getProject(),
                featureSetSpec.getName(),
                featureSetSpec.getVersion(),
                stat,
                date));
      }
      featureNameStatistics.addAll(featureStatistics.getFeatureNameStatistics());
    }
    return featureNameStatistics;
  }

  private StatisticsRetriever getStatisticsRetriever(String storeName)
      throws InvalidProtocolBufferException {
    Store store = storeRepository.getOne(storeName).toProto();
    if (store.getType() != StoreType.BIGQUERY) {
      throw new IllegalArgumentException("Batch statistics are only supported for BigQuery stores");
    }
    return BigQueryStatisticsRetriever.newBuilder()
        .setProjectId(store.getBigqueryConfig().getProjectId())
        .setDatasetId(store.getBigqueryConfig().getDatasetId())
        .setBigquery(BigQueryOptions.getDefaultInstance().getService())
        .build();
  }

  private FeatureSetSpec getFeatureSetSpec(String featureSetId)
      throws InvalidProtocolBufferException {
    String[] split = featureSetId.split("/");
    String project = split[0];
    split = split[1].split(":");
    FeatureSet featureSet =
        specService
            .getFeatureSet(
                GetFeatureSetRequest.newBuilder()
                    .setProject(project)
                    .setName(split[0])
                    .setVersion(Integer.parseInt(split[1]))
                    .build())
            .getFeatureSet();
    return featureSet.getSpec();
  }

  @VisibleForTesting
  public List<FeatureNameStatistics> mergeStatistics(
      List<List<FeatureNameStatistics>> featureNameStatistics) {
    List<FeatureNameStatistics> unnestedList = new ArrayList<>();

    featureNameStatistics.forEach(unnestedList::addAll);
    Map<Path, List<FeatureNameStatistics>> groupByPath =
        unnestedList.stream()
            .collect(Collectors.groupingBy(FeatureNameStatistics::getPath, Collectors.toList()));

    List<FeatureNameStatistics> merged = new ArrayList<>();
    for (Path key : groupByPath.keySet()) {
      List<FeatureNameStatistics> featureNameStatisticsForKey = groupByPath.get(key);
      if (featureNameStatisticsForKey.size() == 1) {
        merged.add(featureNameStatisticsForKey.get(0));
      } else {
        switch (featureNameStatisticsForKey.get(0).getType()) {
          case INT:
          case FLOAT:
            merged.add(mergeNumStatistics(featureNameStatisticsForKey));
            break;
          case STRING:
            merged.add(mergeCategoricalStatistics(groupByPath.get(key)));
            break;
          case BYTES:
            merged.add(mergeByteStatistics(groupByPath.get(key)));
            break;
          case STRUCT:
            merged.add(mergeStructStats(groupByPath.get(key)));
            break;
          default:
            throw new IllegalArgumentException(
                "Statistics are only supported for string, boolean, bytes and numeric features");
        }
      }
    }
    return merged;
  }

  private FeatureNameStatistics mergeStructStats(
      List<FeatureNameStatistics> featureNameStatisticsList) {
    Builder mergedFeatureNameStatistics =
        FeatureNameStatistics.newBuilder()
            .setPath(featureNameStatisticsList.get(0).getPath())
            .setType(featureNameStatisticsList.get(0).getType());

    long totalCount = 0;
    long missingCount = 0;
    long totalNumValues = 0;
    long maxNumValues =
        featureNameStatisticsList.get(0).getStructStats().getCommonStats().getMaxNumValues();
    long minNumValues =
        featureNameStatisticsList.get(0).getStructStats().getCommonStats().getMinNumValues();

    for (FeatureNameStatistics featureNameStatistics : featureNameStatisticsList) {
      StructStatistics structStats = featureNameStatistics.getStructStats();
      totalCount += structStats.getCommonStats().getNumNonMissing();
      missingCount += structStats.getCommonStats().getNumMissing();
      totalNumValues +=
          structStats.getCommonStats().getAvgNumValues()
              * structStats.getCommonStats().getNumNonMissing();
      maxNumValues = max(maxNumValues, structStats.getCommonStats().getMaxNumValues());
      minNumValues = min(minNumValues, structStats.getCommonStats().getMinNumValues());
    }

    StructStatistics mergedStructStatistics =
        StructStatistics.newBuilder()
            .setCommonStats(
                CommonStatistics.newBuilder()
                    .setTotNumValues(totalCount)
                    .setNumNonMissing(totalCount)
                    .setAvgNumValues((float) totalNumValues / totalCount)
                    .setMaxNumValues(maxNumValues)
                    .setMinNumValues(minNumValues)
                    .setNumMissing(missingCount))
            .build();

    return mergedFeatureNameStatistics.setStructStats(mergedStructStatistics).build();
  }

  private FeatureNameStatistics mergeNumStatistics(
      List<FeatureNameStatistics> featureNameStatisticsList) {
    Builder mergedFeatureNameStatistics =
        FeatureNameStatistics.newBuilder()
            .setPath(featureNameStatisticsList.get(0).getPath())
            .setType(featureNameStatisticsList.get(0).getType());

    FeatureNameStatistics first = featureNameStatisticsList.remove(0);
    double max = first.getNumStats().getMax();
    double min = first.getNumStats().getMin();
    double var = pow(first.getNumStats().getStdDev(), 2);
    long totalCount = first.getNumStats().getCommonStats().getNumNonMissing();
    double totalVal = totalCount * first.getNumStats().getMean();
    long missingCount = first.getNumStats().getCommonStats().getNumMissing();
    long zeroes = first.getNumStats().getNumZeros();

    for (FeatureNameStatistics featureNameStatistics : featureNameStatisticsList) {
      NumericStatistics numStats = featureNameStatistics.getNumStats();
      max = max(numStats.getMax(), max);
      min = min(numStats.getMin(), min);
      long count = numStats.getCommonStats().getNumNonMissing();
      double sampleVar = pow(numStats.getStdDev(), 2);
      float aggMean = (float) totalVal / totalCount;
      var = getVar(var, totalCount, aggMean, sampleVar, count, numStats.getMean());
      totalVal += numStats.getMean() * count;
      totalCount += count;
      missingCount += numStats.getCommonStats().getNumMissing();
      zeroes += numStats.getNumZeros();
    }
    NumericStatistics mergedNumericStatistics =
        NumericStatistics.newBuilder()
            .setMax(max)
            .setMin(min)
            .setMean(totalVal / totalCount)
            .setNumZeros(zeroes)
            .setStdDev(sqrt(var))
            .setCommonStats(
                CommonStatistics.newBuilder()
                    .setTotNumValues(totalCount)
                    .setNumNonMissing(totalCount)
                    .setAvgNumValues(1)
                    .setMaxNumValues(1)
                    .setMinNumValues(1)
                    .setNumMissing(missingCount))
            .build();
    return mergedFeatureNameStatistics.setNumStats(mergedNumericStatistics).build();
  }

  // Aggregation of sample variance follows the formula described here:
  // https://www.tandfonline.com/doi/abs/10.1080/00031305.2014.966589
  private double getVar(
      double s1Var, long s1Count, double s1Mean, double s2Var, long s2Count, double s2Mean) {
    long totalCount = s1Count + s2Count;
    return ((s1Count - 1) * s1Var
            + (s2Count - 1) * s2Var
            + ((float) s1Count * s2Count / totalCount) * pow(s1Mean - s2Mean, 2))
        / (s1Count + s2Count - 1);
  }

  private FeatureNameStatistics mergeCategoricalStatistics(
      List<FeatureNameStatistics> featureNameStatisticsList) {
    Builder mergedFeatureNameStatistics =
        FeatureNameStatistics.newBuilder()
            .setPath(featureNameStatisticsList.get(0).getPath())
            .setType(featureNameStatisticsList.get(0).getType());
    long totalCount = 0;
    long missingCount = 0;
    long totalLen = 0;
    for (FeatureNameStatistics featureNameStatistics : featureNameStatisticsList) {
      StringStatistics stringStats = featureNameStatistics.getStringStats();
      totalCount += stringStats.getCommonStats().getNumNonMissing();
      missingCount += stringStats.getCommonStats().getNumMissing();
      totalLen += stringStats.getAvgLength() * stringStats.getCommonStats().getNumNonMissing();
    }
    StringStatistics mergedStringStatistics =
        StringStatistics.newBuilder()
            .setAvgLength((float) totalLen / totalCount)
            .setCommonStats(
                CommonStatistics.newBuilder()
                    .setTotNumValues(totalCount)
                    .setNumNonMissing(totalCount)
                    .setAvgNumValues(1)
                    .setMaxNumValues(1)
                    .setMinNumValues(1)
                    .setNumMissing(missingCount))
            .build();
    return mergedFeatureNameStatistics.setStringStats(mergedStringStatistics).build();
  }

  private FeatureNameStatistics mergeByteStatistics(
      List<FeatureNameStatistics> featureNameStatisticsList) {
    Builder mergedFeatureNameStatistics =
        FeatureNameStatistics.newBuilder()
            .setPath(featureNameStatisticsList.get(0).getPath())
            .setType(featureNameStatisticsList.get(0).getType());

    long totalCount = 0;
    long missingCount = 0;
    float totalNumBytes = 0;
    float maxNumBytes = featureNameStatisticsList.get(0).getBytesStats().getMaxNumBytes();
    float minNumBytes = featureNameStatisticsList.get(0).getBytesStats().getMinNumBytes();

    for (FeatureNameStatistics featureNameStatistics : featureNameStatisticsList) {
      BytesStatistics bytesStats = featureNameStatistics.getBytesStats();
      totalCount += bytesStats.getCommonStats().getNumNonMissing();
      missingCount += bytesStats.getCommonStats().getNumMissing();
      totalNumBytes += bytesStats.getAvgNumBytes() * bytesStats.getCommonStats().getNumNonMissing();
      maxNumBytes = max(maxNumBytes, bytesStats.getMaxNumBytes());
      minNumBytes = min(minNumBytes, bytesStats.getMinNumBytes());
    }

    BytesStatistics mergedBytesStatistics =
        BytesStatistics.newBuilder()
            .setAvgNumBytes(totalNumBytes / totalCount)
            .setMinNumBytes(minNumBytes)
            .setMaxNumBytes(maxNumBytes)
            .setCommonStats(
                CommonStatistics.newBuilder()
                    .setTotNumValues(totalCount)
                    .setNumNonMissing(totalCount)
                    .setAvgNumValues(1)
                    .setMaxNumValues(1)
                    .setMinNumValues(1)
                    .setNumMissing(missingCount))
            .build();

    return mergedFeatureNameStatistics.setBytesStats(mergedBytesStatistics).build();
  }
}
