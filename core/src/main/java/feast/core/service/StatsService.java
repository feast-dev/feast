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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.FeatureStatisticsRepository;
import feast.core.dao.StoreRepository;
import feast.core.exception.RetrievalException;
import feast.core.model.*;
import feast.core.model.Feature;
import feast.proto.core.CoreServiceProto.GetFeatureStatisticsRequest;
import feast.proto.core.CoreServiceProto.GetFeatureStatisticsResponse;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.storage.api.statistics.FeatureStatistics;
import feast.storage.api.statistics.StatisticsRetriever;
import feast.storage.connectors.bigquery.statistics.BigQueryStatisticsRetriever;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.tensorflow.metadata.v0.*;
import org.tensorflow.metadata.v0.FeatureNameStatistics.Builder;

/** Facilitates the retrieval of feature set statistics from historical stores. */
@Slf4j
@Service
public class StatsService {

  private StoreRepository storeRepository;
  private FeatureStatisticsRepository featureStatisticsRepository;
  private FeatureSetRepository featureSetRepository;

  @Autowired
  public StatsService(
      StoreRepository storeRepository,
      FeatureStatisticsRepository featureStatisticsRepository,
      FeatureSetRepository featureSetRepository) {
    this.storeRepository = storeRepository;
    this.featureStatisticsRepository = featureStatisticsRepository;
    this.featureSetRepository = featureSetRepository;
  }

  /**
   * Get {@link DatasetFeatureStatistics} for the requested feature set in the provided datasets or
   * date range for the store provided. The {@link DatasetFeatureStatistics} will contain a list of
   * {@link FeatureNameStatistics} for each feature requested. Results retrieved will be cached
   * indefinitely. To force Feast to recompute the statistics, set forceRefresh to true.
   *
   * <p>Only one of ingestionIds or startDate/endDate should be provided. If both are provided, the
   * former will be used over the latter.
   *
   * <p>If multiple datasetIds or if the date ranges over a few days, statistics will be retrieved
   * for each single unit (dataset id or day) and results aggregated across that set. As a result of
   * this, in such a scenario, statistics that cannot be aggregated will be dropped. This includes
   * all histograms and quantiles, unique values, and top value counts.
   *
   * @param request {@link GetFeatureStatisticsRequest} containing feature set name, subset of
   *     features, dataset ids or date range, and store to retrieve the data from.
   * @return {@link GetFeatureStatisticsResponse} containing {@link DatasetFeatureStatistics} with
   *     the feature statistics requested.
   * @throws IOException
   */
  @Transactional
  public GetFeatureStatisticsResponse getFeatureStatistics(GetFeatureStatisticsRequest request)
      throws IOException {

    // Validate the request
    validateRequest(request);

    // Get the stats retriever for the store requested
    StatisticsRetriever statisticsRetriever = getStatisticsRetriever(request.getStore());

    // 1. Retrieve the feature set spec from the db
    FeatureSet featureSet = getFeatureSet(request.getFeatureSetId());
    if (featureSet == null) {
      throw new IllegalArgumentException(
          String.format(
              "Illegal request. Unable to find feature set %s", request.getFeatureSetId()));
    }

    // 2. Filter out the features requested by the user. If none are provided,
    // use all features in the feature set.
    List<String> features = request.getFeaturesList();
    if (features.size() == 0) {
      features =
          featureSet.getFeatures().stream()
              .filter(feature -> !feature.isArchived())
              .map(Feature::getName)
              .collect(Collectors.toList());
    }

    // 3. Retrieve the statistics from the StatsRetriever.
    List<List<FeatureNameStatistics>> featureNameStatisticsList = new ArrayList<>();
    if (request.getIngestionIdsCount() == 0) {
      Timestamp endDate = request.getEndDate();
      Timestamp startDate = request.getStartDate();
      // If no dataset provided, retrieve by date

      long timestamp = startDate.getSeconds();
      while (timestamp < endDate.getSeconds()) {
        List<FeatureNameStatistics> featureNameStatistics =
            getFeatureNameStatisticsByDate(
                statisticsRetriever, featureSet, features, timestamp, request.getForceRefresh());
        featureNameStatisticsList.add(featureNameStatistics);
        timestamp += 86400; // advance by a day
      }
      if (featureNameStatisticsList.size() == 0) {
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
        DateTime startDateTime = new DateTime(startDate.getSeconds() * 1000, DateTimeZone.UTC);
        DateTime endDateTime = new DateTime(endDate.getSeconds() * 1000, DateTimeZone.UTC);
        throw new RetrievalException(
            String.format(
                "Unable to find any data over provided dates [%s, %s)",
                fmt.print(startDateTime), fmt.print(endDateTime)));
      }
    } else {
      // else, retrieve by dataset
      for (String datasetId : request.getIngestionIdsList()) {
        List<FeatureNameStatistics> featureNameStatistics =
            getFeatureNameStatisticsByDataset(
                statisticsRetriever, featureSet, features, datasetId, request.getForceRefresh());
        featureNameStatisticsList.add(featureNameStatistics);
        if (featureNameStatisticsList.size() == 0) {
          throw new RetrievalException(
              String.format(
                  "Unable to find any data over provided data sets %s",
                  request.getIngestionIdsList()));
        }
      }
    }

    // Merge statistics values across days/datasets
    List<FeatureNameStatistics> featureNameStatistics = mergeStatistics(featureNameStatisticsList);
    long totalCount = getTotalCount(featureNameStatistics.get(0));
    return GetFeatureStatisticsResponse.newBuilder()
        .setDatasetFeatureStatisticsList(
            DatasetFeatureStatisticsList.newBuilder()
                .addDatasets(
                    DatasetFeatureStatistics.newBuilder()
                        .setNumExamples(totalCount)
                        .addAllFeatures(featureNameStatistics)))
        .build();
  }

  /**
   * Get {@link FeatureNameStatistics} by dataset id.
   *
   * @param statisticsRetriever {@link StatisticsRetriever} corresponding to the store to get the
   *     data from.
   * @param featureSet {@link FeatureSet} requested by the user
   * @param features features to retrieve
   * @param datasetId dataset id to subset the data by
   * @param forceRefresh whether to override the values in the cache
   * @return {@link FeatureNameStatistics} for the data within the dataset id provided
   * @throws IOException
   */
  private List<FeatureNameStatistics> getFeatureNameStatisticsByDataset(
      StatisticsRetriever statisticsRetriever,
      FeatureSet featureSet,
      List<String> features,
      String datasetId,
      boolean forceRefresh)
      throws IOException {
    List<FeatureNameStatistics> featureNameStatistics = new ArrayList<>();
    List<String> featuresMissingStats = new ArrayList<>();
    Map<String, Feature> featureNameToFeature =
        featureSet.getFeatures().stream().collect(Collectors.toMap(Feature::getName, f -> f));

    // For each feature requested, check if statistics already exist in the cache
    // If not refreshing data in the cache, retrieve the cached data and add it to the
    // list of FeatureNameStatistics for this dataset.
    // Else, add to the list of features we still need to retrieve statistics for.
    for (String featureName : features) {
      Feature feature = featureNameToFeature.get(featureName);
      Optional<feast.core.model.FeatureStatistics> cachedFeatureStatistics = Optional.empty();
      if (!forceRefresh) {
        cachedFeatureStatistics =
            featureStatisticsRepository.findFeatureStatisticsByFeatureAndDatasetId(
                feature, datasetId);
      }
      if (cachedFeatureStatistics.isPresent()) {
        featureNameStatistics.add(cachedFeatureStatistics.get().toProto());
      } else {
        featuresMissingStats.add(featureName);
      }
    }

    // Retrieve the balance of statistics after checking the cache, and add it to the
    // list of FeatureNameStatistics.
    if (featuresMissingStats.size() > 0) {
      FeatureStatistics featureSetStatistics =
          statisticsRetriever.getFeatureStatistics(
              featureSet.toProto().getSpec(), featuresMissingStats, datasetId);

      // Persist the newly retrieved statistics in the cache.
      for (FeatureNameStatistics stat : featureSetStatistics.getFeatureNameStatistics()) {
        if (isEmpty(stat)) {
          continue;
        }
        Feature feature = featureNameToFeature.get(stat.getName());
        feast.core.model.FeatureStatistics featureStatistics =
            feast.core.model.FeatureStatistics.createForDataset(feature, stat, datasetId);
        Optional<feast.core.model.FeatureStatistics> existingRecord =
            featureStatisticsRepository.findFeatureStatisticsByFeatureAndDatasetId(
                featureStatistics.getFeature(), datasetId);
        existingRecord.ifPresent(statistics -> featureStatistics.setId(statistics.getId()));
        featureStatisticsRepository.save(featureStatistics);
        featureNameStatistics.add(stat);
      }
    }
    return featureNameStatistics;
  }

  /**
   * Get {@link FeatureNameStatistics} by date.
   *
   * @param statisticsRetriever {@link StatisticsRetriever} corresponding to the store to get the
   *     data from.
   * @param featureSet {@link FeatureSet} requested by the user
   * @param features features to retrieve
   * @param timestamp timestamp of the date to subset the data
   * @param forceRefresh whether to override the values in the cache
   * @return {@link FeatureNameStatistics} for the data within the dataset id provided
   * @throws IOException
   */
  private List<FeatureNameStatistics> getFeatureNameStatisticsByDate(
      StatisticsRetriever statisticsRetriever,
      FeatureSet featureSet,
      List<String> features,
      long timestamp,
      boolean forceRefresh)
      throws IOException {
    Date date = Date.from(Instant.ofEpochSecond(timestamp));
    List<FeatureNameStatistics> featureNameStatistics = new ArrayList<>();
    List<String> featuresMissingStats = new ArrayList<>();
    Map<String, Feature> featureNameToFeature =
        featureSet.getFeatures().stream().collect(Collectors.toMap(Feature::getName, f -> f));

    // For each feature requested, check if statistics already exist in the cache
    // If not refreshing data in the cache, retrieve the cached data and add it to the
    // list of FeatureNameStatistics for this date.
    // Else, add to the list of features we still need to retrieve statistics for.
    for (String featureName : features) {
      Feature feature = featureNameToFeature.get(featureName);
      Optional<feast.core.model.FeatureStatistics> cachedFeatureStatistics = Optional.empty();
      if (!forceRefresh) {
        cachedFeatureStatistics =
            featureStatisticsRepository.findFeatureStatisticsByFeatureAndDate(feature, date);
      }
      if (cachedFeatureStatistics.isPresent()) {
        featureNameStatistics.add(cachedFeatureStatistics.get().toProto());
      } else {
        featuresMissingStats.add(featureName);
      }
    }

    // Retrieve the balance of statistics after checking the cache, and add it to the
    // list of FeatureNameStatistics.
    if (featuresMissingStats.size() > 0) {
      FeatureStatistics featureSetStatistics =
          statisticsRetriever.getFeatureStatistics(
              featureSet.toProto().getSpec(),
              featuresMissingStats,
              Timestamp.newBuilder().setSeconds(timestamp).build());

      // Persist the newly retrieved statistics in the cache.
      for (FeatureNameStatistics stat : featureSetStatistics.getFeatureNameStatistics()) {
        if (isEmpty(stat)) {
          continue;
        }
        Feature feature = featureNameToFeature.get(stat.getName());
        feast.core.model.FeatureStatistics featureStatistics =
            feast.core.model.FeatureStatistics.createForDate(feature, stat, date);
        Optional<feast.core.model.FeatureStatistics> existingRecord =
            featureStatisticsRepository.findFeatureStatisticsByFeatureAndDate(
                featureStatistics.getFeature(), date);
        existingRecord.ifPresent(statistics -> featureStatistics.setId(statistics.getId()));
        featureStatisticsRepository.save(featureStatistics);
        featureNameStatistics.add(stat);
      }
    }
    return featureNameStatistics;
  }

  /**
   * Get the {@link StatisticsRetriever} corresponding to the store name provided.
   *
   * @param storeName name of the store to retrieve statistics from
   * @return {@link StatisticsRetriever}
   */
  StatisticsRetriever getStatisticsRetriever(String storeName)
      throws InvalidProtocolBufferException {
    Store store =
        storeRepository
            .findById(storeName)
            .orElseThrow(
                () ->
                    new RetrievalException(
                        String.format("Could not find store with name %s", storeName)));
    StoreProto.Store storeProto = store.toProto();
    if (storeProto.getType() != StoreType.BIGQUERY) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid store %s with type %s specified. Batch statistics are only supported for BigQuery stores",
              store.getName(), store.getType()));
    }
    return BigQueryStatisticsRetriever.create(storeProto.getBigqueryConfig());
  }

  private FeatureSet getFeatureSet(String featureSetId) {
    String[] split = featureSetId.split("/");
    String project;
    String featureSetName;
    if (split.length == 1) {
      project = Project.DEFAULT_NAME;
      featureSetName = split[0];
    } else {
      project = split[0];
      featureSetName = split[1];
    }
    FeatureSet featureSet =
        featureSetRepository.findFeatureSetByNameAndProject_Name(featureSetName, project);
    return featureSet;
  }

  /**
   * Merge feature statistics by name. This method is used to merge statistics retrieved over
   * multiple days or datasets.
   *
   * @param featureNameStatistics {@link FeatureNameStatistics} retrieved from the store
   * @return Merged list of {@link FeatureNameStatistics} by name
   */
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
                    .setTotNumValues(totalNumValues)
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

  private long getTotalCount(FeatureNameStatistics featureNameStatistics) {
    CommonStatistics commonStats;
    switch (featureNameStatistics.getType()) {
      case STRUCT:
        commonStats = featureNameStatistics.getStructStats().getCommonStats();
        break;
      case STRING:
        commonStats = featureNameStatistics.getStringStats().getCommonStats();
        break;
      case BYTES:
        commonStats = featureNameStatistics.getBytesStats().getCommonStats();
        break;
      case FLOAT:
      case INT:
        commonStats = featureNameStatistics.getNumStats().getCommonStats();
        break;
      default:
        throw new RuntimeException("Unable to extract dataset size; Invalid type provided");
    }
    return commonStats.getNumNonMissing() + commonStats.getNumMissing();
  }

  private void validateRequest(GetFeatureStatisticsRequest request) {
    if (request.getIngestionIdsCount() == 0) {
      Timestamp startDate = request.getStartDate();
      Timestamp endDate = request.getEndDate();
      if (!request.hasStartDate() || !request.hasEndDate()) {
        throw new IllegalArgumentException(
            "Invalid request. Either provide dataset ids to retrieve statistics over, or a start date and end date.");
      }
      if (endDate.getSeconds() < startDate.getSeconds()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid request. Start timestamp %d is greater than the end timestamp %d",
                startDate.getSeconds(), endDate.getSeconds()));
      }
    }
  }

  private boolean isEmpty(FeatureNameStatistics featureNameStatistics) {
    switch (featureNameStatistics.getType()) {
      case STRUCT:
        return featureNameStatistics
            .getStructStats()
            .getCommonStats()
            .equals(CommonStatistics.getDefaultInstance());
      case STRING:
        return featureNameStatistics
            .getStringStats()
            .getCommonStats()
            .equals(CommonStatistics.getDefaultInstance());
      case BYTES:
        return featureNameStatistics
            .getBytesStats()
            .getCommonStats()
            .equals(CommonStatistics.getDefaultInstance());
      case FLOAT:
      case INT:
        return featureNameStatistics
            .getNumStats()
            .getCommonStats()
            .equals(CommonStatistics.getDefaultInstance());
      default:
        return true;
    }
  }
}
