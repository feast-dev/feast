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
package feast.core.model;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.*;
import java.util.Date;
import java.util.List;
import javax.persistence.*;
import javax.persistence.Entity;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.tensorflow.metadata.v0.*;

@NoArgsConstructor
@Getter
@Setter
@Entity
@Table(name = "statistics")
public class Statistics {
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private int id;

  // Only one of these fields should be populated.
  private String datasetId;
  private Date date;

  // General statistics
  private String featureType;
  private long count;
  private long numMissing;
  private long minNumValues;
  private long maxNumValues;
  private float avgNumValues;
  private long totalNumValues;
  private byte[] numValuesHistogram;

  // Numeric statistics
  private double mean;
  private double stdev;
  private long zeroes;
  private double min;
  private double max;
  private double median;
  private byte[] numericValueHistogram;
  private byte[] numericValueQuantiles;

  // String statistics
  @Column(name = "n_unique")
  private long unique;

  private float averageLength;
  private byte[] rankHistogram;
  private byte[] topValues;

  // Byte statistics
  private float minBytes;
  private float maxBytes;
  private float avgBytes;

  // Instantiates a Statistics object from a tensorflow metadata FeatureNameStatistics object and a
  // dataset ID.
  public static Statistics fromProto(FeatureNameStatistics featureNameStatistics, String datasetId)
      throws IOException {
    Statistics statistics = Statistics.fromProto(featureNameStatistics);
    statistics.setDatasetId(datasetId);
    return statistics;
  }

  // Instantiates a Statistics object from a tensorflow metadata FeatureNameStatistics object and a
  // date.
  public static Statistics fromProto(FeatureNameStatistics featureNameStatistics, Date date)
      throws IOException {
    Statistics statistics = Statistics.fromProto(featureNameStatistics);
    statistics.setDate(date);
    return statistics;
  }

  public FeatureNameStatistics toProto() throws InvalidProtocolBufferException {
    FeatureNameStatistics.Builder featureNameStatisticsBuilder =
        FeatureNameStatistics.newBuilder().setType(FeatureNameStatistics.Type.valueOf(featureType));
    CommonStatistics commonStatistics =
        CommonStatistics.newBuilder()
            .setNumNonMissing(count - numMissing)
            .setNumMissing(numMissing)
            .setMaxNumValues(maxNumValues)
            .setMinNumValues(minNumValues)
            .setTotNumValues(totalNumValues)
            .setNumValuesHistogram(Histogram.parseFrom(numValuesHistogram))
            .build();

    switch (featureNameStatisticsBuilder.getType()) {
      case INT:
      case FLOAT:
        NumericStatistics numStats =
            NumericStatistics.newBuilder()
                .setCommonStats(commonStatistics)
                .setMean(mean)
                .setStdDev(stdev)
                .setNumZeros(zeroes)
                .setMin(min)
                .setMax(max)
                .setMedian(median)
                .addHistograms(Histogram.parseFrom(numericValueHistogram))
                .addHistograms(Histogram.parseFrom(numericValueQuantiles))
                .build();
        featureNameStatisticsBuilder.setNumStats(numStats);
      case STRING:
        StringStatistics.Builder stringStats =
            StringStatistics.newBuilder()
                .setCommonStats(commonStatistics)
                .setUnique(unique)
                .setAvgLength(averageLength)
                .setRankHistogram(RankHistogram.parseFrom(rankHistogram));
        try (ByteArrayInputStream bis = new ByteArrayInputStream(topValues)) {
          ObjectInputStream ois = new ObjectInputStream(bis);
          List<StringStatistics.FreqAndValue> freqAndValueList =
              (List<StringStatistics.FreqAndValue>) ois.readObject();
          stringStats.addAllTopValues(freqAndValueList);
        } catch (IOException | ClassNotFoundException e) {
          throw new InvalidProtocolBufferException(
              "Failed to parse field: StringStatistics.TopValues. Check if the value is malformed.");
        }
        featureNameStatisticsBuilder.setStringStats(stringStats);
      case BYTES:
        BytesStatistics bytesStats =
            BytesStatistics.newBuilder()
                .setCommonStats(commonStatistics)
                .setAvgNumBytes(avgBytes)
                .setMinNumBytes(minBytes)
                .setMaxNumBytes(maxBytes)
                .build();
        featureNameStatisticsBuilder.setBytesStats(bytesStats);
      case STRUCT:
        StructStatistics structStats =
            StructStatistics.newBuilder().setCommonStats(commonStatistics).build();
        featureNameStatisticsBuilder.setStructStats(structStats);
    }
    return featureNameStatisticsBuilder.build();
  }

  private static Statistics fromProto(FeatureNameStatistics featureNameStatistics)
      throws IOException, IllegalArgumentException {
    Statistics statistics = new Statistics();
    statistics.setFeatureType(featureNameStatistics.getType().toString());
    CommonStatistics commonStats;
    switch (featureNameStatistics.getType()) {
      case FLOAT:
      case INT:
        NumericStatistics numStats = featureNameStatistics.getNumStats();
        commonStats = numStats.getCommonStats();
        statistics.setMean(numStats.getMean());
        statistics.setStdev(numStats.getStdDev());
        statistics.setZeroes(numStats.getNumZeros());
        statistics.setMin(numStats.getMin());
        statistics.setMax(numStats.getMax());
        statistics.setMedian(numStats.getMedian());
        for (Histogram histogram : numStats.getHistogramsList()) {
          switch (histogram.getType()) {
            case STANDARD:
              statistics.setNumericValueHistogram(histogram.toByteArray());
            case QUANTILES:
              statistics.setNumericValueQuantiles(histogram.toByteArray());
            default:
              // invalid type, dropping the values
          }
        }
        break;
      case STRING:
        StringStatistics stringStats = featureNameStatistics.getStringStats();
        commonStats = stringStats.getCommonStats();
        statistics.setUnique(stringStats.getUnique());
        statistics.setAverageLength(stringStats.getAvgLength());
        statistics.setRankHistogram(stringStats.getRankHistogram().toByteArray());
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
          ObjectOutputStream oos = new ObjectOutputStream(bos);
          oos.writeObject(stringStats.getTopValuesList());
        }
        break;
      case BYTES:
        BytesStatistics bytesStats = featureNameStatistics.getBytesStats();
        commonStats = bytesStats.getCommonStats();
        statistics.setUnique(bytesStats.getUnique());
        statistics.setMaxBytes(bytesStats.getMaxNumBytes());
        statistics.setMinBytes(bytesStats.getMinNumBytes());
        statistics.setAvgBytes(bytesStats.getAvgNumBytes());
        break;
      case STRUCT:
        StructStatistics structStats = featureNameStatistics.getStructStats();
        commonStats = structStats.getCommonStats();
        break;
      default:
        throw new IllegalArgumentException("Feature statistics provided were of unknown type.");
    }
    statistics.setCount(commonStats.getNumMissing() + commonStats.getNumNonMissing());
    statistics.setNumMissing(commonStats.getNumMissing());
    statistics.setMinNumValues(commonStats.getMinNumValues());
    statistics.setMaxNumValues(commonStats.getMaxNumValues());
    statistics.setAvgNumValues(commonStats.getAvgNumValues());
    statistics.setTotalNumValues(commonStats.getTotNumValues());
    statistics.setNumValuesHistogram(commonStats.getNumValuesHistogram().toByteArray());

    return statistics;
  }
}
