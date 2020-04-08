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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.tensorflow.metadata.v0.*;

@NoArgsConstructor
@Getter
@Setter
@javax.persistence.Entity
@Table(
    name = "entity_statistics",
    indexes = {
      @Index(
          name = "idx_entity_statistics_entity",
          columnList = "project,feature_set,version,name"),
      @Index(name = "idx_entity_statistics_dataset_id", columnList = "datasetId"),
      @Index(name = "idx_entity_statistics_date", columnList = "date"),
    })
public class EntityStatistics {
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private int id;

  @ManyToOne
  @JoinColumns({
    @JoinColumn(name = "project", referencedColumnName = "project"),
    @JoinColumn(name = "feature_set", referencedColumnName = "feature_set"),
    @JoinColumn(name = "version", referencedColumnName = "version"),
    @JoinColumn(name = "name", referencedColumnName = "name")
  })
  private Entity entity;

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
  public static EntityStatistics fromProto(
      String project,
      String featureSetName,
      int version,
      FeatureNameStatistics featureNameStatistics,
      String datasetId)
      throws IOException {
    EntityStatistics featureStatistics = EntityStatistics.fromProto(featureNameStatistics);
    Entity entity = new Entity();
    entity.setId(
        new FieldId(project, featureSetName, version, featureNameStatistics.getPath().getStep(0)));
    featureStatistics.setEntity(entity);
    featureStatistics.setDatasetId(datasetId);
    return featureStatistics;
  }

  // Instantiates a Statistics object from a tensorflow metadata FeatureNameStatistics object and a
  // date.
  public static EntityStatistics fromProto(
      String project,
      String featureSetName,
      int version,
      FeatureNameStatistics featureNameStatistics,
      Date date)
      throws IOException {
    EntityStatistics entityStatistics = EntityStatistics.fromProto(featureNameStatistics);
    entityStatistics.setDate(date);
    Entity entity = new Entity();
    entity.setId(
        new FieldId(project, featureSetName, version, featureNameStatistics.getPath().getStep(0)));
    entityStatistics.setEntity(entity);
    return entityStatistics;
  }

  public FeatureNameStatistics toProto() throws InvalidProtocolBufferException {
    FeatureNameStatistics.Builder featureNameStatisticsBuilder =
        FeatureNameStatistics.newBuilder()
            .setType(FeatureNameStatistics.Type.valueOf(featureType))
            .setPath(Path.newBuilder().addStep(entity.getId().getName()));
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
        break;
      case STRING:
        StringStatistics.Builder stringStats =
            StringStatistics.newBuilder()
                .setCommonStats(commonStatistics)
                .setUnique(unique)
                .setAvgLength(averageLength);
        if (rankHistogram == null) {
          stringStats.setRankHistogram(RankHistogram.getDefaultInstance());
        } else {
          stringStats.setRankHistogram(RankHistogram.parseFrom(rankHistogram));
        }
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
        break;
      case BYTES:
        BytesStatistics bytesStats =
            BytesStatistics.newBuilder()
                .setCommonStats(commonStatistics)
                .setAvgNumBytes(avgBytes)
                .setMinNumBytes(minBytes)
                .setMaxNumBytes(maxBytes)
                .build();
        featureNameStatisticsBuilder.setBytesStats(bytesStats);
        break;
      case STRUCT:
        StructStatistics structStats =
            StructStatistics.newBuilder().setCommonStats(commonStatistics).build();
        featureNameStatisticsBuilder.setStructStats(structStats);
        break;
    }
    return featureNameStatisticsBuilder.build();
  }

  private static EntityStatistics fromProto(FeatureNameStatistics featureNameStatistics)
      throws IOException, IllegalArgumentException {
    EntityStatistics featureStatistics = new EntityStatistics();
    featureStatistics.setFeatureType(featureNameStatistics.getType().toString());
    CommonStatistics commonStats;
    switch (featureNameStatistics.getType()) {
      case FLOAT:
      case INT:
        NumericStatistics numStats = featureNameStatistics.getNumStats();
        commonStats = numStats.getCommonStats();
        featureStatistics.setMean(numStats.getMean());
        featureStatistics.setStdev(numStats.getStdDev());
        featureStatistics.setZeroes(numStats.getNumZeros());
        featureStatistics.setMin(numStats.getMin());
        featureStatistics.setMax(numStats.getMax());
        featureStatistics.setMedian(numStats.getMedian());
        for (Histogram histogram : numStats.getHistogramsList()) {
          switch (histogram.getType()) {
            case STANDARD:
              featureStatistics.setNumericValueHistogram(histogram.toByteArray());
            case QUANTILES:
              featureStatistics.setNumericValueQuantiles(histogram.toByteArray());
            default:
              // invalid type, dropping the values
          }
        }
        break;
      case STRING:
        StringStatistics stringStats = featureNameStatistics.getStringStats();
        commonStats = stringStats.getCommonStats();
        featureStatistics.setUnique(stringStats.getUnique());
        featureStatistics.setAverageLength(stringStats.getAvgLength());
        featureStatistics.setRankHistogram(stringStats.getRankHistogram().toByteArray());
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
          ObjectOutputStream oos = new ObjectOutputStream(bos);
          oos.writeObject(stringStats.getTopValuesList());
          featureStatistics.setTopValues(bos.toByteArray());
        }
        break;
      case BYTES:
        BytesStatistics bytesStats = featureNameStatistics.getBytesStats();
        commonStats = bytesStats.getCommonStats();
        featureStatistics.setUnique(bytesStats.getUnique());
        featureStatistics.setMaxBytes(bytesStats.getMaxNumBytes());
        featureStatistics.setMinBytes(bytesStats.getMinNumBytes());
        featureStatistics.setAvgBytes(bytesStats.getAvgNumBytes());
        break;
      case STRUCT:
        StructStatistics structStats = featureNameStatistics.getStructStats();
        commonStats = structStats.getCommonStats();
        break;
      default:
        throw new IllegalArgumentException("Feature statistics provided were of unknown type.");
    }
    featureStatistics.setCount(commonStats.getNumMissing() + commonStats.getNumNonMissing());
    featureStatistics.setNumMissing(commonStats.getNumMissing());
    featureStatistics.setMinNumValues(commonStats.getMinNumValues());
    featureStatistics.setMaxNumValues(commonStats.getMaxNumValues());
    featureStatistics.setAvgNumValues(commonStats.getAvgNumValues());
    featureStatistics.setTotalNumValues(commonStats.getTotNumValues());
    featureStatistics.setNumValuesHistogram(commonStats.getNumValuesHistogram().toByteArray());

    return featureStatistics;
  }
}
