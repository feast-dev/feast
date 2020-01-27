/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.ingestion.transform.metrics;

import com.google.auto.value.AutoValue;
import com.google.protobuf.util.Timestamps;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientException;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.EntitySpec.DomainInfoCase;
import feast.core.FeatureSetProto.EntitySpec.PresenceConstraintsCase;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.Value.ValCase;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.tensorflow.metadata.v0.FeaturePresence;
import org.tensorflow.metadata.v0.FloatDomain;
import org.tensorflow.metadata.v0.IntDomain;

@AutoValue
public abstract class WriteRowMetricsDoFn extends DoFn<KV<String, Iterable<FeatureRow>>, Void> {

  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(WriteRowMetricsDoFn.class);

  private static final String METRIC_PREFIX = "feast_ingestion";
  private static final String STORE_TAG_KEY = "feast_store";
  private static final String FEATURE_SET_PROJECT_TAG_KEY = "feast_project_name";
  private static final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  private static final String FEATURE_SET_VERSION_TAG_KEY = "feast_featureSet_version";
  private static final String FEATURE_NAME_TAG_KEY = "feast_feature_name";
  private static final String INGESTION_JOB_NAME_KEY = "feast_job_name";

  public abstract String getStoreName();

  public abstract String getStatsdHost();

  public abstract int getStatsdPort();

  public abstract Map<String, FeatureSet> getFeatureSetByRef();

  private StatsDClient statsd;

  public static Builder newBuilder() {
    return new AutoValue_WriteRowMetricsDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStoreName(String storeName);

    public abstract Builder setStatsdHost(String statsdHost);

    public abstract Builder setStatsdPort(int statsdPort);

    public abstract Builder setFeatureSetByRef(
        Map<String, FeatureSetProto.FeatureSet> featureSetByRef);

    public abstract WriteRowMetricsDoFn build();
  }

  @Setup
  public void setup() {
    try {
      statsd = new NonBlockingStatsDClient(METRIC_PREFIX, getStatsdHost(), getStatsdPort());
    } catch (StatsDClientException e) {
      LOG.warn("Failed to create StatsD client");
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    if (statsd == null) {
      LOG.warn(
          "No StatsD client available, maybe it failed to initialize. No FeatureRow metrics will be sent.");
      return;
    }

    String featureSetRef = c.element().getKey();
    if (featureSetRef == null) {
      return;
    }

    if (!getFeatureSetByRef().containsKey(featureSetRef)) {
      // FeatureRow has a reference not known by the ImportJob. Skip sending metrics.
      return;
    }

    String[] colonSplits = featureSetRef.split(":");
    if (colonSplits.length < 1) {
      LOG.warn(
          "FeatureRow has an invalid FeatureSet reference: " + featureSetRef
              + ". Expected format: PROJECT/FEATURE_SET:VERSION");
      return;
    }

    String[] slashSplits = colonSplits[0].split("/");
    if (slashSplits.length < 2) {
      LOG.warn(
          "FeatureRow has an invalid FeatureSet reference: " + featureSetRef
              + ". Expected format: PROJECT/FEATURE_SET:VERSION");
      return;
    }

    String featureSetProject = slashSplits[0];
    String featureSetName = slashSplits[1];
    String featureSetVersion = colonSplits[1];

    FeatureSet featureSet = getFeatureSetByRef().get(featureSetRef);
    Map<String, EntitySpec> entityNameToSpec = createEntityNameToSpecMap(featureSet);
    Map<String, FeatureSpec> featureNameToSpec = createFeatureNameToSpecMap(featureSet);
    Map<String, Integer> fieldNameToMissingCount = new HashMap<>();
    Map<String, DoubleSummaryStatistics> fieldNameToValueStat = new HashMap<>();

    String[] tags = new String[]{
        STORE_TAG_KEY + ":" + getStoreName(),
        FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
        FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
        FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
        INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName()
    };

    for (FeatureRow row : c.element().getValue()) {
      // All features in a FeatueRow have the same timestamp so lag metrics are recorded per row basis.
      long eventTimestamp = Timestamps.toMillis(row.getEventTimestamp());
      statsd.histogram("feature_row_lag_ms", System.currentTimeMillis() - eventTimestamp, tags);
      statsd.histogram("feature_row_event_time_epoch_ms", eventTimestamp, tags);
      statsd.count("feature_row_ingested_count", 1, tags);

      // Feature value, count and constraint metrics for each feature/entity in a FeatureRow.
      for (Field field : row.getFieldsList()) {
        String fieldName = field.getName();

        // Ensure the map objects have properly initialized value for every key.
        if (!fieldNameToMissingCount.containsKey(fieldName)) {
          fieldNameToMissingCount.put(fieldName, 0);
        }
        if (!fieldNameToValueStat.containsKey(fieldName)) {
          fieldNameToValueStat.put(fieldName, new DoubleSummaryStatistics());
        }

        tags = new String[]{
            STORE_TAG_KEY + ":" + getStoreName(),
            FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
            FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
            FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
            INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName(),
            FEATURE_NAME_TAG_KEY + ":" + fieldName,
        };

        Value val = field.getValue();
        ValCase valCase = val.getValCase();
        DoubleSummaryStatistics valueStat = fieldNameToValueStat.get(fieldName);

        switch (valCase) {
          case INT32_VAL:
            valueStat.accept(val.getInt32Val());
            fieldNameToValueStat.put(fieldName, valueStat);
            writeConstraintMetrics(entityNameToSpec, featureNameToSpec, fieldName, valCase, tags);
            break;
          case INT64_VAL:
            valueStat.accept(val.getInt64Val());
            fieldNameToValueStat.put(fieldName, valueStat);
            writeConstraintMetrics(entityNameToSpec, featureNameToSpec, fieldName, valCase, tags);
            break;
          case DOUBLE_VAL:
            valueStat.accept(val.getDoubleVal());
            fieldNameToValueStat.put(fieldName, valueStat);
            writeConstraintMetrics(entityNameToSpec, featureNameToSpec, fieldName, valCase, tags);
            break;
          case FLOAT_VAL:
            valueStat.accept(val.getFloatVal());
            fieldNameToValueStat.put(fieldName, valueStat);
            writeConstraintMetrics(entityNameToSpec, featureNameToSpec, fieldName, valCase, tags);
            break;
          case BOOL_VAL:
            valueStat.accept(val.getBoolVal() ? 1 : 0);
            fieldNameToValueStat.put(fieldName, valueStat);
            break;
          case BYTES_VAL:
          case STRING_VAL:
          case BYTES_LIST_VAL:
          case FLOAT_LIST_VAL:
          case STRING_LIST_VAL:
          case INT32_LIST_VAL:
          case INT64_LIST_VAL:
          case DOUBLE_LIST_VAL:
          case BOOL_LIST_VAL:
            break;
          case VAL_NOT_SET:
            Integer oldCount = fieldNameToMissingCount.get(fieldName);
            fieldNameToMissingCount.put(fieldName, oldCount + 1);
            break;
        }
      }
    }

    for (Entry<String, Integer> entry : fieldNameToMissingCount.entrySet()) {
      String fieldName = entry.getKey();
      Integer missingCount = entry.getValue();
      tags = new String[]{
          STORE_TAG_KEY + ":" + getStoreName(),
          FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName(),
          FEATURE_NAME_TAG_KEY + ":" + fieldName,
      };
      statsd.count("feature_value_missing_count", missingCount, tags);
    }

    for (Entry<String, DoubleSummaryStatistics> entry : fieldNameToValueStat.entrySet()) {
      String fieldName = entry.getKey();
      DoubleSummaryStatistics valueStat = entry.getValue();
      tags = new String[]{
          STORE_TAG_KEY + ":" + getStoreName(),
          FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName(),
          FEATURE_NAME_TAG_KEY + ":" + fieldName,
      };
      statsd.gauge("feature_value_min", valueStat.getMin(), tags);
      statsd.gauge("feature_value_max", valueStat.getMax(), tags);
      statsd.count("feature_value_count", valueStat.getCount(), tags);
    }
  }

  // Record the acceptable value and count for each feature according to the spec.
  // These can be used to compare against the actual values in the dashboarding / alerting tool.
  private void writeConstraintMetrics(Map<String, EntitySpec> entityNameToSpec,
      Map<String, FeatureSpec> featureNameToSpec, String fieldName, ValCase valCase,
      String[] tags) {
    switch (valCase) {
      case INT32_VAL:
      case INT64_VAL:
        if (entityNameToSpec.containsKey(fieldName)) {
          EntitySpec entitySpec = entityNameToSpec.get(fieldName);
          if (entitySpec.getDomainInfoCase().equals(DomainInfoCase.INT_DOMAIN)) {
            IntDomain intDomain = entitySpec.getIntDomain();
            statsd.gauge("feature_value_domain_min", intDomain.getMin(), tags);
            statsd.gauge("feature_value_domain_max", intDomain.getMax(), tags);
          }
          if (entitySpec.getPresenceConstraintsCase().equals(PresenceConstraintsCase.PRESENCE)) {
            FeaturePresence presence = entitySpec.getPresence();
            statsd.gauge("feature_presence_min_fraction", presence.getMinFraction(), tags);
            statsd.gauge("feature_presence_min_count", presence.getMinCount(), tags);
          }
        } else if (featureNameToSpec.containsKey(fieldName)) {
          FeatureSpec featureSpec = featureNameToSpec.get(fieldName);
          if (featureSpec.getDomainInfoCase().equals(FeatureSpec.DomainInfoCase.INT_DOMAIN)) {
            IntDomain intDomain = featureSpec.getIntDomain();
            statsd.gauge("feature_value_domain_min", intDomain.getMin(), tags);
            statsd.gauge("feature_value_domain_max", intDomain.getMax(), tags);
          }
          if (featureSpec.getPresenceConstraintsCase()
              .equals(FeatureSpec.PresenceConstraintsCase.PRESENCE)) {
            FeaturePresence presence = featureSpec.getPresence();
            statsd.gauge("feature_presence_min_fraction", presence.getMinFraction(), tags);
            statsd.gauge("feature_presence_min_count", presence.getMinCount(), tags);
          }
        }
        break;
      case DOUBLE_VAL:
      case FLOAT_VAL:
        if (entityNameToSpec.containsKey(fieldName)) {
          EntitySpec entitySpec = entityNameToSpec.get(fieldName);
          if (entitySpec.getDomainInfoCase().equals(DomainInfoCase.FLOAT_DOMAIN)) {
            FloatDomain floatDomain = entitySpec.getFloatDomain();
            statsd.gauge("feature_value_domain_min", floatDomain.getMin(), tags);
            statsd.gauge("feature_value_domain_max", floatDomain.getMax(), tags);
          }
          if (entitySpec.getPresenceConstraintsCase().equals(PresenceConstraintsCase.PRESENCE)) {
            FeaturePresence presence = entitySpec.getPresence();
            statsd.gauge("feature_presence_min_fraction", presence.getMinFraction(), tags);
            statsd.gauge("feature_presence_min_count", presence.getMinCount(), tags);
          }
        } else if (featureNameToSpec.containsKey(fieldName)) {
          FeatureSpec featureSpec = featureNameToSpec.get(fieldName);
          if (featureSpec.getDomainInfoCase().equals(FeatureSpec.DomainInfoCase.FLOAT_DOMAIN)) {
            FloatDomain floatDomain = featureSpec.getFloatDomain();
            statsd.gauge("feature_value_domain_min", floatDomain.getMin(), tags);
            statsd.gauge("feature_value_domain_max", floatDomain.getMax(), tags);
          }
          if (featureSpec.getPresenceConstraintsCase()
              .equals(FeatureSpec.PresenceConstraintsCase.PRESENCE)) {
            FeaturePresence presence = featureSpec.getPresence();
            statsd.gauge("feature_presence_min_fraction", presence.getMinFraction(), tags);
            statsd.gauge("feature_presence_min_count", presence.getMinCount(), tags);
          }
        }
        break;
      default:
        break;
    }

  }

  private Map<String, FeatureSpec> createFeatureNameToSpecMap(FeatureSet featureSet) {
    Map<String, FeatureSpec> featureSpecByName = new HashMap<>();
    if (featureSet == null) {
      return featureSpecByName;
    }

    featureSet.getSpec().getFeaturesList().forEach(featureSpec -> {
      featureSpecByName.put(featureSpec.getName(), featureSpec);
    });
    return featureSpecByName;
  }

  private Map<String, EntitySpec> createEntityNameToSpecMap(FeatureSet featureSet) {
    Map<String, EntitySpec> entitySpecByName = new HashMap<>();
    if (featureSet == null) {
      return entitySpecByName;
    }

    featureSet.getSpec().getEntitiesList().forEach(entitySpec -> {
      entitySpecByName.put(entitySpec.getName(), entitySpec);
    });
    return entitySpecByName;
  }
}
