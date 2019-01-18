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

package feast.ingestion.metrics;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.pubsub.FeastPubsubHelper;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsSink;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.GranularityProto.Granularity;

/**
 * MetricsSink class for dogfooding metrics back into feast as FeatureRows with Google Cloud PubSub.
 */
@Slf4j
public class FeastMetricsPubSubSink implements MetricsSink {

  private FeastPubsubHelper pubsubHelper;
  private String jobName;
  private String topicUrl;
  private Granularity.Enum granularity;

  public FeastMetricsPubSubSink(PipelineOptions pipelineOptions) {
    this.pubsubHelper = new FeastPubsubHelper(pipelineOptions);
    this.jobName = pipelineOptions.getJobName();
    FeastMetricsPubSubSinkOptions sinkOptions =
        pipelineOptions.as(FeastMetricsPubSubSinkOptions.class);
    this.topicUrl = sinkOptions.getMetricsPubSubSinkTopicUrl();

    String granularityString =
        Optional.ofNullable(sinkOptions.getMetricsPubSubSinkGranularity()).orElse("HOUR");
    this.granularity = Granularity.Enum.valueOf(granularityString.toUpperCase());

    Preconditions.checkNotNull(topicUrl, "FeastMetricsPubSubSink requires pubsub topic url");
  }

  /**
   * Publish metrics as FeatureRows to PubSub
   *
   * @param metricQueryResults
   * @throws Exception
   */
  @Override
  public void writeMetrics(MetricQueryResults metricQueryResults) throws Exception {
    try {
      for (MetricResult<Long> counter : metricQueryResults.getCounters()) {
        FeatureRow row = FeastMetrics.makeFeatureRow(counter, jobName, granularity);
        log.info(JsonFormat.printer().print(row));
        publish(topicUrl, row);
      }
    } catch (Throwable e) {
      log.error(e.getMessage(), e);
    }
  }

  void publish(String pubSubTopicUrl, FeatureRow row) throws IOException {
    TopicPath topicPath = PubsubClient.topicPathFromPath(pubSubTopicUrl);
    pubsubHelper.publish(topicPath, row);
  }

  public interface FeastMetricsPubSubSinkOptions extends PipelineOptions {
    @Description("PubSub topic to write stats to")
    String getMetricsPubSubSinkTopicUrl();

    void setMetricsPubSubSinkTopicUrl(String value);

    @Description(
        "Granularity to be set on published metrics FeatureRows, this effects storage"
            + " downstream,it does not indicate any aggregation as all metrics are totals.")
    String getMetricsPubSubSinkGranularity();

    void setMetricsPubSubSinkGranularity(String value);
  }

  @AutoService(PipelineOptionsRegistrar.class)
  public static class FeastMetricsPubSubSinkOptionsRegistrar implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return Collections.singleton(ImportJobPipelineOptions.class);
    }
  }
}
