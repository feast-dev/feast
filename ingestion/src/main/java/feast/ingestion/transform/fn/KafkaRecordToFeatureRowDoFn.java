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
package feast.ingestion.transform.fn;

import com.google.auto.value.AutoValue;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.storage.api.writer.FailedElement;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Base64;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.exception.ExceptionUtils;

@AutoValue
public abstract class KafkaRecordToFeatureRowDoFn
    extends DoFn<KafkaRecord<byte[], byte[]>, FeatureRow> {

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static KafkaRecordToFeatureRowDoFn.Builder newBuilder() {
    return new AutoValue_KafkaRecordToFeatureRowDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    public abstract KafkaRecordToFeatureRowDoFn build();
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    byte[] value = context.element().getKV().getValue();
    FeatureRow featureRow;

    try {
      featureRow = FeatureRow.parseFrom(value);
    } catch (InvalidProtocolBufferException e) {
      context.output(
          getFailureTag(),
          FailedElement.newBuilder()
              .setTransformName("KafkaRecordToFeatureRow")
              .setStackTrace(ExceptionUtils.getStackTrace(e))
              .setJobName(context.getPipelineOptions().getJobName())
              .setPayload(new String(Base64.getEncoder().encode(value)))
              .setErrorMessage(e.getMessage())
              .build());
      return;
    }
    context.output(featureRow);
  }
}
