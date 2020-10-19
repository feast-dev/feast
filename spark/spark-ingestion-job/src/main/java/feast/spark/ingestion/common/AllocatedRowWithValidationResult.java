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
package feast.spark.ingestion.common;

import com.google.auto.value.AutoValue;
import feast.proto.types.FeatureRowProto.FeatureRow;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;

@AutoValue
public abstract class AllocatedRowWithValidationResult implements Serializable {

  public abstract FeatureRow getFeatureRow();

  public abstract boolean isValid();

  public abstract List<String> getSubscribedStoreNames();

  @Nullable
  public abstract FailedElement getFailedElement();

  public static AllocatedRowWithValidationResult.Builder newBuilder() {
    return new AutoValue_AllocatedRowWithValidationResult.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract AllocatedRowWithValidationResult.Builder setFeatureRow(FeatureRow featureRow);

    public abstract AllocatedRowWithValidationResult.Builder setValid(boolean valid);

    public abstract AllocatedRowWithValidationResult.Builder setSubscribedStoreNames(
        List<String> subscribedStoreNames);

    public abstract AllocatedRowWithValidationResult.Builder setFailedElement(
        FailedElement statsdPort);

    public abstract AllocatedRowWithValidationResult build();
  }
}
