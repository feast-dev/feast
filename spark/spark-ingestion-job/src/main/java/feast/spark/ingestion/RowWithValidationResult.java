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
package feast.spark.ingestion;

import com.google.auto.value.AutoValue;
import feast.storage.api.writer.FailedElement;
import javax.annotation.Nullable;

@AutoValue
public abstract class RowWithValidationResult {

  public abstract byte[] getFeatureRow();

  public abstract boolean isValid();

  @Nullable
  public abstract FailedElement getFailedElement();

  public static RowWithValidationResult.Builder newBuilder() {
    return new AutoValue_RowWithValidationResult.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract RowWithValidationResult.Builder setFeatureRow(byte[] featureRow);

    public abstract RowWithValidationResult.Builder setValid(boolean valid);

    public abstract RowWithValidationResult.Builder setFailedElement(FailedElement statsdPort);

    public abstract RowWithValidationResult build();
  }
}
