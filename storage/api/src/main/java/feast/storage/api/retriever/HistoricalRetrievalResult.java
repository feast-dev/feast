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
package feast.storage.api.retriever;

import com.google.auto.value.AutoValue;
import feast.serving.ServingAPIProto.DataFormat;
import feast.serving.ServingAPIProto.JobStatus;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;

/** Result of a historical feature retrieval request. */
@AutoValue
public abstract class HistoricalRetrievalResult implements Serializable {

  public abstract String getId();

  public abstract JobStatus getStatus();

  @Nullable
  public abstract String getError();

  @Nullable
  public abstract List<String> getFileUris();

  @Nullable
  public abstract DataFormat getDataFormat();

  /**
   * Instantiates a {@link HistoricalRetrievalResult} indicating that the retrieval was a failure,
   * together with its associated error.
   *
   * @param id retrieval id identifying the retrieval request.
   * @param error error that occurred
   * @return {@link HistoricalRetrievalResult}
   */
  public static HistoricalRetrievalResult error(String id, Exception error) {
    return newBuilder()
        .setId(id)
        .setStatus(JobStatus.JOB_STATUS_DONE)
        .setError(error.getMessage())
        .build();
  }

  /**
   * Instantiates a {@link HistoricalRetrievalResult} indicating that the retrieval was a success,
   * together with the location of the output.
   *
   * @param id retrieval id identifying the retrieval request
   * @param fileUris list of output file URIs
   * @param dataFormat data format of the output files
   * @return
   */
  public static HistoricalRetrievalResult success(
      String id, List<String> fileUris, DataFormat dataFormat) {
    return newBuilder()
        .setId(id)
        .setStatus(JobStatus.JOB_STATUS_DONE)
        .setFileUris(fileUris)
        .setDataFormat(dataFormat)
        .build();
  }

  static Builder newBuilder() {
    return new AutoValue_HistoricalRetrievalResult.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setId(String id);

    abstract Builder setStatus(JobStatus jobStatus);

    abstract Builder setError(String error);

    abstract Builder setFileUris(List<String> fileUris);

    abstract Builder setDataFormat(DataFormat dataFormat);

    abstract HistoricalRetrievalResult build();
  }

  public boolean hasError() {
    return getError() != null;
  }
}
