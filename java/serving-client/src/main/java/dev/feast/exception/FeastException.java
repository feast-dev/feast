/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2025 The Feast Authors
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
package dev.feast.exception;

import io.grpc.StatusRuntimeException;

public class FeastException extends RuntimeException {
  public FeastException(String message) {
    super(message);
  }

  public FeastException(String message, Throwable cause) {
    super(message, cause);
  }

  public FeastException(Throwable cause) {
    super(cause);
  }

  public static FeastException fromStatusException(StatusRuntimeException statusRuntimeException) {
    switch (statusRuntimeException.getStatus().getCode()) {
      case NOT_FOUND:
        return new FeastNotFoundException(
            statusRuntimeException.getMessage(), statusRuntimeException);
      case INVALID_ARGUMENT:
        return new FeastBadRequestException(
            statusRuntimeException.getMessage(), statusRuntimeException);
      case INTERNAL:
        return new FeastInternalException(
            statusRuntimeException.getMessage(), statusRuntimeException);
      default:
        return new FeastException(statusRuntimeException.getMessage(), statusRuntimeException);
    }
  }
}
