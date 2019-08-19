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

package feast.ingestion.model;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.Charsets;
import feast.types.FeatureRowExtendedProto.Error;

public class Errors {
  public static Error toError(String transformName, String message, Throwable throwable) {
    Error error =
        Error.newBuilder()
            .setTransform(transformName)
            .setCause(throwable.getClass().getName())
            .setMessage(message)
            .setStackTrace(getStackTraceString(throwable))
            .build();
    return error;
  }

  public static Error toError(String transformName, Throwable throwable) {
    return toError(transformName, throwable.getMessage(), throwable);
  }

  public static String getStackTraceString(Throwable throwable) {
    ByteArrayOutputStream byteouts = new ByteArrayOutputStream();
    throwable.printStackTrace(new PrintStream(byteouts));
    try {
      return byteouts.toString(StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
