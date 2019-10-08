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

package feast.ingestion.utils;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PathUtil {

  /** Gets a path with a schema if present */
  public static Path getPath(String value) {
    if (value.contains("://")) {
      try {
        return Paths.get(new URI(value));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    } else {
      return Paths.get(value);
    }
  }

  public static String readStringFromUri(String uriPath) throws URISyntaxException, IOException {
    return readStringFromUri(uriPath, StorageOptions.getDefaultInstance().getService());
  }

  public static String readStringFromUri(String uriPath, Storage storage)
      throws URISyntaxException, IOException {
    URI uri = new URI(uriPath);
    String scheme = uri.getScheme();

    if (scheme == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to retrieve the YAML file because the file URI has an invalid scheme '%s'. File URI must start with either 'file://' or 'gs://'. Invalid URI: %s",
              scheme, uriPath));
    }

    String out = "";
    switch (scheme) {
      case "file":
        if (uri.getHost() != null) {
          throw new IllegalArgumentException(
              "Please provide an 'absolute' path for a local file URI, for example use 'file:///tmp/myfile.txt' and NOT 'file://tmp/myfile.txt'. Invalid URI: "
                  + uriPath);
        }
        if (uri.getPath().endsWith("/")) {
          throw new IllegalArgumentException(
              "Please provide a URI to a local file NOT a directory. Invalid URI: " + uriPath);
        }
        out = new String(Files.readAllBytes(Paths.get(uri.getPath())), StandardCharsets.UTF_8);
        break;
      case "gs":
        String bucketName = uri.getHost();
        if (bucketName == null || bucketName.isEmpty()) {
          throw new IllegalArgumentException(
              "Missing bucket in the URI, expected URI in this pattern 'gs://<bucket>/<blob>'. Invalid URI: "
                  + uriPath);
        }
        if (uri.getPath() == null || uri.getPath().isEmpty()) {
          throw new IllegalArgumentException(
              "Missing blob in the URI, expected URI in this pattern 'gs://<bucket>/<blob>'. Invalid URI: "
                  + uriPath);
        }
        if (uri.getPath().endsWith("/")) {
          throw new IllegalArgumentException(
              "Invalid blob in the URI, should not end with a slash, expected URI in this pattern 'gs://<bucket>/<blob>'. Invalid URI: "
                  + uriPath);
        }
        String blobName = uri.getPath().substring(1);
        Blob blob = storage.get(bucketName, blobName);
        if (blob == null) {
          throw new IllegalArgumentException("File not found. Please check your URI: " + uriPath);
        }
        out = new String(blob.getContent(), StandardCharsets.UTF_8);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Failed to retrieve the YAML file because the file URI has an invalid scheme '%s'. File URI must start with either 'file://' or 'gs://'. Invalid URI: %s",
                uri.getScheme(), uriPath));
    }

    if (out.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("'%s' is empty, please check that the uri path is valid", uriPath));
    }

    return out;
  }
}
