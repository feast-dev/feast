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
package feast.core.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("WeakerAccess")
public class PackageUtil {

  // TODO: Unit tests for PackageUtil

  private static Logger LOG = LoggerFactory.getLogger(PackageUtil.class);

  /**
   * Get a local file path from a URL that reference classes or a resource located in Spring Boot
   * packaged jar.
   *
   * <p>The packaged jar will be extracted, if needed, in order to get a file path that directly
   * points to the resource location. Note that the extraction process can take several minutes to
   * complete.
   *
   * <p>One use case of this function is to detect the class path of resources to stage when using
   * Dataflow runner. The resource URL however is in "jar:file:" format, which cannot be handled by
   * default in Apache Beam.
   *
   * <pre>
   * <code>
   * URL url = new URL("jar:file:/tmp/springexample/target/spring-example-1.0-SNAPSHOT.jar!/BOOT-INF/lib/beam-sdks-java-core-2.16.0.jar!/");
   * </code>
   * String resolvedPath = resolveSpringBootPackageClasspath(url);
   * // resolvedPath should point to "/tmp/springexample/target/spring-example-1.0-SNAPSHOT/BOOT-INF/lib/beam-sdks-java-core-2.16.0.jar"
   * // Note that spring-example-1.0-SNAPSHOT.jar is extracted in the process.
   * </pre>
   *
   * @param url Location of the resource or classes to resolve, must start with "jar:file:".
   * @return Local file path that points to the resource file.
   * @throws IOException If read or write error occurs during the resolve process.
   */
  public static String resolveSpringBootPackageClasspath(URL url) throws IOException {
    if (!url.toString().startsWith("jar:file:")) {
      throw new IllegalArgumentException("URL must start with 'jar:file:'");
    }

    String path = url.toString().substring(9).replaceAll("!/", "/");
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    if (path.contains(".jar/BOOT-INF/")) {
      String jarPath = path.substring(0, path.indexOf(".jar/BOOT-INF/") + 4);
      String extractedJarPath = jarPath.substring(0, jarPath.length() - 4);

      if (Files.notExists(Paths.get(extractedJarPath))) {
        LOG.info(
            "Extracting '{}' to '{}' so we can get a local file path for the resource.",
            jarPath,
            extractedJarPath);
        extractJar(jarPath, extractedJarPath);
      }
      path = path.replace(".jar/BOOT-INF/", "/BOOT-INF/");
    }

    return path;
  }

  // TODO: extractJar() currently is quite slow because it only uses a single core to extract the
  //       jar. Extracting a jar packaged by Spring boot, for example, can take more than 5 minutes.
  // One
  //       way to speed it up is to parallelize the extraction.

  /**
   * Extract contents of a jar file to an output directory.
   *
   * <p>Adapted from: https://stackoverflow.com/a/1529707/3949303
   *
   * @param jarPath File path of the jar file to extract.
   * @param destDirPath Destination directory to extract the jar content, will be created if not
   *     exists.
   * @throws IOException If error occured when reading or writing files.
   */
  public static void extractJar(String jarPath, String destDirPath) throws IOException {
    File destDirFile = new File(destDirPath);

    if (destDirFile.exists() && !destDirFile.isDirectory()) {
      throw new IOException(destDirPath + " must be a directory path");
    }

    if (!destDirFile.exists()) {
      if (!destDirFile.mkdirs()) {
        throw new IOException("Failed to create directory: " + destDirPath);
      }
    }

    JarFile jar = new JarFile(jarPath);
    Enumeration enumEntries = jar.entries();

    while (enumEntries.hasMoreElements()) {
      JarEntry jarEntry = (java.util.jar.JarEntry) enumEntries.nextElement();
      File outFile = new java.io.File(destDirPath + File.separator + jarEntry.getName());

      if (jarEntry.isDirectory()) {
        if (!outFile.mkdir()) {
          throw new IOException("Failed to created directory: " + outFile);
        }
        continue;
      }

      InputStream is = jar.getInputStream(jarEntry);
      FileOutputStream fos = new FileOutputStream(outFile);
      while (is.available() > 0) {
        fos.write(is.read());
      }
      fos.close();
      is.close();
    }

    jar.close();
  }
}
