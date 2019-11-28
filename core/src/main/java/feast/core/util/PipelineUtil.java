package feast.core.util;

import static feast.core.util.PackageUtil.resolveSpringBootPackageClasspath;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class PipelineUtil {

  /**
   * Attempts to detect all the resources the class loader has access to. This does not recurse to
   * class loader parents stopping it from pulling in resources from the system class loader.
   *
   * <p>This method extends this implemention
   * https://github.com/apache/beam/blob/01726e9c62313749f9ea7c93063a1178abd1a8db/runners/core-construction-java/src/main/java/org/apache/beam/runners/core/construction/PipelineResources.java#L51
   * to support URL that starts with "jar:file:", usually coming from a packaged Spring Boot jar.
   *
   * @param classLoader The URLClassLoader to use to detect resources to stage.
   * @return A list of absolute paths to the resources the class loader uses.
   * @throws IllegalArgumentException If either the class loader is not a URLClassLoader or one of
   *     the resources the class loader exposes is not a file resource.
   * @throws IOException If there is an error in reading or writing files.
   */
  public static List<String> detectClassPathResourcesToStage(ClassLoader classLoader)
      throws IOException {
    if (!(classLoader instanceof URLClassLoader)) {
      String message =
          String.format(
              "Unable to use ClassLoader to detect classpath elements. "
                  + "Current ClassLoader is %s, only URLClassLoaders are supported.",
              classLoader);
      throw new IllegalArgumentException(message);
    }

    List<String> files = new ArrayList<>();
    for (URL url : ((URLClassLoader) classLoader).getURLs()) {
      if (url.toString().startsWith("jar:file:")) {
        files.add(resolveSpringBootPackageClasspath(url));
        continue;
      }

      try {
        files.add(new File(url.toURI()).getAbsolutePath());
      } catch (IllegalArgumentException | URISyntaxException e) {
        String message = String.format("Unable to convert url (%s) to file.", url);
        throw new IllegalArgumentException(message, e);
      }
    }
    return files;
  }
}
