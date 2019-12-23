package feast.serving.util;

public class SpecUtil {
  public static String generateFeastId(String project, String name, int version) {
    String ref = String.format("%s/%s", project, name);
    if (version > 0) {
      return ref + String.format(":%d", version);
    }
    return ref;
  }
}
