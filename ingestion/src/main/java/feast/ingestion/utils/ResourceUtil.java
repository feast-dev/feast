package feast.ingestion.utils;

import com.google.common.io.Resources;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;

public class ResourceUtil {
  private static final String DEADLETTER_SCHEMA_FILE_PATH = "schemas/deadletter_table_schema.json";
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(ResourceUtil.class);

  public static String getDeadletterTableSchemaJson() {
    String schemaJson = null;
    try {
      schemaJson =
          Resources.toString(
              Resources.getResource(DEADLETTER_SCHEMA_FILE_PATH), StandardCharsets.UTF_8);
    } catch (Exception e) {
      log.error(
          "Unable to read {} file from the resources folder!", DEADLETTER_SCHEMA_FILE_PATH, e);
    }
    return schemaJson;
  }
}
