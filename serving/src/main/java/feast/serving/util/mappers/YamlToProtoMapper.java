package feast.serving.util.mappers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.util.JsonFormat;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Builder;
import java.io.IOException;

public class YamlToProtoMapper {
  private static final ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper jsonWriter = new ObjectMapper();

  public static Store yamlToStoreProto(String yaml) throws IOException {
    Object obj = yamlReader.readValue(yaml, Object.class);
    String jsonString = jsonWriter.writeValueAsString(obj);
    Builder builder = Store.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }
}
