package feast.serving.util.mappers;

import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.types.ValueProto.Value;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// ResponseJSONMapper maps GRPC Response types to more human readable JSON responses
public class ResponseJSONMapper {

  public static List<Map<String, Object>> mapGetOnlineFeaturesResponse(
      GetOnlineFeaturesResponse response) {
    return response.getFieldValuesList().stream()
        .map(fieldValue -> convertFieldValuesToMap(fieldValue))
        .collect(Collectors.toList());
  }

  private static Map<String, Object> convertFieldValuesToMap(FieldValues fieldValues) {
    return fieldValues.getFieldsMap().entrySet().stream()
        .collect(Collectors.toMap(es -> es.getKey(), es -> extractValue(es.getValue())));
  }

  private static Object extractValue(Value value) {
    switch (value.getValCase().getNumber()) {
      case 1:
        return value.getBytesVal();
      case 2:
        return value.getStringVal();
      case 3:
        return value.getInt32Val();
      case 4:
        return value.getInt64Val();
      case 5:
        return value.getDoubleVal();
      case 6:
        return value.getFloatVal();
      case 7:
        return value.getBoolVal();
      case 11:
        return value.getBytesListVal();
      case 12:
        return value.getStringListVal();
      case 13:
        return value.getInt32ListVal();
      case 14:
        return value.getInt64ListVal();
      case 15:
        return value.getDoubleListVal();
      case 16:
        return value.getFloatListVal();
      case 17:
        return value.getBoolListVal();
      default:
        return null;
    }
  }
}
