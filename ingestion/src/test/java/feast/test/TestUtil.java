package feast.test;

import feast.types.FieldProto;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType;
import feast.types.ValueProto.ValueType.Enum;
import java.io.IOException;
import redis.embedded.RedisServer;

public class TestUtil {
  public static class LocalRedis {

    private static RedisServer server;

    /**
     * Start local Redis for used in testing at "localhost"
     *
     * @param port port number
     * @throws IOException if Redis failed to start
     */
    public static void start(int port) throws IOException {
      server = new RedisServer(port);
      server.start();
    }

    public static void stop() {
      server.stop();
    }
  }

  /**
   * Create a field object with given name and type.
   *
   * @param name of the field.
   * @param value of the field. Should be compatible with the valuetype given.
   * @param valueType type of the field.
   * @return Field object
   */
  public static Field field(String name, Object value, ValueType.Enum valueType) {
    Field.Builder fieldBuilder = Field.newBuilder()
        .setName(name);
    switch (valueType) {
      case INT32:
        return fieldBuilder.setValue(Value.newBuilder().setInt32Val((int) value)).build();
      case INT64:
        return fieldBuilder.setValue(Value.newBuilder().setInt64Val((int) value)).build();
      case FLOAT:
        return fieldBuilder.setValue(Value.newBuilder().setFloatVal((float) value)).build();
      case DOUBLE:
        return fieldBuilder.setValue(Value.newBuilder().setDoubleVal((double) value)).build();
      case STRING:
        return fieldBuilder.setValue(Value.newBuilder().setStringVal((String) value)).build();
      default:
        throw new IllegalStateException("Unexpected valueType: " + value.getClass());
    }
  }
}
