package feast.serving.util.mappers;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import java.io.IOException;
import org.junit.Test;

public class YamlToProtoMapperTest {

  @Test
  public void shouldConvertYamlToProto() throws IOException {
    String yaml =
        "name: test\n"
            + "type: REDIS\n"
            + "redis_config:\n"
            + "  host: localhost\n"
            + "  port: 6379\n"
            + "subscriptions:\n"
            + "- name: \"*\"\n"
            + "  version: \">0\"\n";
    Store store = YamlToProtoMapper.yamlToStoreProto(yaml);
    Store expected =
        Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379))
            .addSubscriptions(Subscription.newBuilder().setName("*").setVersion(">0"))
            .build();
    assertThat(store, equalTo(expected));
  }
}
