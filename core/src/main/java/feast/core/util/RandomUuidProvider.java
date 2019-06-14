package feast.core.util;

import java.util.UUID;

public class RandomUuidProvider implements UuidProvider {
  @Override
  public String getUuid() {
    return UUID.randomUUID().toString().replace("-","");
  }
}
