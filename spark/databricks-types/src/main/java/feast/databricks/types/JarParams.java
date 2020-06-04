package feast.databricks.types;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JarParams {
    private String kafka_broker;
    private String topic_name;
}
