package feast.databricks.types;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SparkSubmitParams {
    private String main_class_name;
}
