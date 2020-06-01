package feast.core.job.databricks;

import lombok.Getter;
import lombok.Setter;

import javax.validation.*;
import javax.validation.constraints.NotBlank;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

/**
 * DatabricksRunnerConfig contains configuration fields for the Databricks job runner.
 */
@Getter
@Setter
public class DatabricksRunnerConfig {
    public DatabricksRunnerConfig(Map<String, String> runnerConfigOptions) {
        for (Field field : DatabricksRunnerConfig.class.getFields()) {
            String fieldName = field.getName();
            try {
                if (!runnerConfigOptions.containsKey(fieldName)) {
                    continue;
                }
                String value = runnerConfigOptions.get(fieldName);

                if (Boolean.class.equals(field.getType())) {
                    field.set(this, Boolean.valueOf(value));
                    continue;
                }
                if (field.getType() == Integer.class) {
                    field.set(this, Integer.valueOf(value));
                    continue;
                }
                field.set(this, value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(
                        String.format(
                                "Could not successfully convert DatabricksRunnerConfig for key: %s", fieldName),
                        e);
            }

        }
        validate();
    }

    /* Project id to use when launching jobs. */
    @NotBlank
    public String databricksService;


    /** Validates Databricks runner configuration options */
    public void validate() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();

        Set<ConstraintViolation<DatabricksRunnerConfig>> databricksRunnerConfigViolation =
                validator.validate(this);
        if (!databricksRunnerConfigViolation.isEmpty()) {
            throw new ConstraintViolationException(databricksRunnerConfigViolation);
        }
    }

}
