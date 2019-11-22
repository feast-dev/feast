package feast.core.validators;

import static feast.core.validators.Matchers.checkValidCharacters;

import com.google.common.collect.Sets;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class FeatureSetValidator {
  public static void validateSpec(FeatureSetSpec featureSetSpec) {
    checkValidCharacters(featureSetSpec.getName(), "name");
    checkUniqueColumns(featureSetSpec.getEntitiesList(), featureSetSpec.getFeaturesList());
    for (EntitySpec entitySpec : featureSetSpec.getEntitiesList()) {
      checkValidCharacters(entitySpec.getName(), "entities::name");
    }
    for (FeatureSpec featureSpec : featureSetSpec.getFeaturesList()) {
      checkValidCharacters(featureSpec.getName(), "features::name");
    }
  }

  private static void checkUniqueColumns(
      List<EntitySpec> entitySpecs, List<FeatureSpec> featureSpecs) {
    List<String> names = entitySpecs.stream().map(EntitySpec::getName).collect(Collectors.toList());
    featureSpecs.stream().map(f -> names.add(f.getName()));
    HashSet<String> nameSet = Sets.newHashSet(names);
    if (nameSet.size() != names.size()) {
      throw new IllegalArgumentException(
          String.format("fields within a featureset must be unique."));
    }
  }
}
