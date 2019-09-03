package feast.core.dao;

import feast.core.model.FeatureSet;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

/** JPA repository supplying FeatureSet objects keyed by id. */
public interface FeatureSetRepository  extends JpaRepository<FeatureSet, String> {
  // find all versions of featureSets matching the given name.
  public List<FeatureSet> findByName(String name);
}
