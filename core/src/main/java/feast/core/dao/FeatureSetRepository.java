package feast.core.dao;

import feast.core.model.FeatureSet;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

/** JPA repository supplying FeatureSet objects keyed by id. */
public interface FeatureSetRepository  extends JpaRepository<FeatureSet, String> {
  // find all versions of featureSets matching the given name.
  List<FeatureSet> findByName(String name);

  // find all versions of featureSets with names matching the regex
  @Query(nativeQuery=true, value="SELECT * FROM feature_sets WHERE name LIKE ?1")
  List<FeatureSet> findByNameWithWildcard(String name);
}
