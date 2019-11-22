package feast.core.dao;

import feast.core.model.Store;
import org.springframework.data.jpa.repository.JpaRepository;

/** JPA repository supplying Store objects keyed by id. */
public interface StoreRepository extends JpaRepository<Store, String> {}
