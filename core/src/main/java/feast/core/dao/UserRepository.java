package feast.core.dao;

import feast.core.model.User;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/** JPA repository supplying User objects keyed by id. */
public interface UserRepository extends JpaRepository<User, String> {
    Optional<User> findByName(String user_name);

    boolean existsUserByName(String user_name);
}
