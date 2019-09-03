package feast.core.model;

import feast.core.StoreProto;
import feast.core.StoreProto.Store.StoreType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
@AllArgsConstructor
@Entity
@Table(name = "stores")
public class Store {

  // Name of the store. Must be unique
  @Id
  @Column(name = "name", nullable = false, unique = true)
  private String name;

  // Type of the store, should map to feast.core.Store.StoreType
  @Column(name = "type", nullable = false)
  private String type;

  // Connection string to the database
  @Column(name = "database_uri", nullable = false)
  private String databaseURI;

  // FeatureSets this store is subscribed to, comma delimited.
  @Column(name = "subscriptions")
  private String subscriptions;

  public Store() {
    super();
  }

  public static Store fromProto(StoreProto.Store storeProto) {
    String subs = String.join(",", storeProto.getSubscriptionsList());
    return new Store(storeProto.getName(), storeProto.getType().toString(),
        storeProto.getDatabaseURI(), subs);
  }

  public StoreProto.Store toProto() {
    return StoreProto.Store.newBuilder()
        .setName(name)
        .setType(StoreType.valueOf(type))
        .setDatabaseURI(databaseURI)
        .build();
  }
}
