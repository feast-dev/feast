package registry

import (
	"database/sql"
	"testing"

	"github.com/feast-dev/feast/go/protos/feast/core"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// getRegistrySchemaDDL returns the DDL statements for creating the registry schema.
// Schema must match the Python SQLAlchemy schema defined in:
// sdk/python/feast/infra/registry/sql.py
// When the Python schema evolves, this function must be updated accordingly.
func getRegistrySchemaDDL() []string {
	return []string{
		`CREATE TABLE projects (project_id TEXT PRIMARY KEY, project_name TEXT NOT NULL, last_updated_timestamp INTEGER NOT NULL, project_proto BLOB NOT NULL);`,
		`CREATE TABLE entities (entity_name TEXT NOT NULL, project_id TEXT NOT NULL, last_updated_timestamp INTEGER NOT NULL, entity_proto BLOB NOT NULL, PRIMARY KEY (entity_name, project_id));`,
		`CREATE TABLE feature_views (feature_view_name TEXT NOT NULL, project_id TEXT NOT NULL, last_updated_timestamp INTEGER NOT NULL, materialized_intervals BLOB NULL, feature_view_proto BLOB NOT NULL, user_metadata BLOB NULL, PRIMARY KEY (feature_view_name, project_id));`,
		`CREATE TABLE stream_feature_views (feature_view_name TEXT NOT NULL, project_id TEXT NOT NULL, last_updated_timestamp INTEGER NOT NULL, feature_view_proto BLOB NOT NULL, user_metadata BLOB NULL, PRIMARY KEY (feature_view_name, project_id));`,
		`CREATE TABLE on_demand_feature_views (feature_view_name TEXT NOT NULL, project_id TEXT NOT NULL, last_updated_timestamp INTEGER NOT NULL, feature_view_proto BLOB NOT NULL, user_metadata BLOB NULL, PRIMARY KEY (feature_view_name, project_id));`,
		`CREATE TABLE feature_services (feature_service_name TEXT NOT NULL, project_id TEXT NOT NULL, last_updated_timestamp INTEGER NOT NULL, feature_service_proto BLOB NOT NULL, PRIMARY KEY (feature_service_name, project_id));`,
		`CREATE TABLE data_sources (data_source_name TEXT NOT NULL, project_id TEXT NOT NULL, last_updated_timestamp INTEGER NOT NULL, data_source_proto BLOB NOT NULL, PRIMARY KEY (data_source_name, project_id));`,
		`CREATE TABLE saved_datasets (saved_dataset_name TEXT NOT NULL, project_id TEXT NOT NULL, last_updated_timestamp INTEGER NOT NULL, saved_dataset_proto BLOB NOT NULL, PRIMARY KEY (saved_dataset_name, project_id));`,
		`CREATE TABLE validation_references (validation_reference_name TEXT NOT NULL, project_id TEXT NOT NULL, last_updated_timestamp INTEGER NOT NULL, validation_reference_proto BLOB NOT NULL, PRIMARY KEY (validation_reference_name, project_id));`,
		`CREATE TABLE permissions (permission_name TEXT NOT NULL, project_id TEXT NOT NULL, last_updated_timestamp INTEGER NOT NULL, permission_proto BLOB NOT NULL, PRIMARY KEY (permission_name, project_id));`,
		`CREATE TABLE managed_infra (infra_name TEXT NOT NULL, project_id TEXT NOT NULL, last_updated_timestamp INTEGER NOT NULL, infra_proto BLOB NOT NULL, PRIMARY KEY (infra_name, project_id));`,
	}
}

func TestMySQLRegistryStore_GetRegistryProto_FromSQLRegistrySchema(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err, "failed to open sqlite db")
	defer db.Close()

	project := "feature_repo"
	lastUpdated := int64(1710000000)

	for _, stmt := range getRegistrySchemaDDL() {
		_, err := db.Exec(stmt)
		require.NoError(t, err, "failed to create tables")
	}

	projectProto := &core.Project{
		Spec: &core.ProjectSpec{Name: project},
	}
	entityProto := &core.Entity{
		Spec: &core.EntitySpecV2{Name: "driver", Project: project},
	}
	featureViewProto := &core.FeatureView{
		Spec: &core.FeatureViewSpec{Name: "driver_stats", Project: project},
	}
	featureServiceProto := &core.FeatureService{
		Spec: &core.FeatureServiceSpec{Name: "driver_stats_service", Project: project},
	}
	infraProto := &core.Infra{}

	projectBlob, err := proto.Marshal(projectProto)
	require.NoError(t, err, "failed to marshal project proto")
	entityBlob, err := proto.Marshal(entityProto)
	require.NoError(t, err, "failed to marshal entity proto")
	featureViewBlob, err := proto.Marshal(featureViewProto)
	require.NoError(t, err, "failed to marshal feature view proto")
	featureServiceBlob, err := proto.Marshal(featureServiceProto)
	require.NoError(t, err, "failed to marshal feature service proto")
	infraBlob, err := proto.Marshal(infraProto)
	require.NoError(t, err, "failed to marshal infra proto")

	_, err = db.Exec(
		"INSERT INTO projects (project_id, project_name, last_updated_timestamp, project_proto) VALUES (?, ?, ?, ?)",
		project, project, lastUpdated, projectBlob,
	)
	require.NoError(t, err, "failed to insert project row")

	_, err = db.Exec(
		"INSERT INTO entities (entity_name, project_id, last_updated_timestamp, entity_proto) VALUES (?, ?, ?, ?)",
		"driver", project, lastUpdated, entityBlob,
	)
	require.NoError(t, err, "failed to insert entity row")

	_, err = db.Exec(
		"INSERT INTO feature_views (feature_view_name, project_id, last_updated_timestamp, feature_view_proto) VALUES (?, ?, ?, ?)",
		"driver_stats", project, lastUpdated, featureViewBlob,
	)
	require.NoError(t, err, "failed to insert feature view row")

	_, err = db.Exec(
		"INSERT INTO feature_services (feature_service_name, project_id, last_updated_timestamp, feature_service_proto) VALUES (?, ?, ?, ?)",
		"driver_stats_service", project, lastUpdated, featureServiceBlob,
	)
	require.NoError(t, err, "failed to insert feature service row")

	_, err = db.Exec(
		"INSERT INTO managed_infra (infra_name, project_id, last_updated_timestamp, infra_proto) VALUES (?, ?, ?, ?)",
		"infra_obj", project, lastUpdated, infraBlob,
	)
	require.NoError(t, err, "failed to insert infra row")

	store := newMySQLRegistryStoreWithDB(db, project)
	registryProto, err := store.GetRegistryProto()
	require.NoError(t, err, "GetRegistryProto failed")

	assert.Equal(t, REGISTRY_SCHEMA_VERSION, registryProto.RegistrySchemaVersion)
	require.Len(t, registryProto.Projects, 1)
	assert.Equal(t, project, registryProto.Projects[0].Spec.GetName())
	require.Len(t, registryProto.Entities, 1)
	assert.Equal(t, "driver", registryProto.Entities[0].Spec.GetName())
	require.Len(t, registryProto.FeatureViews, 1)
	assert.Equal(t, "driver_stats", registryProto.FeatureViews[0].Spec.GetName())
	require.Len(t, registryProto.FeatureServices, 1)
	assert.Equal(t, "driver_stats_service", registryProto.FeatureServices[0].Spec.GetName())
	require.NotNil(t, registryProto.LastUpdated)
	assert.Equal(t, lastUpdated, registryProto.LastUpdated.GetSeconds())
	assert.NotNil(t, registryProto.Infra)
}

func TestMySQLRegistryStore_SchemeRouting(t *testing.T) {
	registryConfig := &RegistryConfig{
		Path: "mysql://user:pass@localhost:3306/feast",
	}
	store, err := getRegistryStoreFromScheme(registryConfig.Path, registryConfig, "", "feature_repo")
	require.NoError(t, err, "getRegistryStoreFromScheme failed")
	assert.IsType(t, &MySQLRegistryStore{}, store)
}
