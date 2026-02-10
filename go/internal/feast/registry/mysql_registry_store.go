// Package registry implements Feast registry stores.
//
// MySQL Registry Store:
// The MySQL registry store provides read-only access to a Feast registry stored in MySQL.
// It queries a database schema matching the Python SQLAlchemy schema defined in
// sdk/python/feast/infra/registry/sql.py. When the Python schema evolves, the Go queries
// in this package must be updated accordingly.
package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SQL queries for loading registry objects from the MySQL registry.
// These queries assume the schema defined in sdk/python/feast/infra/registry/sql.py.
const (
	queryProjects             = "SELECT project_proto FROM projects WHERE project_id = ?"
	queryEntities             = "SELECT entity_proto FROM entities WHERE project_id = ?"
	queryFeatureViews         = "SELECT feature_view_proto FROM feature_views WHERE project_id = ?"
	queryStreamFeatureViews   = "SELECT feature_view_proto FROM stream_feature_views WHERE project_id = ?"
	queryOnDemandFeatureViews = "SELECT feature_view_proto FROM on_demand_feature_views WHERE project_id = ?"
	queryFeatureServices      = "SELECT feature_service_proto FROM feature_services WHERE project_id = ?"
	queryDataSources          = "SELECT data_source_proto FROM data_sources WHERE project_id = ?"
	querySavedDatasets        = "SELECT saved_dataset_proto FROM saved_datasets WHERE project_id = ?"
	queryValidationReferences = "SELECT validation_reference_proto FROM validation_references WHERE project_id = ?"
	queryPermissions          = "SELECT permission_proto FROM permissions WHERE project_id = ?"
	queryManagedInfra         = "SELECT infra_proto FROM managed_infra WHERE project_id = ?"
	queryMaxLastUpdated       = "SELECT MAX(last_updated_timestamp) FROM projects WHERE project_id = ?"
)

type MySQLRegistryStore struct {
	dsn            string
	dsnErr         error
	db             *sql.DB
	dbOnce         sync.Once
	dbErr          error
	project        string
	driverName     string
	registryConfig *RegistryConfig
}

// NewMySQLRegistryStore creates a MySQLRegistryStore from a SQLAlchemy-style URL or a raw DSN.
func NewMySQLRegistryStore(config *RegistryConfig, repoPath string, project string) *MySQLRegistryStore {
	dsn, err := mysqlURLToDSN(config.Path)
	return &MySQLRegistryStore{
		dsn:            dsn,
		dsnErr:         err,
		project:        project,
		driverName:     "mysql",
		registryConfig: config,
	}
}

// newMySQLRegistryStoreWithDB is for tests to inject a pre-configured DB handle.
func newMySQLRegistryStoreWithDB(db *sql.DB, project string) *MySQLRegistryStore {
	return &MySQLRegistryStore{
		db:         db,
		project:    project,
		driverName: "mysql",
	}
}

func (r *MySQLRegistryStore) GetRegistryProto() (*core.Registry, error) {
	if r.project == "" {
		return nil, errors.New("mysql registry store requires a project name")
	}
	db, err := r.getDB()
	if err != nil {
		return nil, err
	}

	queryTimeout := r.getQueryTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping MySQL registry database: %w", err)
	}

	registry := &core.Registry{
		RegistrySchemaVersion: REGISTRY_SCHEMA_VERSION,
	}

	projects, err := readProtoRows(ctx, db,
		queryProjects,
		[]any{r.project},
		func() *core.Project { return &core.Project{} },
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load projects: %w", err)
	}
	registry.Projects = projects

	if lastUpdated, err := r.getMaxProjectUpdatedTimestamp(ctx, db); err == nil && lastUpdated != nil {
		registry.LastUpdated = lastUpdated
	}

	entities, err := readProtoRows(ctx, db,
		queryEntities,
		[]any{r.project},
		func() *core.Entity { return &core.Entity{} },
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load entities: %w", err)
	}
	registry.Entities = entities

	featureViews, err := readProtoRows(ctx, db,
		queryFeatureViews,
		[]any{r.project},
		func() *core.FeatureView { return &core.FeatureView{} },
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load feature_views: %w", err)
	}
	registry.FeatureViews = featureViews

	streamFeatureViews, err := readProtoRows(ctx, db,
		queryStreamFeatureViews,
		[]any{r.project},
		func() *core.StreamFeatureView { return &core.StreamFeatureView{} },
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load stream_feature_views: %w", err)
	}
	registry.StreamFeatureViews = streamFeatureViews

	onDemandFeatureViews, err := readProtoRows(ctx, db,
		queryOnDemandFeatureViews,
		[]any{r.project},
		func() *core.OnDemandFeatureView { return &core.OnDemandFeatureView{} },
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load on_demand_feature_views: %w", err)
	}
	registry.OnDemandFeatureViews = onDemandFeatureViews

	featureServices, err := readProtoRows(ctx, db,
		queryFeatureServices,
		[]any{r.project},
		func() *core.FeatureService { return &core.FeatureService{} },
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load feature_services: %w", err)
	}
	registry.FeatureServices = featureServices

	dataSources, err := readProtoRows(ctx, db,
		queryDataSources,
		[]any{r.project},
		func() *core.DataSource { return &core.DataSource{} },
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load data_sources: %w", err)
	}
	registry.DataSources = dataSources

	savedDatasets, err := readProtoRows(ctx, db,
		querySavedDatasets,
		[]any{r.project},
		func() *core.SavedDataset { return &core.SavedDataset{} },
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load saved_datasets: %w", err)
	}
	registry.SavedDatasets = savedDatasets

	validationReferences, err := readProtoRows(ctx, db,
		queryValidationReferences,
		[]any{r.project},
		func() *core.ValidationReference { return &core.ValidationReference{} },
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load validation_references: %w", err)
	}
	registry.ValidationReferences = validationReferences

	permissions, err := readProtoRows(ctx, db,
		queryPermissions,
		[]any{r.project},
		func() *core.Permission { return &core.Permission{} },
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load permissions: %w", err)
	}
	registry.Permissions = permissions

	infra, err := readProtoRows(ctx, db,
		queryManagedInfra,
		[]any{r.project},
		func() *core.Infra { return &core.Infra{} },
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load managed_infra: %w", err)
	}
	if len(infra) > 0 {
		registry.Infra = infra[0]
	}

	log.Debug().Str("project", r.project).Msg("Loaded registry from MySQL")
	return registry, nil
}

func (r *MySQLRegistryStore) UpdateRegistryProto(rp *core.Registry) error {
	return errors.New("not implemented in MySQLRegistryStore")
}

func (r *MySQLRegistryStore) Teardown() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

func (r *MySQLRegistryStore) getDB() (*sql.DB, error) {
	r.dbOnce.Do(func() {
		if r.db != nil {
			// Already initialized (e.g., via newMySQLRegistryStoreWithDB for tests)
			return
		}
		if r.dsnErr != nil {
			r.dbErr = fmt.Errorf("invalid MySQL registry DSN: %w", r.dsnErr)
			return
		}
		if r.dsn == "" {
			r.dbErr = errors.New("mysql registry store requires a non-empty DSN")
			return
		}
		db, err := sql.Open(r.driverName, r.dsn)
		if err != nil {
			r.dbErr = fmt.Errorf("failed to open MySQL registry database: %w", err)
			return
		}
		applyMySQLPoolSettings(db, r.registryConfig)
		r.db = db
	})
	if r.dbErr != nil {
		return nil, r.dbErr
	}
	return r.db, nil
}

func (r *MySQLRegistryStore) getMaxProjectUpdatedTimestamp(ctx context.Context, db *sql.DB) (*timestamppb.Timestamp, error) {
	var maxUpdated sql.NullInt64
	err := db.QueryRowContext(ctx,
		queryMaxLastUpdated,
		r.project,
	).Scan(&maxUpdated)
	if err != nil {
		return nil, err
	}
	if !maxUpdated.Valid {
		return nil, nil
	}
	return timestamppb.New(time.Unix(maxUpdated.Int64, 0)), nil
}

func (r *MySQLRegistryStore) getQueryTimeout() time.Duration {
	if r.registryConfig != nil && r.registryConfig.MySQLQueryTimeoutSeconds > 0 {
		return time.Duration(r.registryConfig.MySQLQueryTimeoutSeconds) * time.Second
	}
	return time.Duration(defaultMySQLQueryTimeoutSeconds) * time.Second
}

func readProtoRows[T proto.Message](ctx context.Context, db *sql.DB, query string, args []any, newProto func() T) ([]T, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]T, 0)
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		msg := newProto()
		if err := proto.Unmarshal(data, msg); err != nil {
			return nil, err
		}
		results = append(results, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func mysqlURLToDSN(registryPath string) (string, error) {
	if strings.TrimSpace(registryPath) == "" {
		return "", errors.New("mysql registry path is empty")
	}

	parsed, err := url.Parse(registryPath)
	if err != nil {
		return "", err
	}

	if parsed.Scheme == "" {
		// Assume raw DSN.
		return registryPath, nil
	}
	if !isMySQLScheme(parsed.Scheme) {
		return "", fmt.Errorf("unsupported mysql scheme %q", parsed.Scheme)
	}

	cfg := mysql.NewConfig()
	if parsed.User != nil {
		cfg.User = parsed.User.Username()
		if pwd, ok := parsed.User.Password(); ok {
			cfg.Passwd = pwd
		}
	}

	cfg.Net = "tcp"
	if host := parsed.Hostname(); host != "" {
		if port := parsed.Port(); port != "" {
			cfg.Addr = net.JoinHostPort(host, port)
		} else {
			cfg.Addr = host
		}
	}

	cfg.DBName = strings.TrimPrefix(parsed.Path, "/")
	if cfg.DBName == "" {
		return "", errors.New("mysql registry path missing database name")
	}

	params := parsed.Query()
	if socket := params.Get("unix_socket"); socket != "" {
		cfg.Net = "unix"
		cfg.Addr = socket
		params.Del("unix_socket")
	}

	if len(params) > 0 {
		cfg.Params = map[string]string{}
		for key, values := range params {
			if len(values) > 0 {
				cfg.Params[key] = values[len(values)-1]
			} else {
				cfg.Params[key] = ""
			}
		}
	}

	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	if _, ok := cfg.Params["parseTime"]; !ok {
		cfg.Params["parseTime"] = "true"
	}

	return cfg.FormatDSN(), nil
}

func applyMySQLPoolSettings(db *sql.DB, config *RegistryConfig) {
	if config == nil {
		return
	}
	if config.MySQLMaxOpenConns > 0 {
		db.SetMaxOpenConns(config.MySQLMaxOpenConns)
	}
	if config.MySQLMaxIdleConns > 0 {
		db.SetMaxIdleConns(config.MySQLMaxIdleConns)
	}
	if config.MySQLConnMaxLifetimeSeconds > 0 {
		db.SetConnMaxLifetime(time.Duration(config.MySQLConnMaxLifetimeSeconds) * time.Second)
	}
}

func isMySQLScheme(scheme string) bool {
	return strings.ToLower(scheme) == "mysql"
}
