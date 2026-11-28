package onlinestore

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

// testBigtableFeastProject is the Feast project of stores built by newTestBigtable; table names derive from it.
const testBigtableFeastProject = "feast_test"

// newTestBigtable starts an in-memory Bigtable (bttest) and returns a store built on it from the
// online_store config map cfg, plus an admin client and a data client for creating tables and writing
// rows. srvOpts reach the bttest gRPC server, so tests can add interceptors (for example to count or
// delay ReadRows streams). Everything is closed when the test ends.
//
// It sets BIGTABLE_EMULATOR_HOST, which turns off the client's built-in Cloud Monitoring metrics, so
// tests using it cannot call t.Parallel. Dialing with option.WithGRPCConn also bypasses the client's own
// channel pool, so these tests do not exercise it: not its PingAndWarm priming at construction, which is
// what fails startup on bad credentials in production, nor the session data plane the client can use for
// single-key reads.
func newTestBigtable(t *testing.T, cfg map[string]interface{}, srvOpts ...grpc.ServerOption) (*BigtableOnlineStore, *bigtable.AdminClient, *bigtable.Client) {
	t.Helper()
	srv, err := bttest.NewServer("localhost:0", srvOpts...)
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	t.Setenv("BIGTABLE_EMULATOR_HOST", srv.Addr)

	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	parsed, err := parseBigtableConfig(cfg)
	require.NoError(t, err)
	require.NotEmpty(t, parsed.ProjectID, "newTestBigtable needs project_id in cfg; tests never resolve ADC")

	ctx := context.Background()
	admin, err := bigtable.NewAdminClient(ctx, parsed.ProjectID, parsed.Instance, option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })

	data, err := bigtable.NewClient(ctx, parsed.ProjectID, parsed.Instance, option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = data.Close() })

	repoConfig := &registry.RepoConfig{Project: testBigtableFeastProject, OnlineStore: cfg}
	store, err := newBigtableOnlineStore(ctx, testBigtableFeastProject, repoConfig, parsed, 5*time.Second,
		option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(store.Destruct)
	return store, admin, data
}

// mustParseBigtableConfig parses an online_store config map as feature_store.yaml would give it.
func mustParseBigtableConfig(t *testing.T, m map[string]interface{}) bigtableConfig {
	t.Helper()
	cfg, err := parseBigtableConfig(m)
	require.NoError(t, err)
	return cfg
}

// bigtableRowKey is Python's BigtableOnlineStore._compute_row_key for one entity key and view, built the
// way reads build it, so the parity tests pin the keys reads send.
func bigtableRowKey(entityKey *types.EntityKey, featureViewName string, serializationVersion int64) (string, error) {
	hash, err := bigtableEntityHash(entityKey, serializationVersion)
	if err != nil {
		return "", err
	}
	return bigtableRowKeyFromHash(hash, featureViewName), nil
}
