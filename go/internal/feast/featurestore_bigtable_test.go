package feast

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/bigtable/bttest"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

// TestBigtablePythonMaterializeGoRead materializes the test feature repo into an in-memory Bigtable with
// the Python SDK's `feast apply` and `feast materialize-incremental`, then reads it back through
// FeatureStore.GetOnlineFeatures and the Go Bigtable store, and checks the values and event timestamps
// against the latest parquet row of each driver. It needs the `feast` CLI with the gcp extra
// (google-cloud-bigtable) and skips without it.
func TestBigtablePythonMaterializeGoRead(t *testing.T) {
	if testing.Short() {
		t.Skip("runs feast apply and materialize")
	}
	requireFeastWithBigtable(t)

	srv, err := bttest.NewServer("localhost:0")
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	t.Setenv("BIGTABLE_EMULATOR_HOST", srv.Addr)

	repoPath := newBigtableFeatureRepo(t)
	runFeast(t, repoPath, "apply")
	runFeast(t, repoPath, "materialize-incremental", time.Now().UTC().Format("2006-01-02T15:04:05"))

	config, err := registry.NewRepoConfigFromFile(repoPath)
	require.NoError(t, err)
	fs, err := NewFeatureStore(config, nil)
	require.NoError(t, err)
	t.Cleanup(fs.onlineStore.Destruct)

	const missingDriver = 9999
	drivers := []int64{1001, 1002, 1003, missingDriver}
	entityValues := make([]*types.Value, len(drivers))
	for i, id := range drivers {
		entityValues[i] = &types.Value{Val: &types.Value_Int64Val{Int64Val: id}}
	}
	vectors, err := fs.GetOnlineFeatures(context.Background(),
		[]string{"driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate", "driver_hourly_stats:avg_daily_trips"},
		nil, map[string]*types.RepeatedValue{"driver_id": {Val: entityValues}}, nil, false)
	require.NoError(t, err)

	rows, err := test.ReadParquet(filepath.Join(featureRepoBasePath, "feature_repo", "driver_stats.parquet"))
	require.NoError(t, err)
	latest := test.GetLatestFeatures(rows, map[int64]bool{1001: true, 1002: true, 1003: true})

	byName := make(map[string]int, len(vectors))
	for i, v := range vectors {
		byName[v.Name] = i
	}
	convRate := vectors[byName["conv_rate"]]
	accRate := vectors[byName["acc_rate"]]
	trips := vectors[byName["avg_daily_trips"]]
	for i, id := range drivers {
		if id == missingDriver {
			assert.Equal(t, serving.FieldStatus_NOT_FOUND, convRate.Statuses[i], "driver %d", id)
			continue
		}
		want := latest[id]
		require.NotNil(t, want, "driver %d has no parquet row", id)
		assert.Equal(t, serving.FieldStatus_PRESENT, convRate.Statuses[i], "driver %d", id)
		assert.Equal(t, want.ConvRate, convRate.Values.(*array.Float32).Value(i), "driver %d conv_rate", id)
		assert.Equal(t, want.AccRate, accRate.Values.(*array.Float32).Value(i), "driver %d acc_rate", id)
		assert.Equal(t, int64(want.AvgDailyTrips), trips.Values.(*array.Int64).Value(i), "driver %d avg_daily_trips", id)
		// ReadParquet returns driver_stats.parquet's raw microsecond timestamps (it reads them as arrow.Second).
		assert.Equal(t, time.UnixMicro(want.EventTimestamp).Unix(), convRate.Timestamps[i].GetSeconds(), "driver %d event_ts", id)
	}
}

// requireFeastWithBigtable skips the test unless the `feast` CLI is on PATH and the Python beside it can
// import google.cloud.bigtable (the gcp extra).
func requireFeastWithBigtable(t *testing.T) {
	t.Helper()
	feastPath, err := exec.LookPath("feast")
	if err != nil {
		t.Skip("needs the feast CLI on PATH")
	}
	python := filepath.Join(filepath.Dir(feastPath), "python")
	if out, err := exec.Command(python, "-c", "import google.cloud.bigtable").CombinedOutput(); err != nil {
		t.Skipf("needs the feast CLI with the gcp extra (google-cloud-bigtable): %v: %s", err, out)
	}
}

// newBigtableFeatureRepo copies the test feature repo's definitions and data into a temporary repo whose
// online store is Bigtable on the emulator, and returns its path.
func newBigtableFeatureRepo(t *testing.T) string {
	t.Helper()
	repoPath := t.TempDir()
	source := filepath.Join(featureRepoBasePath, "feature_repo")
	for _, name := range []string{"__init__.py", "example.py", "driver_stats.parquet"} {
		data, err := os.ReadFile(filepath.Join(source, name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(repoPath, name), data, 0o644))
	}
	yaml := `project: bigtable_e2e
registry: data/registry.db
provider: local
online_store:
    type: bigtable
    project_id: test-project
    instance: test-instance
entity_key_serialization_version: 3
`
	require.NoError(t, os.WriteFile(filepath.Join(repoPath, "feature_store.yaml"), []byte(yaml), 0o644))
	return repoPath
}

// runFeast runs one feast CLI command in repoPath, with the test's environment (the emulator address).
func runFeast(t *testing.T, repoPath string, args ...string) {
	t.Helper()
	cmd := exec.Command("feast", args...)
	cmd.Dir = repoPath
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, fmt.Sprintf("feast %v: %s", args, out))
}
