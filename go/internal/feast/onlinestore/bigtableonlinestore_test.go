package onlinestore

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

func TestParseBigtableConfig(t *testing.T) {
	// defaults is the config of {"instance": "i"}: chunks of 5 keys, 50 at a time, no store-wide cap.
	// read_timeout_ms defaults to 60 s, the Python client's ReadRows retry deadline, so an unset budget
	// never lets the client retry without end.
	defaults := bigtableConfig{Instance: "i", KeysPerRequest: 5, MaxConcurrentChunks: 50, ReadTimeout: 60 * time.Second}
	tests := []struct {
		name    string
		in      map[string]interface{}
		want    bigtableConfig
		wantErr string
	}{
		{"defaults", map[string]interface{}{"type": "bigtable", "instance": "i"}, defaults, ""},
		{"all set", map[string]interface{}{"instance": "i", "project_id": "p", "app_profile": "serve",
			"keys_per_request": float64(25), "max_concurrent_chunks": float64(4),
			"read_timeout_ms": float64(150), "max_inflight_reads": float64(300), "max_inflight_wait_ms": float64(15),
			"max_versions": float64(2)},
			bigtableConfig{ProjectID: "p", Instance: "i", AppProfile: "serve", KeysPerRequest: 25,
				MaxConcurrentChunks: 4, ReadTimeout: 150 * time.Millisecond, MaxInflightReads: 300,
				MaxInflightWait: 15 * time.Millisecond}, ""},
		{"int values", map[string]interface{}{"instance": "i", "keys_per_request": 100, "max_versions": 1},
			bigtableConfig{Instance: "i", KeysPerRequest: 100, MaxConcurrentChunks: 50, ReadTimeout: 60 * time.Second}, ""},
		{"missing instance", map[string]interface{}{}, bigtableConfig{}, "instance"},
		{"empty instance", map[string]interface{}{"instance": ""}, bigtableConfig{}, "instance"},
		{"quoted number", map[string]interface{}{"instance": "i", "keys_per_request": "50"}, bigtableConfig{}, "keys_per_request"},
		{"fraction", map[string]interface{}{"instance": "i", "keys_per_request": 2.5}, bigtableConfig{}, "keys_per_request"},
		{"zero", map[string]interface{}{"instance": "i", "max_concurrent_chunks": float64(0)}, bigtableConfig{}, "max_concurrent_chunks"},
		{"negative", map[string]interface{}{"instance": "i", "read_timeout_ms": float64(-1)}, bigtableConfig{}, "read_timeout_ms"},
		{"non-string profile", map[string]interface{}{"instance": "i", "app_profile": float64(1)}, bigtableConfig{}, "app_profile"},
		{"max_versions zero ignored", map[string]interface{}{"instance": "i", "max_versions": float64(0)}, defaults, ""},
		{"max_versions non-number ignored", map[string]interface{}{"instance": "i", "max_versions": "two"}, defaults, ""},
		{"bad max_inflight_reads", map[string]interface{}{"instance": "i", "max_inflight_reads": true}, bigtableConfig{}, "max_inflight_reads"},
		{"zero max_inflight_reads", map[string]interface{}{"instance": "i", "max_inflight_reads": float64(0)}, bigtableConfig{}, "max_inflight_reads"},
		// max_inflight_wait_ms may be 0, which is also its default: a full cap rejects a read at once.
		{"zero max_inflight_wait_ms", map[string]interface{}{"instance": "i", "max_inflight_wait_ms": float64(0)}, defaults, ""},
		{"negative max_inflight_wait_ms", map[string]interface{}{"instance": "i", "max_inflight_wait_ms": float64(-1)},
			bigtableConfig{}, "max_inflight_wait_ms must be a non-negative integer"},
		{"fractional max_inflight_wait_ms", map[string]interface{}{"instance": "i", "max_inflight_wait_ms": 2.5},
			bigtableConfig{}, "max_inflight_wait_ms"},
		{"bad max_inflight_wait_ms", map[string]interface{}{"instance": "i", "max_inflight_wait_ms": "10"},
			bigtableConfig{}, "max_inflight_wait_ms"},
		// Keys only the Python SDK uses, and unknown keys, are ignored.
		{"other keys ignored", map[string]interface{}{"instance": "i", "write_app_profile": "batch",
			"initial_tablets": float64(16), "not_a_setting": true}, defaults, ""},
		// An explicit null (`project_id:`, `read_timeout_ms: null`, or an unset ${VAR} after os.ExpandEnv)
		// leaves the default, as Python's Optional fields do.
		{"null project_id", map[string]interface{}{"instance": "i", "project_id": nil}, defaults, ""},
		{"null app_profile", map[string]interface{}{"instance": "i", "app_profile": nil}, defaults, ""},
		{"null keys_per_request", map[string]interface{}{"instance": "i", "keys_per_request": nil}, defaults, ""},
		{"null read_timeout_ms", map[string]interface{}{"instance": "i", "read_timeout_ms": nil}, defaults, ""},
		{"null max_concurrent_chunks", map[string]interface{}{"instance": "i", "max_concurrent_chunks": nil}, defaults, ""},
		{"null max_inflight_reads", map[string]interface{}{"instance": "i", "max_inflight_reads": nil}, defaults, ""},
		{"null max_inflight_wait_ms", map[string]interface{}{"instance": "i", "max_inflight_wait_ms": nil}, defaults, ""},
		{"null instance", map[string]interface{}{"instance": nil}, bigtableConfig{}, "instance is required"},
		// adaptive_chunking and disable_metrics are false unless set to a YAML boolean true.
		{"switches on", map[string]interface{}{"instance": "i", "adaptive_chunking": true, "disable_metrics": true},
			bigtableConfig{Instance: "i", KeysPerRequest: 5, MaxConcurrentChunks: 50, ReadTimeout: 60 * time.Second,
				AdaptiveChunking: true, DisableMetrics: true}, ""},
		{"switches off", map[string]interface{}{"instance": "i", "adaptive_chunking": false, "disable_metrics": false},
			defaults, ""},
		{"null adaptive_chunking", map[string]interface{}{"instance": "i", "adaptive_chunking": nil}, defaults, ""},
		{"null disable_metrics", map[string]interface{}{"instance": "i", "disable_metrics": nil}, defaults, ""},
		{"quoted disable_metrics", map[string]interface{}{"instance": "i", "disable_metrics": "false"},
			bigtableConfig{}, "disable_metrics must be true or false"},
		{"quoted adaptive_chunking", map[string]interface{}{"instance": "i", "adaptive_chunking": "true"},
			bigtableConfig{}, "adaptive_chunking must be true or false"},
		{"numeric disable_metrics", map[string]interface{}{"instance": "i", "disable_metrics": float64(1)},
			bigtableConfig{}, "disable_metrics must be true or false"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBigtableConfig(tt.in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// bigtableFixtures mirrors testdata/bigtable_fixtures.json, written by the Python generator
// testdata/gen_bigtable_fixtures.py from the in-tree Python BigtableOnlineStore. A Python unit test
// (test_bigtable_go_fixtures.py) fails when the committed file no longer matches what Python writes.
type bigtableFixtures struct {
	TableNames []struct {
		Project  string   `json:"project"`
		Entities []string `json:"entities"`
		Want     string   `json:"want"`
	} `json:"table_names"`
	RowKeys []struct {
		Name       string   `json:"name"`
		Entityless bool     `json:"entityless"`
		JoinKeys   []string `json:"join_keys"`
		Values     []struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"values"`
		View string  `json:"view"`
		Want *string `json:"want"`
	} `json:"row_keys"`
	Cells []struct {
		Name        string          `json:"name"`
		ValueB64    string          `json:"value_b64"`
		EventTS     string          `json:"event_ts"`
		WantSeconds int64           `json:"want_seconds"`
		WantNanos   int32           `json:"want_nanos"`
		WantValue   json.RawMessage `json:"want_value"`
	} `json:"cells"`
	MurmurVectors []struct {
		Length int    `json:"length"`
		Want   string `json:"want"`
	} `json:"murmur_vectors"`
}

func loadBigtableFixtures(t *testing.T) bigtableFixtures {
	t.Helper()
	raw, err := os.ReadFile("testdata/bigtable_fixtures.json")
	require.NoError(t, err)
	var f bigtableFixtures
	require.NoError(t, json.Unmarshal(raw, &f))
	require.NotEmpty(t, f.TableNames)
	require.NotEmpty(t, f.RowKeys)
	require.NotEmpty(t, f.Cells)
	require.NotEmpty(t, f.MurmurVectors)
	return f
}

// fixtureValue builds a types.Value from the generator's {type, value} encoding: bytes are base64,
// integers are decimal strings, strings and uuids are as-is.
func fixtureValue(typ, value string) (*types.Value, error) {
	switch typ {
	case "string":
		return &types.Value{Val: &types.Value_StringVal{StringVal: value}}, nil
	case "uuid":
		return &types.Value{Val: &types.Value_UuidVal{UuidVal: value}}, nil
	case "bytes":
		b, err := base64.StdEncoding.DecodeString(value)
		return &types.Value{Val: &types.Value_BytesVal{BytesVal: b}}, err
	case "int32":
		n, err := strconv.ParseInt(value, 10, 32)
		return &types.Value{Val: &types.Value_Int32Val{Int32Val: int32(n)}}, err
	case "int64":
		n, err := strconv.ParseInt(value, 10, 64)
		return &types.Value{Val: &types.Value_Int64Val{Int64Val: n}}, err
	}
	return nil, fmt.Errorf("unknown fixture value type %q", typ)
}

func TestBigtableTableNameParity(t *testing.T) {
	for _, tc := range loadBigtableFixtures(t).TableNames {
		t.Run(fmt.Sprintf("%s/%v", tc.Project, tc.Entities), func(t *testing.T) {
			got := bigtableTableName(tc.Project, tc.Entities)
			assert.Equal(t, tc.Want, got)
			assert.LessOrEqual(t, len([]rune(got)), bigtableTableNameMaxLength)
		})
	}
}

func TestBigtableRowKeyParity(t *testing.T) {
	entityless := 0
	for _, tc := range loadBigtableFixtures(t).RowKeys {
		if tc.Entityless {
			entityless++
		}
		t.Run(tc.Name, func(t *testing.T) {
			key := &types.EntityKey{JoinKeys: tc.JoinKeys}
			for _, v := range tc.Values {
				val, err := fixtureValue(v.Type, v.Value)
				require.NoError(t, err)
				key.EntityValues = append(key.EntityValues, val)
			}
			if tc.Entityless {
				// The generator built this key from Python's DUMMY_ENTITY_ID / DUMMY_ENTITY_VAL; they must
				// be what the Go model uses.
				assert.Equal(t, []string{model.DUMMY_ENTITY_ID}, tc.JoinKeys)
				assert.True(t, proto.Equal(&model.DUMMY_ENTITY_VALUE, key.EntityValues[0]))
			}
			got, err := bigtableRowKey(key, tc.View, 3)
			if tc.Want == nil {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, *tc.Want, got)
			// Serving builds row keys in rowTargets from the entity hash; it must give the same key.
			hash, err := bigtableEntityHash(key, 3)
			require.NoError(t, err)
			assert.Equal(t, *tc.Want, bigtableRowKeyFromHash(hash, tc.View))
			_, served := (&bigtableTableRead{views: []*bigtableViewRead{{name: tc.View}}}).rowTargets([]string{hash})
			assert.Equal(t, []string{*tc.Want}, []string(served), "the row key serving reads")
		})
	}
	assert.Equal(t, 1, entityless, "fixture must have one entityless row key")
}

func TestNormalizeSerializationVersion(t *testing.T) {
	tests := []struct {
		in, want int64
		wantErr  bool
	}{
		{0, 3, false},
		{3, 3, false},
		{1, 0, true},
		{2, 0, true},
	}
	for _, tt := range tests {
		t.Run(strconv.FormatInt(tt.in, 10), func(t *testing.T) {
			got, err := normalizeSerializationVersion(tt.in)
			if tt.wantErr {
				require.ErrorContains(t, err, "entity_key_serialization_version")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseEventTS(t *testing.T) {
	tenUTC := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		in      string
		want    time.Time
		wantErr bool
	}{
		{"2026-11-17T10:00:00.123456+00:00", tenUTC.Add(123456 * time.Microsecond), false},
		{"2026-11-17T10:00:00+00:00", tenUTC, false},
		{"2026-11-17T15:30:00+05:30", tenUTC, false},
		{"2026-11-17T15:53:28+05:53:28", tenUTC, false}, // offset with seconds, as isoformat writes it
		{"2026-11-17T04:06:31.5-05:53:29", tenUTC.Add(500 * time.Millisecond), false},
		{"2026-11-17T10:00:00", tenUTC, false}, // naive reads as UTC
		{"garbage", time.Time{}, true},
		{"", time.Time{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseEventTS([]byte(tt.in))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want.Unix(), got.GetSeconds())
			assert.Equal(t, int32(tt.want.Nanosecond()), got.GetNanos())
		})
	}
	got, err := parseEventTS([]byte("2026-11-17T10:00:00.123456+00:00"))
	require.NoError(t, err)
	assert.Equal(t, int64(1794909600), got.GetSeconds())
	assert.Equal(t, int32(123456000), got.GetNanos())
}

// TestBigtableCellFixtures checks that Go reads the cells Python writes: each event_ts gives Python's
// epoch seconds and nanos, and each serialized value decodes to the value Python recorded as JSON.
func TestBigtableCellFixtures(t *testing.T) {
	for _, tc := range loadBigtableFixtures(t).Cells {
		t.Run(tc.Name, func(t *testing.T) {
			ts, err := parseEventTS([]byte(tc.EventTS))
			require.NoError(t, err)
			assert.Equal(t, tc.WantSeconds, ts.GetSeconds())
			assert.Equal(t, tc.WantNanos, ts.GetNanos())

			raw, err := base64.StdEncoding.DecodeString(tc.ValueB64)
			require.NoError(t, err)
			var got, want types.Value
			require.NoError(t, proto.Unmarshal(raw, &got))
			require.NoError(t, protojson.Unmarshal(tc.WantValue, &want))
			assert.True(t, proto.Equal(&want, &got), "got %v, want %v", &got, &want)
		})
	}
}

func TestResolveProjectID(t *testing.T) {
	adcErr := errors.New("no credentials")
	tests := []struct {
		name       string
		configured string
		envProject string // GOOGLE_CLOUD_PROJECT
		envLegacy  string // GCLOUD_PROJECT
		found      string
		findErr    error
		want       string
		wantFind   bool
		wantErr    string
	}{
		{name: "configured wins", configured: "p", envProject: "env", envLegacy: "legacy", found: "adc", want: "p"},
		{name: "GOOGLE_CLOUD_PROJECT before credentials", envProject: "env", envLegacy: "legacy", found: "adc",
			want: "env"},
		{name: "GCLOUD_PROJECT before credentials", envLegacy: "legacy", found: "adc", want: "legacy"},
		{name: "falls back to credentials", found: "adc", want: "adc", wantFind: true},
		{name: "all empty", wantFind: true, wantErr: "GOOGLE_CLOUD_PROJECT"},
		{name: "credentials error", findErr: adcErr, wantFind: true, wantErr: "project_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GOOGLE_CLOUD_PROJECT", tt.envProject)
			t.Setenv("GCLOUD_PROJECT", tt.envLegacy)
			called := false
			find := func(context.Context) (string, error) {
				called = true
				return tt.found, tt.findErr
			}
			got, err := resolveProjectID(context.Background(), tt.configured, find)
			assert.Equal(t, tt.wantFind, called, "find called")
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				if tt.findErr != nil {
					assert.ErrorIs(t, err, tt.findErr)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTolerableStartupPingError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		emulator bool
		want     bool
	}{
		{"NotFound (empty instance)", status.Error(codes.NotFound, "No tables found for instance"), false, true},
		{"NotFound wrapped", fmt.Errorf("ping: %w", status.Error(codes.NotFound, "x")), false, true},
		{"Unimplemented on an emulator", status.Error(codes.Unimplemented, "x"), true, true},
		{"Unimplemented on real Bigtable", status.Error(codes.Unimplemented, "x"), false, false},
		{"PermissionDenied", status.Error(codes.PermissionDenied, "x"), false, false},
		{"Unauthenticated", status.Error(codes.Unauthenticated, "x"), true, false},
		{"DeadlineExceeded", status.Error(codes.DeadlineExceeded, "x"), false, false},
		{"context deadline", context.DeadlineExceeded, false, false},
		{"not a status error", errors.New("dial failed"), false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, ok := tolerableStartupPingError(tt.err, tt.emulator)
			assert.Equal(t, tt.want, ok)
			assert.Equal(t, tt.want, reason != "", "a reason exactly when tolerated")
		})
	}
}

// pingAnswerServer is a Bigtable data server whose PingAndWarm returns code; every other RPC is
// unimplemented.
type pingAnswerServer struct {
	btpb.UnimplementedBigtableServer
	code codes.Code
}

func (s *pingAnswerServer) PingAndWarm(context.Context, *btpb.PingAndWarmRequest) (*btpb.PingAndWarmResponse, error) {
	return nil, status.Error(s.code, "test answer")
}

// TestNewBigtableOnlineStoreStartupPingAnswers runs the constructor against a server whose PingAndWarm
// fails with each code, with BIGTABLE_EMULATOR_HOST set as for any emulator. The "real Bigtable" side of
// Unimplemented is covered by TestTolerableStartupPingError: without the emulator variable the client
// would start its Cloud Monitoring exporter.
func TestNewBigtableOnlineStoreStartupPingAnswers(t *testing.T) {
	tests := []struct {
		code      codes.Code
		wantStart bool
	}{
		{codes.NotFound, true},
		{codes.Unimplemented, true},
		{codes.PermissionDenied, false},
		{codes.Unauthenticated, false},
	}
	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			lis, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)
			srv := grpc.NewServer()
			btpb.RegisterBigtableServer(srv, &pingAnswerServer{code: tt.code})
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(srv.Stop)
			t.Setenv("BIGTABLE_EMULATOR_HOST", lis.Addr().String())
			conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })

			cfg := mustParseBigtableConfig(t, map[string]interface{}{"instance": "i", "project_id": "p"})
			store, err := newBigtableOnlineStore(context.Background(), "feast", &registry.RepoConfig{}, cfg,
				5*time.Second, option.WithGRPCConn(conn))
			if !tt.wantStart {
				require.ErrorContains(t, err, "PingAndWarm")
				assert.Equal(t, tt.code, status.Code(err))
				assert.Nil(t, store)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, store)
			store.Destruct()
		})
	}
}

func TestNewBigtableOnlineStoreAgainstBttest(t *testing.T) {
	store, _, _ := newTestBigtable(t, map[string]interface{}{"instance": "i", "project_id": "p"})
	require.NotNil(t, store)
	assert.Equal(t, testBigtableFeastProject, store.project)
	assert.Equal(t, int64(bigtableSerializationVersion), store.serializationVersion)
	assert.Same(t, store.table("t"), store.table("t"), "table handles are cached")

	_, err := store.OnlineRead(context.Background(), nil, nil, nil)
	require.ErrorIs(t, err, errors.ErrUnsupported)

	assert.NotPanics(t, store.Destruct)
	assert.NotPanics(t, store.Destruct)
}

func TestNewBigtableOnlineStoreRejectsOldSerialization(t *testing.T) {
	cfg := map[string]interface{}{"type": "bigtable", "instance": "i", "project_id": "p"}
	repoConfig := &registry.RepoConfig{Project: "feast", OnlineStore: cfg, EntityKeySerializationVersion: 2}
	_, err := NewBigtableOnlineStore(repoConfig.Project, repoConfig, cfg)
	require.ErrorContains(t, err, "entity_key_serialization_version")
}

// TestNewBigtableOnlineStoreStartupTimeout points the store at a TCP listener that accepts connections
// and never speaks gRPC, so PingAndWarm can only end by the startup timeout.
func TestNewBigtableOnlineStoreStartupTimeout(t *testing.T) {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Setenv("BIGTABLE_EMULATOR_HOST", lis.Addr().String()) // no built-in metrics exporter
	accepted := make(chan []net.Conn, 1)
	go func() {
		var conns []net.Conn
		for {
			c, err := lis.Accept()
			if err != nil {
				accepted <- conns
				return
			}
			conns = append(conns, c)
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		for _, c := range <-accepted {
			_ = c.Close()
		}
	})
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	cfg := mustParseBigtableConfig(t, map[string]interface{}{"instance": "i", "project_id": "p"})
	type result struct {
		store *BigtableOnlineStore
		err   error
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		s, err := newBigtableOnlineStore(context.Background(), "feast", &registry.RepoConfig{}, cfg,
			200*time.Millisecond, option.WithGRPCConn(conn))
		done <- result{s, err}
	}()
	select {
	case r := <-done:
		require.ErrorContains(t, r.err, "PingAndWarm")
		assert.Nil(t, r.store)
		assert.Less(t, time.Since(start), 2*time.Second)
	case <-time.After(2 * time.Second):
		t.Fatal("constructor did not return within 2s of a 200ms startup timeout")
	}
}

// TestBigtableConfigValidate: validate rejects settings that would break reads (slices.Chunk panics on a
// zero size, a zero-capacity slot channel blocks forever, a negative channel size panics, and Bigtable
// takes at most 100 keys per request), and the constructor runs it before creating a client.
func TestBigtableConfigValidate(t *testing.T) {
	valid := mustParseBigtableConfig(t, map[string]interface{}{"instance": "i", "project_id": "p"})
	require.NoError(t, valid.validate())
	tests := []struct {
		name    string
		edit    func(*bigtableConfig)
		wantErr string
	}{
		{"zero keys_per_request", func(c *bigtableConfig) { c.KeysPerRequest = 0 }, "keys_per_request must be between 1 and 100, got 0"},
		{"keys_per_request over 100", func(c *bigtableConfig) { c.KeysPerRequest = 101 }, "keys_per_request must be between 1 and 100, got 101"},
		{"zero max_concurrent_chunks", func(c *bigtableConfig) { c.MaxConcurrentChunks = 0 }, "max_concurrent_chunks must be positive, got 0"},
		{"zero read timeout", func(c *bigtableConfig) { c.ReadTimeout = 0 }, "read_timeout_ms must be positive, got 0"},
		{"negative max_inflight_reads", func(c *bigtableConfig) { c.MaxInflightReads = -1 }, "max_inflight_reads must not be negative, got -1"},
		{"negative max_inflight_wait_ms", func(c *bigtableConfig) { c.MaxInflightWait = -time.Millisecond }, "max_inflight_wait_ms must not be negative, got -1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.edit(&cfg)
			require.ErrorContains(t, cfg.validate(), tt.wantErr)
			s, err := newBigtableOnlineStore(context.Background(), "feast", &registry.RepoConfig{}, cfg, time.Second)
			require.ErrorContains(t, err, tt.wantErr)
			assert.Nil(t, s)
		})
	}
	over, err := parseBigtableConfig(map[string]interface{}{"instance": "i", "keys_per_request": float64(101)})
	require.NoError(t, err, "parse reads the value; validate rejects it")
	require.ErrorContains(t, over.validate(), "keys_per_request")
}

func TestNewOnlineStoreBigtableType(t *testing.T) {
	_, err := NewOnlineStore(&registry.RepoConfig{
		Project:     "feast",
		OnlineStore: map[string]interface{}{"type": "bigtable"},
	})
	require.ErrorContains(t, err, "instance")
}

// TestBigtableMurmurHashVectors: the entity-id hash matches Python's mmh3.hash_bytes(bytes(range(n)))
// for every length 1 to 100, which covers every 16-byte tail size and several full blocks.
func TestBigtableMurmurHashVectors(t *testing.T) {
	vectors := loadBigtableFixtures(t).MurmurVectors
	require.Len(t, vectors, 100)
	for i, v := range vectors {
		require.Equal(t, i+1, v.Length)
		data := make([]byte, v.Length)
		for j := range data {
			data[j] = byte(j)
		}
		got := bigtableMurmurHash(data)
		assert.Equal(t, v.Want, hex.EncodeToString(got[:]), "length %d", v.Length)
	}
}
