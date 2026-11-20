package onlinestore

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

// seedRow is one Bigtable row to write in family "features": qualifier -> raw cell bytes, all cells at
// timestamp Micros (a whole number of milliseconds, as Bigtable requires).
type seedRow struct {
	Key    string
	Cells  map[string][]byte
	Micros int64
}

// seedRows creates table (with family "features") if it does not exist yet, then writes each row with
// its own Apply and an explicit cell timestamp. With no rows it only creates the table.
func seedRows(t *testing.T, admin *bigtable.AdminClient, client *bigtable.Client, table string, rows ...seedRow) {
	t.Helper()
	ctx := context.Background()
	if err := admin.CreateTable(ctx, table); status.Code(err) != codes.AlreadyExists {
		require.NoError(t, err)
	}
	if err := admin.CreateColumnFamily(ctx, table, bigtableFeatureFamily); status.Code(err) != codes.AlreadyExists {
		require.NoError(t, err)
	}
	tbl := client.Open(table)
	for _, r := range rows {
		mut := bigtable.NewMutation()
		for qualifier, value := range r.Cells {
			mut.Set(bigtableFeatureFamily, qualifier, bigtable.Timestamp(r.Micros), value)
		}
		require.NoError(t, tbl.Apply(ctx, r.Key, mut))
	}
}

// pyCell is a Python-written cell from testdata/bigtable_fixtures.json: the serialized value, the
// event_ts string Python wrote with it, and what both must decode to.
type pyCell struct {
	raw     []byte
	eventTS []byte
	want    *types.Value
	seconds int64
	nanos   int32
}

// readEnv is a store on bttest plus the Python-produced cells to seed it with.
type readEnv struct {
	store  *BigtableOnlineStore
	admin  *bigtable.AdminClient
	client *bigtable.Client
	cells  map[string]pyCell // by fixture name: "int64", "string", "float list", ...
}

func newReadEnv(t *testing.T) *readEnv {
	t.Helper()
	return newReadEnvWithConfig(t, nil)
}

// newReadEnvWithConfig is newReadEnv with extra online_store keys (instance and project_id are set) and
// options for the bttest server, such as a readRowsSpy.
func newReadEnvWithConfig(t *testing.T, extra map[string]interface{}, srvOpts ...grpc.ServerOption) *readEnv {
	t.Helper()
	cfg := map[string]interface{}{"instance": "i", "project_id": "p"}
	for k, v := range extra {
		cfg[k] = v
	}
	store, admin, client := newTestBigtable(t, cfg, srvOpts...)
	env := &readEnv{store: store, admin: admin, client: client, cells: map[string]pyCell{}}
	for _, c := range loadBigtableFixtures(t).Cells {
		raw, err := base64.StdEncoding.DecodeString(c.ValueB64)
		require.NoError(t, err)
		want := &types.Value{}
		require.NoError(t, protojson.Unmarshal(c.WantValue, want))
		env.cells[c.Name] = pyCell{raw: raw, eventTS: []byte(c.EventTS), want: want, seconds: c.WantSeconds, nanos: c.WantNanos}
	}
	for _, name := range []string{"int64", "string", "float list", "double", "bool"} {
		require.Contains(t, env.cells, name)
	}
	return env
}

// cell returns a fixture cell by name.
func (e *readEnv) cell(name string) pyCell { return e.cells[name] }

// rowKey is the Python row key of entity key ek in view.
func (e *readEnv) rowKey(t *testing.T, ek *types.EntityKey, view string) string {
	t.Helper()
	key, err := bigtableRowKey(ek, view, e.store.serializationVersion)
	require.NoError(t, err)
	return key
}

// read calls OnlineReadWithEntities with every view on entity "subscriber" unless byView says otherwise.
func (e *readEnv) read(entityKeys []*types.EntityKey, views, features []string, byView map[string][]string) ([][]FeatureData, error) {
	if byView == nil {
		byView = map[string][]string{}
		for _, v := range views {
			byView[v] = []string{"subscriber"}
		}
	}
	return e.store.OnlineReadWithEntities(context.Background(), entityKeys, views, features, byView)
}

const subscriberTable = testBigtableFeastProject + ".subscriber"

func userKey(id string) *types.EntityKey {
	return &types.EntityKey{
		JoinKeys:     []string{"user_profile_id"},
		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: id}}},
	}
}

// cells builds a row's cells: event_ts from tsCell, then qualifier/cell pairs.
func cells(tsCell pyCell, pairs ...interface{}) map[string][]byte {
	m := map[string][]byte{bigtableEventTSQualifier: tsCell.eventTS}
	for i := 0; i < len(pairs); i += 2 {
		switch v := pairs[i+1].(type) {
		case pyCell:
			m[pairs[i].(string)] = v.raw
		case []byte:
			m[pairs[i].(string)] = v
		}
	}
	return m
}

// assertSlot checks one output slot: reference, value and timestamp. A nil want means NullVal.
func assertSlot(t *testing.T, got *FeatureData, view, feature string, want *types.Value, seconds int64, nanos int32) {
	t.Helper()
	assert.Equal(t, view, got.Reference.FeatureViewName, "view of %s", feature)
	assert.Equal(t, feature, got.Reference.FeatureName)
	if want == nil {
		want = &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}}
	}
	assert.True(t, proto.Equal(want, &got.Value), "%s/%s: got %v, want %v", view, feature, &got.Value, want)
	assert.Equal(t, seconds, got.Timestamp.GetSeconds(), "%s/%s seconds", view, feature)
	assert.Equal(t, nanos, got.Timestamp.GetNanos(), "%s/%s nanos", view, feature)
}

// assertCell checks a slot that holds fixture cell c and the row timestamp of tsCell.
func assertCell(t *testing.T, got *FeatureData, view, feature string, c, tsCell pyCell) {
	t.Helper()
	assertSlot(t, got, view, feature, c.want, tsCell.seconds, tsCell.nanos)
}

func requireShape(t *testing.T, got [][]FeatureData, rowLens ...int) {
	t.Helper()
	require.Len(t, got, len(rowLens))
	for i, n := range rowLens {
		if n < 0 {
			require.Nil(t, got[i], "row %d", i)
			continue
		}
		require.NotNil(t, got[i], "row %d", i)
		require.Len(t, got[i], n, "row %d", i)
	}
}

func TestBigtableReadAllFeatures(t *testing.T) {
	e := newReadEnv(t)
	i64, str, fl := e.cell("int64"), e.cell("string"), e.cell("float list")
	u1, u2 := userKey("u1"), userKey("u2")
	seedRows(t, e.admin, e.client, subscriberTable,
		seedRow{Key: e.rowKey(t, u1, "fvA"), Cells: cells(i64, "f1", i64, "f2", str), Micros: 1000},
		seedRow{Key: e.rowKey(t, u2, "fvA"), Cells: cells(str, "f1", fl, "f2", i64), Micros: 1000},
	)
	got, err := e.read([]*types.EntityKey{u1, u2}, []string{"fvA", "fvA"}, []string{"f1", "f2"}, nil)
	require.NoError(t, err)
	requireShape(t, got, 2, 2)
	assertCell(t, &got[0][0], "fvA", "f1", i64, i64)
	assertCell(t, &got[0][1], "fvA", "f2", str, i64)
	assertCell(t, &got[1][0], "fvA", "f1", fl, str) // +05:30 event_ts
	assertCell(t, &got[1][1], "fvA", "f2", i64, str)
}

func TestBigtableReadFeatureSubset(t *testing.T) {
	e := newReadEnv(t)
	i64, str := e.cell("int64"), e.cell("string")
	u1 := userKey("u1")
	seedRows(t, e.admin, e.client, subscriberTable,
		seedRow{Key: e.rowKey(t, u1, "fvA"), Cells: cells(i64, "f1", i64, "f2", str), Micros: 1000})
	got, err := e.read([]*types.EntityKey{u1}, []string{"fvA"}, []string{"f2"}, nil)
	require.NoError(t, err)
	requireShape(t, got, 1)
	assertCell(t, &got[0][0], "fvA", "f2", str, i64)
}

func TestBigtableReadTwoViewsOneTable(t *testing.T) {
	e := newReadEnv(t)
	i64, str, dbl := e.cell("int64"), e.cell("string"), e.cell("double")
	u1 := userKey("u1")
	seedRows(t, e.admin, e.client, subscriberTable,
		seedRow{Key: e.rowKey(t, u1, "fvA"), Cells: cells(i64, "a1", i64), Micros: 1000},
		seedRow{Key: e.rowKey(t, u1, "fvB"), Cells: cells(str, "b1", dbl, "b2", str), Micros: 1000},
	)
	got, err := e.read([]*types.EntityKey{u1}, []string{"fvB", "fvA", "fvB"}, []string{"b2", "a1", "b1"}, nil)
	require.NoError(t, err)
	requireShape(t, got, 3)
	assertCell(t, &got[0][0], "fvB", "b2", str, str)
	assertCell(t, &got[0][1], "fvA", "a1", i64, i64)
	assertCell(t, &got[0][2], "fvB", "b1", dbl, str)
}

// TestBigtableReadTwoTablesInOneCall: fvA is on entity a, fvB on entity b, both with join key
// user_profile_id. One call must read both tables.
func TestBigtableReadTwoTablesInOneCall(t *testing.T) {
	e := newReadEnv(t)
	i64, str, bl := e.cell("int64"), e.cell("string"), e.cell("bool")
	u1, u2 := userKey("u1"), userKey("u2")
	tableA, tableB := testBigtableFeastProject+".a", testBigtableFeastProject+".b"
	seedRows(t, e.admin, e.client, tableA,
		seedRow{Key: e.rowKey(t, u1, "fvA"), Cells: cells(i64, "f", i64), Micros: 1000})
	seedRows(t, e.admin, e.client, tableB,
		seedRow{Key: e.rowKey(t, u1, "fvB"), Cells: cells(str, "f", str), Micros: 1000},
		seedRow{Key: e.rowKey(t, u2, "fvB"), Cells: cells(i64, "f", bl), Micros: 1000},
	)
	byView := map[string][]string{"fvA": {"a"}, "fvB": {"b"}}
	got, err := e.read([]*types.EntityKey{u1, u2}, []string{"fvA", "fvB"}, []string{"f", "f"}, byView)
	require.NoError(t, err)
	requireShape(t, got, 2, 2)
	assertCell(t, &got[0][0], "fvA", "f", i64, i64)
	assertCell(t, &got[0][1], "fvB", "f", str, str)
	assertSlot(t, &got[1][0], "fvA", "f", nil, 0, 0) // u2 has no fvA row in table a
	assertCell(t, &got[1][1], "fvB", "f", bl, i64)
}

// TestBigtableReadEntityless reads an entityless view from "<project>.__dummy". The serving layer
// passes the model's ["__dummy"]; an empty entity list names the same table.
func TestBigtableReadEntityless(t *testing.T) {
	for name, entities := range map[string][]string{"model dummy entity": {model.DUMMY_ENTITY_NAME}, "no entities": {}} {
		t.Run(name, func(t *testing.T) {
			e := newReadEnv(t)
			i64 := e.cell("int64")
			dummy := &types.EntityKey{
				JoinKeys:     []string{model.DUMMY_ENTITY_ID},
				EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: model.DUMMY_ENTITY_VAL}}},
			}
			seedRows(t, e.admin, e.client, testBigtableFeastProject+".__dummy",
				seedRow{Key: e.rowKey(t, dummy, "global_stats"), Cells: cells(i64, "total", i64), Micros: 1000})
			got, err := e.read([]*types.EntityKey{dummy}, []string{"global_stats"}, []string{"total"},
				map[string][]string{"global_stats": entities})
			require.NoError(t, err)
			requireShape(t, got, 1)
			assertCell(t, &got[0][0], "global_stats", "total", i64, i64)
		})
	}
}

func TestBigtableReadMissingRow(t *testing.T) {
	e := newReadEnv(t)
	i64 := e.cell("int64")
	u1, ghost := userKey("u1"), userKey("ghost")
	seedRows(t, e.admin, e.client, subscriberTable,
		seedRow{Key: e.rowKey(t, u1, "fvA"), Cells: cells(i64, "f1", i64), Micros: 1000})
	got, err := e.read([]*types.EntityKey{ghost, u1}, []string{"fvA"}, []string{"f1"}, nil)
	require.NoError(t, err)
	requireShape(t, got, -1, 1)
	assertCell(t, &got[1][0], "fvA", "f1", i64, i64)
}

func TestBigtableReadPartialEntity(t *testing.T) {
	e := newReadEnv(t)
	i64 := e.cell("int64")
	u1, ghost := userKey("u1"), userKey("ghost")
	seedRows(t, e.admin, e.client, subscriberTable,
		seedRow{Key: e.rowKey(t, u1, "fvA"), Cells: cells(i64, "a1", i64), Micros: 1000})
	got, err := e.read([]*types.EntityKey{u1, ghost}, []string{"fvA", "fvB", "fvB"}, []string{"a1", "b1", "b2"}, nil)
	require.NoError(t, err)
	requireShape(t, got, 3, -1)
	assertCell(t, &got[0][0], "fvA", "a1", i64, i64)
	assertSlot(t, &got[0][1], "fvB", "b1", nil, 0, 0)
	assertSlot(t, &got[0][2], "fvB", "b2", nil, 0, 0)
}

func TestBigtableReadMissingFeature(t *testing.T) {
	e := newReadEnv(t)
	i64, str := e.cell("int64"), e.cell("string")
	u1 := userKey("u1")
	seedRows(t, e.admin, e.client, subscriberTable,
		seedRow{Key: e.rowKey(t, u1, "fvA"), Cells: cells(str, "f1", i64), Micros: 1000})
	got, err := e.read([]*types.EntityKey{u1}, []string{"fvA", "fvA"}, []string{"f1", "f2"}, nil)
	require.NoError(t, err)
	requireShape(t, got, 2)
	assertCell(t, &got[0][0], "fvA", "f1", i64, str)
	assertSlot(t, &got[0][1], "fvA", "f2", nil, str.seconds, str.nanos) // NullVal keeps the row's event_ts
}

func TestBigtableReadLatestVersionOnly(t *testing.T) {
	e := newReadEnv(t)
	i64, str := e.cell("int64"), e.cell("string")
	u1 := userKey("u1")
	key := e.rowKey(t, u1, "fvA")
	seedRows(t, e.admin, e.client, subscriberTable,
		seedRow{Key: key, Cells: cells(i64, "f1", i64), Micros: 2000},
		seedRow{Key: key, Cells: cells(str, "f1", str), Micros: 1000}, // older version, written second
	)
	got, err := e.read([]*types.EntityKey{u1}, []string{"fvA"}, []string{"f1"}, nil)
	require.NoError(t, err)
	requireShape(t, got, 1)
	assertCell(t, &got[0][0], "fvA", "f1", i64, i64)
}

func TestBigtableReadRegexMetacharacters(t *testing.T) {
	e := newReadEnv(t)
	i64, str, dbl := e.cell("int64"), e.cell("string"), e.cell("double")
	u1 := userKey("u1")
	seedRows(t, e.admin, e.client, subscriberTable,
		seedRow{Key: e.rowKey(t, u1, "fvA"), Cells: cells(i64, "a.b", i64, "c+d", str, "aXb", dbl), Micros: 1000})
	got, err := e.read([]*types.EntityKey{u1}, []string{"fvA", "fvA"}, []string{"a.b", "c+d"}, nil)
	require.NoError(t, err)
	requireShape(t, got, 2)
	assertCell(t, &got[0][0], "fvA", "a.b", i64, i64)
	assertCell(t, &got[0][1], "fvA", "c+d", str, i64)
}

func TestBigtableQualifierPattern(t *testing.T) {
	plan, err := planBigtableRead("p", []string{"fvA", "fvB", "fvA", "fvA"}, []string{"c+d", "a.b", "a.b", "c+d"},
		map[string][]string{"fvA": {"e"}, "fvB": {"e"}})
	require.NoError(t, err)
	require.Len(t, plan.tables, 1)
	assert.Equal(t, `^(a\.b|c\+d|event_ts)$`, plan.tables[0].qualifierPattern())
}

// TestBigtableReadFilterKeepsNewestCellOnly pins the filter chain: the requested qualifiers, then
// LatestNFilter(1) (printed as col(*,1)). Dropping the latest-cell limit would return older versions,
// which Bigtable's in-memory tier never serves.
func TestBigtableReadFilterKeepsNewestCellOnly(t *testing.T) {
	plan, err := planBigtableRead("p", []string{"fvA"}, []string{"f"}, map[string][]string{"fvA": {"e"}})
	require.NoError(t, err)
	require.Len(t, plan.tables, 1)
	assert.Equal(t, `(col(.*:^(f|event_ts)$) | col(*,1))`, plan.tables[0].readFilter().String())
}

// TestBigtableReadIgnoresUnrequestedQualifiers: a qualifier the request did not ask for must not change
// the slots or fail the read.
func TestBigtableReadIgnoresUnrequestedQualifiers(t *testing.T) {
	// A stale qualifier left by an older schema; the server-side filter drops it.
	t.Run("stale qualifier", func(t *testing.T) {
		e := newReadEnv(t)
		i64 := e.cell("int64")
		u1 := userKey("u1")
		seedRows(t, e.admin, e.client, subscriberTable,
			seedRow{Key: e.rowKey(t, u1, "fvA"), Cells: cells(i64, "f1", i64, "old_feature", []byte{0xff, 0xff}), Micros: 1000})
		got, err := e.read([]*types.EntityKey{u1}, []string{"fvA"}, []string{"f1"}, nil)
		require.NoError(t, err)
		requireShape(t, got, 1)
		assertCell(t, &got[0][0], "fvA", "f1", i64, i64)
	})
	// The filter is the union of the table's requested names, so fvA's corrupt b1 passes it because
	// fvB:b1 is requested. The callback must skip it for fvA and still read fvB's own b1.
	t.Run("other view's qualifier", func(t *testing.T) {
		e := newReadEnv(t)
		i64, str := e.cell("int64"), e.cell("string")
		u1 := userKey("u1")
		seedRows(t, e.admin, e.client, subscriberTable,
			seedRow{Key: e.rowKey(t, u1, "fvA"), Cells: cells(i64, "a1", i64, "b1", []byte{0xff, 0xff}), Micros: 1000},
			seedRow{Key: e.rowKey(t, u1, "fvB"), Cells: cells(str, "b1", str), Micros: 1000},
		)
		got, err := e.read([]*types.EntityKey{u1}, []string{"fvA", "fvB"}, []string{"a1", "b1"}, nil)
		require.NoError(t, err)
		requireShape(t, got, 2)
		assertCell(t, &got[0][0], "fvA", "a1", i64, i64)
		assertCell(t, &got[0][1], "fvB", "b1", str, str)
	})
}

// TestBigtableReadDuplicatesAndSameNames: entity u1 at positions 0 and 2, fvA:f1 requested twice, and
// f1 in both fvA and fvB. Every position must get its own view's value.
func TestBigtableReadDuplicatesAndSameNames(t *testing.T) {
	e := newReadEnv(t)
	i64, str, fl, dbl := e.cell("int64"), e.cell("string"), e.cell("float list"), e.cell("double")
	u1, u2 := userKey("u1"), userKey("u2")
	seedRows(t, e.admin, e.client, subscriberTable,
		seedRow{Key: e.rowKey(t, u1, "fvA"), Cells: cells(i64, "f1", i64, "f2", str), Micros: 1000},
		seedRow{Key: e.rowKey(t, u1, "fvB"), Cells: cells(str, "f1", fl), Micros: 1000},
		seedRow{Key: e.rowKey(t, u2, "fvA"), Cells: cells(str, "f1", dbl, "f2", i64), Micros: 1000},
	)
	got, err := e.read([]*types.EntityKey{u1, u2, u1},
		[]string{"fvA", "fvA", "fvB", "fvA"}, []string{"f1", "f2", "f1", "f1"}, nil)
	require.NoError(t, err)
	requireShape(t, got, 4, 4, 4)
	for _, row := range []int{0, 2} {
		assertCell(t, &got[row][0], "fvA", "f1", i64, i64)
		assertCell(t, &got[row][1], "fvA", "f2", str, i64)
		assertCell(t, &got[row][2], "fvB", "f1", fl, str)
		assertCell(t, &got[row][3], "fvA", "f1", i64, i64)
	}
	assertCell(t, &got[1][0], "fvA", "f1", dbl, str)
	assertCell(t, &got[1][1], "fvA", "f2", i64, str)
	assertSlot(t, &got[1][2], "fvB", "f1", nil, 0, 0)
	assertCell(t, &got[1][3], "fvA", "f1", dbl, str)
}

// TestBigtableReadCorruptCellsFail: corrupt data fails the whole read, never NOT_FOUND, and no partial
// result comes back even though the other entity's row is fine.
func TestBigtableReadCorruptCellsFail(t *testing.T) {
	tests := []struct {
		name      string
		cells     func(e *readEnv) map[string][]byte
		qualifier string
	}{
		{"bad event_ts", func(e *readEnv) map[string][]byte {
			return map[string][]byte{"event_ts": []byte("not a time"), "f1": e.cell("int64").raw}
		}, "event_ts"},
		{"missing event_ts", func(e *readEnv) map[string][]byte {
			return map[string][]byte{"f1": e.cell("int64").raw}
		}, "event_ts"},
		{"undecodable value", func(e *readEnv) map[string][]byte {
			return cells(e.cell("int64"), "f1", []byte{0xff, 0xff})
		}, "f1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newReadEnv(t)
			good, bad := userKey("good"), userKey("bad")
			seedRows(t, e.admin, e.client, subscriberTable,
				seedRow{Key: e.rowKey(t, good, "fvA"), Cells: cells(e.cell("int64"), "f1", e.cell("int64")), Micros: 1000},
				seedRow{Key: e.rowKey(t, bad, "fvA"), Cells: tt.cells(e), Micros: 1000},
			)
			got, err := e.read([]*types.EntityKey{good, bad}, []string{"fvA"}, []string{"f1"}, nil)
			require.Error(t, err)
			assert.Nil(t, got)
			assert.ErrorContains(t, err, subscriberTable)
			assert.ErrorContains(t, err, "fvA")
			assert.ErrorContains(t, err, tt.qualifier)
			assert.Equal(t, 1, strings.Count(err.Error(), "bigtable online store:"), "one store prefix: %v", err)
		})
	}
}

// TestBigtableReadMissingTableErrors: a view whose table was never created fails with the table's
// name.
func TestBigtableReadMissingTableErrors(t *testing.T) {
	e := newReadEnv(t)
	got, err := e.read([]*types.EntityKey{userKey("u1")}, []string{"fvA"}, []string{"f1"},
		map[string][]string{"fvA": {"never_created"}})
	require.ErrorContains(t, err, testBigtableFeastProject+".never_created")
	assert.Equal(t, codes.NotFound, status.Code(err), "the gRPC NotFound status stays wrapped")
	assert.Nil(t, got)
}

func TestBigtableReadInputValidation(t *testing.T) {
	e := newReadEnv(t)
	ctx := context.Background()
	u1 := []*types.EntityKey{userKey("u1")}

	got, err := e.store.OnlineReadWithEntities(ctx, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Empty(t, got)

	// Entity keys but no features: one nil row per key, no error.
	got, err = e.store.OnlineReadWithEntities(ctx, []*types.EntityKey{userKey("u1"), userKey("u2")}, nil, nil, nil)
	require.NoError(t, err)
	requireShape(t, got, -1, -1)

	_, err = e.store.OnlineReadWithEntities(ctx, u1, []string{"fvA", "fvA"}, []string{"f1"},
		map[string][]string{"fvA": {"subscriber"}})
	require.ErrorContains(t, err, "featureViewNames")

	got, err = e.store.OnlineReadWithEntities(ctx, u1, []string{"fvA", "fvB"}, []string{"f1", "f1"},
		map[string][]string{"fvA": {"subscriber"}})
	require.ErrorContains(t, err, "fvB")
	assert.Nil(t, got)
}

func TestBigtableOnlineReadNeedsEntities(t *testing.T) {
	e := newReadEnv(t)
	got, err := e.store.OnlineRead(context.Background(), []*types.EntityKey{userKey("u1")}, []string{"fvA"}, []string{"f1"})
	require.ErrorIs(t, err, errors.ErrUnsupported)
	assert.ErrorContains(t, err, "OnlineReadWithEntities")
	assert.Nil(t, got)
}

// readRowsSpy is a bttest stream interceptor that records every ReadRows request, tracks how many
// ReadRows streams are open at once, and can hold a stream (hold) after its request arrives and before
// the server answers it. Set hold before the server starts.
type readRowsSpy struct {
	hold func(ctx context.Context, req *btpb.ReadRowsRequest)

	mu                    sync.Mutex
	requests              []*btpb.ReadRowsRequest
	inFlight, maxInFlight int
}

const readRowsMethod = "/google.bigtable.v2.Bigtable/ReadRows"

func (s *readRowsSpy) option() grpc.ServerOption {
	return grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod != readRowsMethod {
			return handler(srv, ss)
		}
		s.mu.Lock()
		s.inFlight++
		s.maxInFlight = max(s.maxInFlight, s.inFlight)
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			s.inFlight--
			s.mu.Unlock()
		}()
		return handler(srv, &spyStream{ServerStream: ss, spy: s})
	})
}

func (s *readRowsSpy) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests, s.maxInFlight = nil, 0
}

func (s *readRowsSpy) requestCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

// keysPerStreamByTable returns, for each table, the row-key count of each ReadRows stream it received.
func (s *readRowsSpy) keysPerStreamByTable() map[string][]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]int{}
	for _, req := range s.requests {
		table := req.GetTableName()[strings.LastIndex(req.GetTableName(), "/")+1:]
		out[table] = append(out[table], len(req.GetRows().GetRowKeys()))
	}
	return out
}

type spyStream struct {
	grpc.ServerStream
	spy *readRowsSpy
}

func (w *spyStream) RecvMsg(m any) error {
	if err := w.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if req, ok := m.(*btpb.ReadRowsRequest); ok {
		w.spy.mu.Lock()
		w.spy.requests = append(w.spy.requests, req)
		w.spy.mu.Unlock()
		if w.spy.hold != nil {
			w.spy.hold(w.Context(), req)
		}
	}
	return nil
}

func requestHasKey(req *btpb.ReadRowsRequest, key string) bool {
	for _, k := range req.GetRows().GetRowKeys() {
		if string(k) == key {
			return true
		}
	}
	return false
}

// userKeys returns n entity keys u000, u001, ...
func userKeys(n int) []*types.EntityKey {
	keys := make([]*types.EntityKey, n)
	for i := range keys {
		keys[i] = userKey(fmt.Sprintf("u%03d", i))
	}
	return keys
}

// int64Cell is a serialized feast.types.Value holding v.
func int64Cell(t *testing.T, v int64) []byte {
	t.Helper()
	b, err := proto.Marshal(&types.Value{Val: &types.Value_Int64Val{Int64Val: v}})
	require.NoError(t, err)
	return b
}

// seedView writes view's row for every key in keys into table, feature "f" = the key's index.
func (e *readEnv) seedView(t *testing.T, table, view string, keys []*types.EntityKey) {
	t.Helper()
	rows := make([]seedRow, len(keys))
	for i, k := range keys {
		rows[i] = seedRow{Key: e.rowKey(t, k, view), Cells: cells(e.cell("int64"), "f", int64Cell(t, int64(i))), Micros: 1000}
	}
	seedRows(t, e.admin, e.client, table, rows...)
}

// TestBigtableReadChunkBoundaries: each table's row keys go out in ceil(keys/keys_per_request)
// ReadRows streams of at most keys_per_request keys, and every entity is filled.
func TestBigtableReadChunkBoundaries(t *testing.T) {
	tableA, tableB := testBigtableFeastProject+".a", testBigtableFeastProject+".b"
	byView := map[string][]string{"fvA": {"a"}, "fvB": {"b"}}
	for _, perRequest := range []int{100, 50} {
		t.Run(fmt.Sprintf("keys_per_request %d", perRequest), func(t *testing.T) {
			spy := &readRowsSpy{}
			e := newReadEnvWithConfig(t, map[string]interface{}{"keys_per_request": float64(perRequest)}, spy.option())
			keys := userKeys(101)
			e.seedView(t, tableA, "fvA", keys)
			e.seedView(t, tableB, "fvB", keys)
			i64 := e.cell("int64")
			for _, n := range []int{0, 1, 100, 101} {
				t.Run(fmt.Sprintf("%d entities", n), func(t *testing.T) {
					spy.reset()
					got, err := e.read(keys[:n], []string{"fvA", "fvB"}, []string{"f", "f"}, byView)
					require.NoError(t, err)
					require.Len(t, got, n)
					for i, row := range got {
						require.Len(t, row, 2, "entity %d", i)
						want := &types.Value{Val: &types.Value_Int64Val{Int64Val: int64(i)}}
						assertSlot(t, &row[0], "fvA", "f", want, i64.seconds, i64.nanos)
						assertSlot(t, &row[1], "fvB", "f", want, i64.seconds, i64.nanos)
					}
					streams := spy.keysPerStreamByTable()
					for _, table := range []string{tableA, tableB} {
						assert.Len(t, streams[table], (n+perRequest-1)/perRequest, "ReadRows streams on %s", table)
						total := 0
						for _, c := range streams[table] {
							assert.LessOrEqual(t, c, perRequest)
							total += c
						}
						assert.Equal(t, n, total, "row keys sent to %s", table)
					}
				})
			}
		})
	}
}

// TestBigtableReadConcurrencyLimit: max_concurrent_chunks caps the streams of one call across all its
// tables, not per table.
func TestBigtableReadConcurrencyLimit(t *testing.T) {
	// The first two streams wait for each other, so both are open at once whatever the timing; later
	// streams pass at once. The 5 s bound only stops a broken test hanging.
	var arrived atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	spy := &readRowsSpy{hold: func(ctx context.Context, _ *btpb.ReadRowsRequest) {
		if arrived.Add(1) >= 2 {
			releaseOnce.Do(func() { close(release) })
		}
		select {
		case <-release:
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}}
	e := newReadEnvWithConfig(t, map[string]interface{}{"keys_per_request": float64(10), "max_concurrent_chunks": float64(2)},
		spy.option())
	keys := userKeys(50)
	e.seedView(t, testBigtableFeastProject+".a", "fvA", keys)
	e.seedView(t, testBigtableFeastProject+".b", "fvB", keys)
	got, err := e.read(keys, []string{"fvA", "fvB"}, []string{"f", "f"}, map[string][]string{"fvA": {"a"}, "fvB": {"b"}})
	require.NoError(t, err)
	require.Len(t, got, 50)
	assert.Equal(t, 10, spy.requestCount(), "5 chunks per table")
	assert.Equal(t, 2, spy.maxInFlight, "streams open at once")
}

// TestBigtableChunkSize: off, the size is always keys_per_request. On, keys_per_request is the floor and
// the size grows just enough to send the call's row keys in one round of max_concurrent_chunks, up to 100.
func TestBigtableChunkSize(t *testing.T) {
	tests := []struct {
		name                         string
		adaptive                     bool
		perRequest, maxChunks, nKeys int
		want                         int
	}{
		{"off, small read", false, 5, 50, 10, 5},
		{"off, large read", false, 5, 50, 500, 5},
		{"on, no keys", true, 5, 50, 0, 5},
		{"on, one key", true, 5, 50, 1, 5},
		{"on, 50 keys fit one round", true, 5, 50, 50, 5},
		{"on, 250 keys fill one round exactly", true, 5, 50, 250, 5},
		{"on, 251 keys grow to 6", true, 5, 50, 251, 6},
		{"on, 500 keys", true, 5, 50, 500, 10},
		{"on, 501 keys round up", true, 5, 50, 501, 11},
		{"on, 1500 keys", true, 5, 50, 1500, 30},
		{"on, capped at 100", true, 5, 50, 100000, 100},
		{"on, keys_per_request 100 always 100", true, 100, 50, 50, 100},
		{"on, keys_per_request 100 on a large read", true, 100, 50, 100000, 100},
		{"on, one stream", true, 5, 1, 30, 30},
		{"on, one stream capped at 100", true, 5, 1, 1000, 100},
		{"on, max_concurrent_chunks at its limit", true, 5, math.MaxInt32, 1000, 5},
		{"on, keys at the int32 limit", true, 5, math.MaxInt32, math.MaxInt32, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := bigtableConfig{AdaptiveChunking: tt.adaptive, KeysPerRequest: tt.perRequest, MaxConcurrentChunks: tt.maxChunks}
			assert.Equal(t, tt.want, bigtableChunkSize(cfg, tt.nKeys))
		})
	}
}

// TestBigtableReadAdaptiveChunking: with keys_per_request 5 and 8 streams, adaptive_chunking keeps
// 5-key chunks while a read fits one round (40 keys), and above that sends one round of larger chunks,
// up to 100 keys each. Every entity is filled.
func TestBigtableReadAdaptiveChunking(t *testing.T) {
	table := testBigtableFeastProject + ".a"
	byView := map[string][]string{"fvA": {"a"}}
	tests := []struct {
		name                                   string
		adaptive                               bool
		entities                               int
		wantStreams, wantMaxKeys, wantInFlight int
	}{
		{"off, 20 entities", false, 20, 4, 5, 4},
		{"on, 20 entities keep 5-key chunks", true, 20, 4, 5, 4},
		{"on, 1 entity", true, 1, 1, 5, 1},
		{"off, 50 entities take two rounds", false, 50, 10, 5, 8},
		{"on, 50 entities take one round of 7", true, 50, 8, 7, 8},
		{"on, 1000 entities capped at 100 keys", true, 1000, 10, 100, 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Hold every stream until wantInFlight have arrived, so the in-flight count shows the chunks run in
			// parallel without depending on timing. The 5 s bound only stops a hang when fewer arrive; the
			// maxInFlight assertion then fails.
			var arrived atomic.Int32
			release := make(chan struct{})
			var releaseOnce sync.Once
			spy := &readRowsSpy{hold: func(ctx context.Context, _ *btpb.ReadRowsRequest) {
				if int(arrived.Add(1)) >= tt.wantInFlight {
					releaseOnce.Do(func() { close(release) })
				}
				select {
				case <-release:
				case <-ctx.Done():
				case <-time.After(5 * time.Second):
				}
			}}
			e := newReadEnvWithConfig(t, map[string]interface{}{"keys_per_request": float64(5),
				"max_concurrent_chunks": float64(8), "adaptive_chunking": tt.adaptive}, spy.option())
			keys := userKeys(tt.entities)
			e.seedView(t, table, "fvA", keys)
			spy.reset()
			got, err := e.read(keys, []string{"fvA"}, []string{"f"}, byView)
			require.NoError(t, err)
			require.Len(t, got, tt.entities)
			i64 := e.cell("int64")
			for i, row := range got {
				want := &types.Value{Val: &types.Value_Int64Val{Int64Val: int64(i)}}
				assertSlot(t, &row[0], "fvA", "f", want, i64.seconds, i64.nanos)
			}
			streams := spy.keysPerStreamByTable()[table]
			assert.Len(t, streams, tt.wantStreams, "ReadRows streams")
			total := 0
			for _, c := range streams {
				assert.LessOrEqual(t, c, tt.wantMaxKeys)
				total += c
			}
			assert.Equal(t, tt.entities, total, "row keys sent")
			assert.Equal(t, tt.wantInFlight, spy.maxInFlight, "streams open at once")
		})
	}
}

// TestBigtableReadAdaptiveChunkingTwoTables: the chunk size comes from the call's total row keys and each
// table is cut separately, so a call over two tables can plan more ReadRows than max_concurrent_chunks;
// only the number in flight stays capped.
func TestBigtableReadAdaptiveChunkingTwoTables(t *testing.T) {
	tableA, tableB := testBigtableFeastProject+".a", testBigtableFeastProject+".b"
	byView := map[string][]string{"fvA": {"a"}, "fvB": {"b"}}
	tests := []struct {
		name                      string
		maxChunks, entities       int
		wantPerTable, wantMaxKeys int
	}{
		// 100 row keys over 8 streams: size 13, so each table's 50 keys are 4 chunks, 8 in all.
		{"balanced", 8, 50, 4, 13},
		// 20 row keys over 8 streams fit one round at the floor of 5: each table's 10 keys are 2 chunks.
		{"floor", 8, 10, 2, 5},
		// 60 row keys over 3 streams: size 20, so each table's 30 keys are 2 chunks, 4 in all.
		{"more chunks than streams", 3, 30, 2, 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spy := &readRowsSpy{}
			e := newReadEnvWithConfig(t, map[string]interface{}{"keys_per_request": float64(5),
				"max_concurrent_chunks": float64(tt.maxChunks), "adaptive_chunking": true}, spy.option())
			keys := userKeys(tt.entities)
			e.seedView(t, tableA, "fvA", keys)
			e.seedView(t, tableB, "fvB", keys)
			spy.reset()
			got, err := e.read(keys, []string{"fvA", "fvB"}, []string{"f", "f"}, byView)
			require.NoError(t, err)
			require.Len(t, got, tt.entities)
			i64 := e.cell("int64")
			for i, row := range got {
				want := &types.Value{Val: &types.Value_Int64Val{Int64Val: int64(i)}}
				assertSlot(t, &row[0], "fvA", "f", want, i64.seconds, i64.nanos)
				assertSlot(t, &row[1], "fvB", "f", want, i64.seconds, i64.nanos)
			}
			streams := spy.keysPerStreamByTable()
			for _, table := range []string{tableA, tableB} {
				assert.Len(t, streams[table], tt.wantPerTable, "ReadRows streams on %s", table)
				total := 0
				for _, c := range streams[table] {
					assert.LessOrEqual(t, c, tt.wantMaxKeys)
					total += c
				}
				assert.Equal(t, tt.entities, total, "row keys sent to %s", table)
			}
			assert.LessOrEqual(t, spy.maxInFlight, tt.maxChunks, "streams open at once")
		})
	}
}

// TestBigtableClientConfig: the app profile is passed through and everything else keeps the library's
// defaults.
func TestBigtableClientConfig(t *testing.T) {
	assert.Equal(t, bigtable.ClientConfig{AppProfile: "serve"}, bigtableClientConfig(bigtableConfig{AppProfile: "serve"}))
	assert.Equal(t, bigtable.ClientConfig{}, bigtableClientConfig(bigtableConfig{}))
}

// TestBigtableAdmitRead: with max_inflight_reads unset, a read is always admitted. With it set, a read
// that needs more slots than are free is rejected at once when max_inflight_wait_ms is 0; with a wait, it
// is admitted when slots free within the wait and rejected when they do not. A rejection is
// RESOURCE_EXHAUSTED; a read whose own context ends the wait gets the context error instead.
func TestBigtableAdmitRead(t *testing.T) {
	ctx := context.Background()
	release, err := (&BigtableOnlineStore{}).admitRead(ctx, 50)
	require.NoError(t, err)
	release()

	newStore := func(wait time.Duration) *BigtableOnlineStore {
		return &BigtableOnlineStore{inflight: semaphore.NewWeighted(2),
			cfg: bigtableConfig{MaxInflightReads: 2, MaxInflightWait: wait}}
	}
	assertRejected := func(t *testing.T, err error) {
		t.Helper()
		var rejected *bigtableReadRejectedError
		require.ErrorAs(t, err, &rejected)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		assert.ErrorContains(t, err, "2 of max_inflight_reads 2 not free")
	}

	t.Run("fail fast", func(t *testing.T) {
		s := newStore(0)
		held, err := s.admitRead(ctx, 1)
		require.NoError(t, err)
		_, err = s.admitRead(ctx, 2)
		assertRejected(t, err)
		held()
		release, err := s.admitRead(ctx, 2)
		require.NoError(t, err, "released slots are free again")
		release()
	})
	t.Run("admitted within the wait", func(t *testing.T) {
		s := newStore(5 * time.Second)
		held, err := s.admitRead(ctx, 2)
		require.NoError(t, err)
		time.AfterFunc(20*time.Millisecond, held)
		release, err := s.admitRead(ctx, 2)
		require.NoError(t, err)
		release()
	})
	t.Run("rejected after the wait", func(t *testing.T) {
		s := newStore(20 * time.Millisecond)
		held, err := s.admitRead(ctx, 1)
		require.NoError(t, err)
		defer held()
		start := time.Now()
		_, err = s.admitRead(ctx, 2)
		assertRejected(t, err)
		assert.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	})
	t.Run("caller's context ends the wait", func(t *testing.T) {
		s := newStore(5 * time.Second)
		held, err := s.admitRead(ctx, 2)
		require.NoError(t, err)
		defer held()
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = s.admitRead(cancelled, 1)
		require.ErrorIs(t, err, context.Canceled)
		assert.ErrorContains(t, err, "max_inflight_reads 2")
		assert.False(t, errors.As(err, new(*bigtableReadRejectedError)), "not a rejection")
	})
}

// TestBigtableReadMaxInflightReads: max_inflight_reads caps the ReadRows in flight across concurrent
// calls, which max_concurrent_chunks (per call) does not. 4 callers of 4 chunks each, with 8 streams per
// call and a long max_inflight_wait_ms, never have more than 3 streams open at once, and every call is
// filled.
func TestBigtableReadMaxInflightReads(t *testing.T) {
	const limit = 3
	// Hold the first streams until limit have arrived, so the in-flight count reaches the cap without
	// depending on timing; later streams pass at once. The 5 s bound only stops a hang.
	var arrived atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	spy := &readRowsSpy{hold: func(ctx context.Context, _ *btpb.ReadRowsRequest) {
		if int(arrived.Add(1)) >= limit {
			releaseOnce.Do(func() { close(release) })
		}
		select {
		case <-release:
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}}
	e := newReadEnvWithConfig(t, map[string]interface{}{"keys_per_request": float64(5),
		"max_concurrent_chunks": float64(8), "max_inflight_reads": float64(limit),
		"max_inflight_wait_ms": float64(5000)}, spy.option())
	keys := userKeys(20)
	e.seedView(t, subscriberTable, "fvA", keys)
	spy.reset()
	var wg sync.WaitGroup
	errs := make([]error, 4)
	results := make([][][]FeatureData, 4)
	for c := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[c], errs[c] = e.store.OnlineReadWithEntities(context.Background(), keys, []string{"fvA"},
				[]string{"f"}, map[string][]string{"fvA": {"subscriber"}})
		}()
	}
	wg.Wait()
	i64 := e.cell("int64")
	for c := range errs {
		require.NoError(t, errs[c], "caller %d", c)
		require.Len(t, results[c], 20)
		for i, row := range results[c] {
			want := &types.Value{Val: &types.Value_Int64Val{Int64Val: int64(i)}}
			assertSlot(t, &row[0], "fvA", "f", want, i64.seconds, i64.nanos)
		}
	}
	assert.Equal(t, 16, spy.requestCount(), "4 chunks per caller")
	assert.Equal(t, limit, spy.maxInFlight, "streams open at once across callers")
}

// TestBigtableReadMaxInflightReadsRejects: with max_inflight_reads 2 and no max_inflight_wait_ms, a read
// of 2 chunks while another read holds 1 slot is rejected at once as RESOURCE_EXHAUSTED, sends no ReadRows
// (whole reads are admitted, not single chunks) and is counted under status "rejected". A read whose
// deadline ends its wait for slots fails with context.DeadlineExceeded and also sends nothing.
func TestBigtableReadMaxInflightReadsRejects(t *testing.T) {
	for _, tt := range []struct {
		name   string
		waitMs float64
	}{
		{"fail fast", 0},
		{"deadline ends the wait", 10000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			arrived := make(chan struct{})
			unblock := make(chan struct{})
			var once sync.Once
			spy := &readRowsSpy{hold: func(ctx context.Context, _ *btpb.ReadRowsRequest) {
				once.Do(func() { close(arrived) })
				select {
				case <-unblock:
				case <-ctx.Done():
				case <-time.After(5 * time.Second):
				}
			}}
			e := newReadEnvWithConfig(t, map[string]interface{}{"keys_per_request": float64(5),
				"max_inflight_reads": float64(2), "max_inflight_wait_ms": tt.waitMs}, spy.option())
			keys := userKeys(10)
			e.seedView(t, subscriberTable, "fvA", keys)
			read := func(ctx context.Context, keys []*types.EntityKey) ([][]FeatureData, error) {
				return e.store.OnlineReadWithEntities(ctx, keys, []string{"fvA"}, []string{"f"},
					map[string][]string{"fvA": {"subscriber"}})
			}
			spy.reset()
			holderDone := make(chan error, 1)
			go func() {
				_, err := read(context.Background(), keys[:3])
				holderDone <- err
			}()
			<-arrived // the first read holds 1 of the 2 slots
			rejectedBefore := durationSamples(t, "rejected")
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			got, err := read(ctx, keys) // 2 chunks
			assert.Nil(t, got)
			if tt.waitMs == 0 {
				assert.Equal(t, codes.ResourceExhausted, status.Code(err))
				assert.ErrorContains(t, err, "2 of max_inflight_reads 2 not free within max_inflight_wait_ms 0")
				assert.Equal(t, rejectedBefore+1, durationSamples(t, "rejected"))
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				assert.ErrorContains(t, err, "max_inflight_reads 2")
				assert.Equal(t, rejectedBefore, durationSamples(t, "rejected"))
			}
			assert.Equal(t, 1, spy.requestCount(), "the second read sent no ReadRows")
			close(unblock)
			require.NoError(t, <-holderDone)
		})
	}
}

// TestBigtableClientConfigMetrics: disable_metrics sets the no-op metrics provider; left unset, the provider
// stays nil, which the library reads as its default Cloud Monitoring exporter.
func TestBigtableClientConfigMetrics(t *testing.T) {
	assert.Nil(t, bigtableClientConfig(bigtableConfig{}).MetricsProvider)
	assert.Equal(t, bigtable.NoopMetricsProvider{}, bigtableClientConfig(bigtableConfig{DisableMetrics: true}).MetricsProvider)
}

// TestBigtableMetricsMode: the startup log names why metrics are off, the config switch first.
func TestBigtableMetricsMode(t *testing.T) {
	t.Setenv("BIGTABLE_EMULATOR_HOST", "")
	assert.Equal(t, "default", bigtableMetricsMode(bigtableConfig{}))
	assert.Equal(t, "disabled (disable_metrics)", bigtableMetricsMode(bigtableConfig{DisableMetrics: true}))
	t.Setenv("BIGTABLE_EMULATOR_HOST", "localhost:1")
	assert.Equal(t, "disabled (emulator)", bigtableMetricsMode(bigtableConfig{}))
	assert.Equal(t, "disabled (disable_metrics)", bigtableMetricsMode(bigtableConfig{DisableMetrics: true}))
}

// TestBigtableReadFirstChunkErrorCancelsOthers: one of 5 chunks hits a corrupt cell while the other 4
// are held open by the server. The decode error must cancel them, so the call returns that error well
// before the caller's 2 s deadline.
func TestBigtableReadFirstChunkErrorCancelsOthers(t *testing.T) {
	spy := &readRowsSpy{}
	e := newReadEnvWithConfig(t, map[string]interface{}{"keys_per_request": float64(10), "max_concurrent_chunks": float64(5)},
		spy.option())
	keys := userKeys(50)
	e.seedView(t, subscriberTable, "fvA", keys)
	badKey := e.rowKey(t, keys[0], "fvA")
	seedRows(t, e.admin, e.client, subscriberTable,
		seedRow{Key: badKey, Cells: cells(e.cell("int64"), "f", []byte{0xff, 0xff}), Micros: 2000})
	spy.hold = func(ctx context.Context, req *btpb.ReadRowsRequest) {
		deadline := time.After(3 * time.Second)
		for {
			// The bad chunk waits for all 5 streams to reach the server; the others wait to be cancelled.
			if requestHasKey(req, badKey) && spy.requestCount() == 5 {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-deadline:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	got, err := e.store.OnlineReadWithEntities(ctx, keys, []string{"fvA"}, []string{"f"},
		map[string][]string{"fvA": {"subscriber"}})
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.Nil(t, got)
	assert.ErrorContains(t, err, "decode feast.types.Value", "the corrupt cell's error, not a cancellation")
	assert.NotErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 2*time.Second)
	assert.Equal(t, 5, spy.requestCount())
}

// TestBigtableReadContextCancelled: a context cancelled before the call, or while the server holds the
// ReadRows stream, fails the read with an error wrapping context.Canceled.
func TestBigtableReadContextCancelled(t *testing.T) {
	read := func(e *readEnv, ctx context.Context, keys []*types.EntityKey) ([][]FeatureData, error) {
		return e.store.OnlineReadWithEntities(ctx, keys, []string{"fvA"}, []string{"f"},
			map[string][]string{"fvA": {"subscriber"}})
	}
	t.Run("before the call", func(t *testing.T) {
		e := newReadEnv(t)
		keys := userKeys(3)
		e.seedView(t, subscriberTable, "fvA", keys)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		got, err := read(e, ctx, keys)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, got)
	})
	t.Run("mid-stream", func(t *testing.T) {
		arrived := make(chan struct{})
		var once sync.Once
		spy := &readRowsSpy{hold: func(ctx context.Context, _ *btpb.ReadRowsRequest) {
			once.Do(func() { close(arrived) })
			select { // released by the client's cancellation; the bound only stops a broken test hanging
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
		}}
		e := newReadEnvWithConfig(t, nil, spy.option())
		keys := userKeys(3)
		e.seedView(t, subscriberTable, "fvA", keys)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-arrived
			cancel()
		}()
		start := time.Now()
		got, err := read(e, ctx, keys)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, got)
		assert.Less(t, time.Since(start), 5*time.Second, "the cancellation, not the hold bound, ended the read")
		assert.Equal(t, 1, spy.requestCount())
	})
}

// TestBigtableReadTimeout: read_timeout_ms bounds the read, the caller's shorter deadline still wins,
// and a budget the read fits in changes nothing. The server holds every ReadRows stream for 50 ms.
func TestBigtableReadTimeout(t *testing.T) {
	tests := []struct {
		name           string
		readTimeoutMs  float64
		callerDeadline time.Duration // 0: none
		wantBudgetErr  bool          // the error names read_timeout_ms
		wantErr        bool
	}{
		{name: "budget expires", readTimeoutMs: 1, wantErr: true, wantBudgetErr: true},
		{name: "caller deadline is shorter", readTimeoutMs: 60000, callerDeadline: time.Millisecond, wantErr: true},
		{name: "read fits the budget", readTimeoutMs: 5000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spy := &readRowsSpy{hold: func(context.Context, *btpb.ReadRowsRequest) { time.Sleep(50 * time.Millisecond) }}
			e := newReadEnvWithConfig(t, map[string]interface{}{"read_timeout_ms": tt.readTimeoutMs}, spy.option())
			keys := userKeys(3)
			e.seedView(t, subscriberTable, "fvA", keys)
			ctx := context.Background()
			if tt.callerDeadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.callerDeadline)
				defer cancel()
			}
			got, err := e.store.OnlineReadWithEntities(ctx, keys, []string{"fvA"}, []string{"f"},
				map[string][]string{"fvA": {"subscriber"}})
			if !tt.wantErr {
				require.NoError(t, err)
				require.Len(t, got, 3)
				return
			}
			require.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Nil(t, got)
			if tt.wantBudgetErr {
				assert.ErrorContains(t, err, "read_timeout_ms")
			} else {
				assert.NotContains(t, err.Error(), "read_timeout_ms")
			}
		})
	}
}

// TestBigtableReadDefaultTimeout: with read_timeout_ms unset and no caller deadline, a read still has a
// deadline, the 60 s default, so the client's retries of DeadlineExceeded, Unavailable and Aborted end
// (cloud.google.com/go/bigtable retries them until the context is done).
func TestBigtableReadDefaultTimeout(t *testing.T) {
	read := func(e *readEnv, keys []*types.EntityKey) ([][]FeatureData, error) {
		return e.store.OnlineReadWithEntities(context.Background(), keys, []string{"fvA"}, []string{"f"},
			map[string][]string{"fvA": {"subscriber"}})
	}
	t.Run("the default budget reaches ReadRows", func(t *testing.T) {
		var mu sync.Mutex
		var timeLeft []time.Duration // per ReadRows stream; -1 when the stream has no deadline
		srvOpt := grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if info.FullMethod == readRowsMethod {
				left := time.Duration(-1)
				if deadline, ok := ss.Context().Deadline(); ok {
					left = time.Until(deadline)
				}
				mu.Lock()
				timeLeft = append(timeLeft, left)
				mu.Unlock()
			}
			return handler(srv, ss)
		})
		e := newReadEnvWithConfig(t, nil, srvOpt)
		keys := userKeys(3)
		e.seedView(t, subscriberTable, "fvA", keys)
		got, err := read(e, keys)
		require.NoError(t, err)
		require.Len(t, got, 3)
		mu.Lock()
		defer mu.Unlock()
		require.Len(t, timeLeft, 1)
		assert.Greater(t, timeLeft[0], 50*time.Second, "ReadRows carries the 60 s default deadline")
		assert.LessOrEqual(t, timeLeft[0], 60*time.Second)
	})
	t.Run("retries of Unavailable end at the budget", func(t *testing.T) {
		var attempts atomic.Int32
		srvOpt := grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if info.FullMethod != readRowsMethod {
				return handler(srv, ss)
			}
			attempts.Add(1)
			return status.Error(codes.Unavailable, "test: ReadRows is always unavailable")
		})
		// A short budget stands in for the 60 s default, so the test proves the mechanism quickly.
		e := newReadEnvWithConfig(t, map[string]interface{}{"read_timeout_ms": float64(300)}, srvOpt)
		keys := userKeys(3)
		e.seedView(t, subscriberTable, "fvA", keys)
		start := time.Now()
		got, err := read(e, keys)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.ErrorContains(t, err, "read_timeout_ms")
		assert.Nil(t, got)
		assert.Less(t, time.Since(start), 5*time.Second)
		assert.GreaterOrEqual(t, attempts.Load(), int32(2), "the client retried Unavailable until the budget ended it")
	})
}

// resultFingerprint renders a result as text: "nil" for a nil row, else each slot's reference, value
// bytes and timestamp.
func resultFingerprint(rows [][]FeatureData) string {
	var b strings.Builder
	for _, row := range rows {
		if row == nil {
			b.WriteString("nil\n")
			continue
		}
		for i := range row {
			v, err := proto.MarshalOptions{Deterministic: true}.Marshal(&row[i].Value)
			if err != nil {
				v = []byte(err.Error())
			}
			fmt.Fprintf(&b, "%s/%s=%x@%d.%d;", row[i].Reference.FeatureViewName, row[i].Reference.FeatureName,
				v, row[i].Timestamp.GetSeconds(), row[i].Timestamp.GetNanos())
		}
		b.WriteString("\n")
	}
	return b.String()
}

// assertConcurrentBaseline checks the single-threaded result of TestBigtableReadConcurrentCallers
// against what it seeded: fvA f1 = i for odd i, fvB b1 = 1000+i for i%3 == 0, a nil row when neither,
// and NullVal with a zero timestamp for the view an existing row lacks.
func assertConcurrentBaseline(t *testing.T, e *readEnv, got [][]FeatureData) {
	t.Helper()
	require.Len(t, got, 250)
	i64, str := e.cell("int64"), e.cell("string")
	for i, row := range got {
		hasA, hasB := i%2 == 1, i%3 == 0
		if !hasA && !hasB {
			require.Nil(t, row, "entity %d", i)
			continue
		}
		require.Len(t, row, 3, "entity %d", i)
		for _, slot := range []int{0, 2} {
			if hasA {
				assertSlot(t, &row[slot], "fvA", "f1", &types.Value{Val: &types.Value_Int64Val{Int64Val: int64(i)}}, i64.seconds, i64.nanos)
			} else {
				assertSlot(t, &row[slot], "fvA", "f1", nil, 0, 0)
			}
		}
		if hasB {
			assertSlot(t, &row[1], "fvB", "b1", &types.Value{Val: &types.Value_Int64Val{Int64Val: int64(1000 + i)}}, str.seconds, str.nanos)
		} else {
			assertSlot(t, &row[1], "fvB", "b1", nil, 0, 0)
		}
	}
}

// TestBigtableReadConcurrentCallers: 32 goroutines x 20 calls of 250 entities on one store, two views
// with partial coverage and one feature requested twice. Every result must equal the single-threaded
// one (run under -race). Rows are kept small because bttest compiles the column regex for every cell,
// which dominates the runtime.
func TestBigtableReadConcurrentCallers(t *testing.T) {
	if testing.Short() {
		t.Skip("32 x 20 reads of 250 entities against bttest")
	}
	e := newReadEnv(t)
	keys := userKeys(250)
	var rows []seedRow
	for i, k := range keys {
		if i%2 == 1 { // odd entities have an fvA row
			rows = append(rows, seedRow{Key: e.rowKey(t, k, "fvA"),
				Cells: cells(e.cell("int64"), "f1", int64Cell(t, int64(i))), Micros: 1000})
		}
		if i%3 == 0 { // every 3rd entity has an fvB row; even non-multiples of 3 have neither
			rows = append(rows, seedRow{Key: e.rowKey(t, k, "fvB"),
				Cells: cells(e.cell("string"), "b1", int64Cell(t, int64(1000+i))), Micros: 1000})
		}
	}
	seedRows(t, e.admin, e.client, subscriberTable, rows...)
	views, features := []string{"fvA", "fvB", "fvA"}, []string{"f1", "b1", "f1"}
	want, err := e.read(keys, views, features, nil)
	require.NoError(t, err)
	assertConcurrentBaseline(t, e, want)
	wantPrint := resultFingerprint(want)

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := 0; c < 20; c++ {
				got, err := e.read(keys, views, features, nil)
				if !assert.NoError(t, err) || !assert.Equal(t, wantPrint, resultFingerprint(got)) {
					return
				}
			}
		}()
	}
	wg.Wait()
}

// onlineReadSpanRecorder keeps only bigtable.OnlineRead spans of traces a span test registered, so spans
// from every other test in the process (the client's own, other reads) never pile up in it.
type onlineReadSpanRecorder struct {
	*tracetest.SpanRecorder
	traces sync.Map // trace.TraceID -> struct{}
}

func (r *onlineReadSpanRecorder) OnStart(context.Context, sdktrace.ReadWriteSpan) {}

func (r *onlineReadSpanRecorder) OnEnd(s sdktrace.ReadOnlySpan) {
	if _, ok := r.traces.Load(s.SpanContext().TraceID()); ok && s.Name() == "bigtable.OnlineRead" {
		r.SpanRecorder.OnEnd(s)
	}
}

var (
	spanRecorderOnce sync.Once
	spanRecorder     *onlineReadSpanRecorder
)

// installSpanRecorder installs one SDK tracer provider for the rest of the test process. It is installed
// once and never restored: the package tracer is an otel global delegate, which binds to the first
// provider ever set (global.SetTracerProvider uses a sync.Once), so a provider swapped in per test would
// only be seen by the first test of a -count=N run.
func installSpanRecorder() *onlineReadSpanRecorder {
	spanRecorderOnce.Do(func() {
		spanRecorder = &onlineReadSpanRecorder{SpanRecorder: tracetest.NewSpanRecorder()}
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder)))
	})
	return spanRecorder
}

// readSpan runs read under a fresh parent span and returns the bigtable.OnlineRead span it produced. The
// recorder is emptied before returning.
func readSpan(t *testing.T, read func(ctx context.Context)) sdktrace.ReadOnlySpan {
	t.Helper()
	recorder := installSpanRecorder()
	ctx, parent := otel.Tracer("bigtable-test").Start(context.Background(), "test-parent")
	traceID := parent.SpanContext().TraceID()
	recorder.traces.Store(traceID, struct{}{})
	read(ctx)
	parent.End()
	recorder.traces.Delete(traceID)
	ended := recorder.Ended()
	recorder.Reset()
	require.Len(t, ended, 1, "one bigtable.OnlineRead span in the trace")
	require.Equal(t, parent.SpanContext().SpanID(), ended[0].Parent().SpanID())
	return ended[0]
}

func spanAttributes(s sdktrace.ReadOnlySpan) map[string]attribute.Value {
	out := map[string]attribute.Value{}
	for _, kv := range s.Attributes() {
		out[string(kv.Key)] = kv.Value
	}
	return out
}

func TestBigtableReadSpanAttributes(t *testing.T) {
	e := newReadEnvWithConfig(t, map[string]interface{}{"keys_per_request": float64(4)})
	keys := userKeys(3)
	tableA, tableB := testBigtableFeastProject+".a", testBigtableFeastProject+".b"
	e.seedView(t, subscriberTable, "fvA", keys[:2])
	e.seedView(t, subscriberTable, "fvB", keys[:1])
	e.seedView(t, tableA, "fvA", keys)
	e.seedView(t, tableB, "fvB", keys[1:])
	tests := []struct {
		name      string
		byView    map[string][]string
		wantTable string
		wantRows  int64
		wantErr   bool
	}{
		// 3 entities x 2 views = 6 row keys in 2 chunks of at most 4; 3 rows exist.
		{name: "one table", byView: map[string][]string{"fvA": {"subscriber"}, "fvB": {"subscriber"}},
			wantTable: subscriberTable, wantRows: 3},
		// 3 row keys per table, 1 chunk each; 3 + 2 rows exist. Tables are comma-joined in request order.
		{name: "two tables", byView: map[string][]string{"fvA": {"a"}, "fvB": {"b"}},
			wantTable: tableA + "," + tableB, wantRows: 5},
		{name: "error", byView: map[string][]string{"fvA": {"a"}, "fvB": {"never_created"}},
			wantTable: tableA + "," + testBigtableFeastProject + ".never_created", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			span := readSpan(t, func(ctx context.Context) {
				_, err = e.store.OnlineReadWithEntities(ctx, keys, []string{"fvA", "fvB"}, []string{"f", "f"}, tt.byView)
			})
			attrs := spanAttributes(span)
			keysOf := make([]string, 0, len(attrs))
			for k := range attrs {
				keysOf = append(keysOf, k)
			}
			assert.ElementsMatch(t, []string{"bigtable.table", "feast.entity_count", "bigtable.row_keys",
				"bigtable.chunk_size", "bigtable.chunks", "bigtable.chunk_rounds", "bigtable.call_slot_wait_ms",
				"bigtable.inflight_wait_ms", "bigtable.rows_found"}, keysOf)
			assert.Equal(t, tt.wantTable, attrs["bigtable.table"].AsString())
			assert.Equal(t, int64(3), attrs["feast.entity_count"].AsInt64())
			assert.Equal(t, int64(6), attrs["bigtable.row_keys"].AsInt64())
			assert.Equal(t, int64(4), attrs["bigtable.chunk_size"].AsInt64())
			assert.Equal(t, int64(2), attrs["bigtable.chunks"].AsInt64())
			assert.Equal(t, int64(1), attrs["bigtable.chunk_rounds"].AsInt64(), "2 chunks fit 50 slots")
			assert.GreaterOrEqual(t, attrs["bigtable.call_slot_wait_ms"].AsFloat64(), 0.0)
			assert.GreaterOrEqual(t, attrs["bigtable.inflight_wait_ms"].AsFloat64(), 0.0)
			if !tt.wantErr {
				require.NoError(t, err)
				assert.Equal(t, tt.wantRows, attrs["bigtable.rows_found"].AsInt64())
				assert.Equal(t, otelcodes.Unset, span.Status().Code)
				return
			}
			require.Error(t, err)
			assert.Equal(t, otelcodes.Error, span.Status().Code)
			assert.Contains(t, span.Status().Description, "never_created")
			require.NotEmpty(t, span.Events())
			assert.Equal(t, "exception", span.Events()[0].Name, "RecordError adds an exception event")
		})
	}
}

// durationSamples is the number of observations of the duration histogram under status.
func durationSamples(t *testing.T, status string) uint64 {
	t.Helper()
	obs, err := bigtableReadDuration.GetMetricWithLabelValues(status)
	require.NoError(t, err)
	m := &dto.Metric{}
	require.NoError(t, obs.(prometheus.Metric).Write(m))
	return m.GetHistogram().GetSampleCount()
}

// TestBigtableReadMetrics: a call over two tables adds each table's sent chunks and found rows to that
// table's counters and one duration observation under its status; a failed call counts only the chunks
// it sent. The metrics are process-wide, so the test compares before and after.
func TestBigtableReadMetrics(t *testing.T) {
	e := newReadEnvWithConfig(t, map[string]interface{}{"keys_per_request": float64(2)})
	keys := userKeys(5)
	tableA, tableB := testBigtableFeastProject+".metrics_a", testBigtableFeastProject+".metrics_b"
	e.seedView(t, tableA, "fvA", keys[:3])
	e.seedView(t, tableB, "fvB", keys[:1])
	byView := map[string][]string{"fvB": {"metrics_b"}, "fvA": {"metrics_a"}}
	counter := func(c *prometheus.CounterVec, table string) float64 {
		return testutil.ToFloat64(c.WithLabelValues(table))
	}
	chunksA, chunksB := counter(bigtableReadChunks, tableA), counter(bigtableReadChunks, tableB)
	rowsA, rowsB := counter(bigtableReadRowsFound, tableA), counter(bigtableReadRowsFound, tableB)
	okBefore := durationSamples(t, "ok")
	_, err := e.read(keys, []string{"fvB", "fvA"}, []string{"f", "f"}, byView)
	require.NoError(t, err)
	assert.Equal(t, 3.0, counter(bigtableReadChunks, tableA)-chunksA, "5 keys in chunks of 2")
	assert.Equal(t, 3.0, counter(bigtableReadChunks, tableB)-chunksB)
	assert.Equal(t, 3.0, counter(bigtableReadRowsFound, tableA)-rowsA)
	assert.Equal(t, 1.0, counter(bigtableReadRowsFound, tableB)-rowsB)
	assert.Equal(t, okBefore+1, durationSamples(t, "ok"))

	missing := testBigtableFeastProject + ".never_created_metrics"
	sentBefore := counter(bigtableReadChunks, missing)
	errBefore := durationSamples(t, "error")
	_, err = e.read(keys, []string{"fvA"}, []string{"f"}, map[string][]string{"fvA": {"never_created_metrics"}})
	require.Error(t, err)
	assert.Equal(t, errBefore+1, durationSamples(t, "error"))
	sent := counter(bigtableReadChunks, missing) - sentBefore
	assert.GreaterOrEqual(t, sent, 1.0, "the failing chunk was sent")
	assert.LessOrEqual(t, sent, 3.0, "no more than the 3 planned")
}
