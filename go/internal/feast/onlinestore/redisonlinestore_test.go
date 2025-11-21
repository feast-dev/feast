//go:build !integration

package onlinestore

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/utils"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewRedisOnlineStore(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.Equal(t, opts.Password, "")
	assert.Equal(t, opts.DB, 0)
	assert.Nil(t, opts.TLSConfig)
}

func TestNewRedisOnlineStoreWithPassword(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379,password=secret",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.Equal(t, opts.Password, "secret")
}

func TestNewRedisOnlineStoreWithDB(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379,db=1",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.Equal(t, opts.DB, 1)
}

func TestNewRedisOnlineStoreWithSsl(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379,ssl=true",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.NotNil(t, opts.TLSConfig)
}

func TestBuildFeatureViewIndices(t *testing.T) {
	r := &RedisOnlineStore{}

	t.Run("test with empty featureViewNames and featureNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{}, []string{})
		assert.Equal(t, 0, len(featureViewIndices))
		assert.Equal(t, 0, len(indicesFeatureView))
		assert.Equal(t, 0, index)
	})

	t.Run("test with non-empty featureNames and empty featureViewNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{}, []string{"feature1", "feature2"})
		assert.Equal(t, 0, len(featureViewIndices))
		assert.Equal(t, 0, len(indicesFeatureView))
		assert.Equal(t, 2, index)
	})

	t.Run("test with non-empty featureViewNames and featureNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{"view1", "view2"}, []string{"feature1", "feature2"})
		assert.Equal(t, 2, len(featureViewIndices))
		assert.Equal(t, 2, len(indicesFeatureView))
		assert.Equal(t, 4, index)
		assert.Equal(t, "view1", indicesFeatureView[2])
		assert.Equal(t, "view2", indicesFeatureView[3])
	})

	t.Run("test with duplicate featureViewNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{"view1", "view1"}, []string{"feature1", "feature2"})
		assert.Equal(t, 1, len(featureViewIndices))
		assert.Equal(t, 1, len(indicesFeatureView))
		assert.Equal(t, 3, index)
		assert.Equal(t, "view1", indicesFeatureView[2])
	})
}

func TestBuildHsetKeys(t *testing.T) {
	r := &RedisOnlineStore{}

	t.Run("test with empty featureViewNames and featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildRedisHashSetKeys([]string{}, []string{}, map[int]string{}, 0)
		assert.Equal(t, 0, len(hsetKeys))
		assert.Equal(t, 0, len(featureNames))
	})

	t.Run("test with non-empty featureViewNames and featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildRedisHashSetKeys([]string{"view1", "view2"}, []string{"feature1", "feature2"}, map[int]string{2: "view1", 3: "view2"}, 4)
		assert.Equal(t, 4, len(hsetKeys))
		assert.Equal(t, 4, len(featureNames))
		assert.Equal(t, "_ts:view1", hsetKeys[2])
		assert.Equal(t, "_ts:view2", hsetKeys[3])
		assert.Contains(t, featureNames, "_ts:view1")
		assert.Contains(t, featureNames, "_ts:view2")
	})

	t.Run("test with more featureViewNames than featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildRedisHashSetKeys([]string{"view1", "view2", "view3"}, []string{"feature1", "feature2", "feature3"}, map[int]string{3: "view1", 4: "view2", 5: "view3"}, 6)
		assert.Equal(t, 6, len(hsetKeys))
		assert.Equal(t, 6, len(featureNames))
		assert.Equal(t, "_ts:view1", hsetKeys[3])
		assert.Equal(t, "_ts:view2", hsetKeys[4])
		assert.Equal(t, "_ts:view3", hsetKeys[5])
		assert.Contains(t, featureNames, "_ts:view1")
		assert.Contains(t, featureNames, "_ts:view2")
		assert.Contains(t, featureNames, "_ts:view3")
	})
}

func TestBuildRedisKeys(t *testing.T) {
	r := &RedisOnlineStore{
		project: "test_project",
		config: &registry.RepoConfig{
			EntityKeySerializationVersion: 3,
		},
	}

	entity_key1 := types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1005}}},
	}

	entity_key2 := types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}}},
	}

	error_entity_key1 := types.EntityKey{
		JoinKeys:     []string{"driver_id", "vehicle_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1005}}},
	}

	t.Run("test with empty entityKeys", func(t *testing.T) {
		redisKeys, redisKeyToEntityIndex, err := r.buildRedisKeys([]*types.EntityKey{})
		assert.Nil(t, err)
		assert.Equal(t, 0, len(redisKeys))
		assert.Equal(t, 0, len(redisKeyToEntityIndex))
	})

	t.Run("test with single entityKey", func(t *testing.T) {
		entityKeys := []*types.EntityKey{&entity_key1}
		redisKeys, redisKeyToEntityIndex, err := r.buildRedisKeys(entityKeys)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(redisKeys))
		assert.Equal(t, 1, len(redisKeyToEntityIndex))
	})

	t.Run("test with multiple entityKeys", func(t *testing.T) {
		entityKeys := []*types.EntityKey{
			&entity_key1, &entity_key2,
		}
		redisKeys, redisKeyToEntityIndex, err := r.buildRedisKeys(entityKeys)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(redisKeys))
		assert.Equal(t, 2, len(redisKeyToEntityIndex))
	})

	t.Run("test with error in buildRedisKey", func(t *testing.T) {
		entityKeys := []*types.EntityKey{&error_entity_key1}
		_, _, err := r.buildRedisKeys(entityKeys)
		assert.NotNil(t, err)
	})
}

func newMiniRedisStore(t *testing.T) (*RedisOnlineStore, *miniredis.Miniredis) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := &RedisOnlineStore{
		project: "test_project",
		t:       redisNode,
		client:  client,
		config: &registry.RepoConfig{
			EntityKeySerializationVersion: 3,
		},
	}
	return store, mr
}

func Test_batchHMGET(t *testing.T) {
	ctx := context.Background()
	store, _ := newMiniRedisStore(t)

	entityKeyBin := []byte{0xAA, 0xBB}

	// members = simulated sort keys
	members := [][]byte{{0x01}, {0x02}}

	grp := &fvGroup{
		view:          "fv",
		featNames:     []string{"f1"},
		fieldHashes:   []string{utils.Mmh3FieldHash("fv", "f1")},
		tsKey:         "_ts:fv",
		columnIndexes: []int{0},
	}

	results := make([][]RangeFeatureData, 1)
	results[0] = []RangeFeatureData{
		{
			FeatureView:     "fv",
			FeatureName:     "f1",
			Values:          []interface{}{},
			Statuses:        []serving.FieldStatus{},
			EventTimestamps: []timestamppb.Timestamp{},
		},
	}

	// test hashes matching pattern HASH = entityKeyBytes+sortKeyBytes
	for _, sk := range members {
		hk := utils.BuildHashKey(entityKeyBin, sk)
		ts := timestamppb.Now()
		tsB, _ := proto.Marshal(ts)
		vB, _ := proto.Marshal(&types.Value{
			Val: &types.Value_StringVal{StringVal: "val-" + string(sk)},
		})
		// fields: tsKey + field hash must match what batchHMGET asks for
		require.NoError(t, store.client.HSet(
			ctx,
			hk,
			grp.tsKey, tsB,
			grp.fieldHashes[0], vB,
		).Err())
	}

	err := batchHMGET(
		ctx,
		store.client,
		entityKeyBin,
		members,
		[]string{grp.fieldHashes[0], grp.tsKey},
		"fv",
		grp,
		results,
		0,
	)
	require.NoError(t, err)

	require.Len(t, results[0][0].Values, 2)
	assert.Equal(t, "val-\x01", results[0][0].Values[0].(*types.Value).GetStringVal())
	assert.Equal(t, "val-\x02", results[0][0].Values[1].(*types.Value).GetStringVal())
}

func writeTestRecord(
	t *testing.T,
	store *RedisOnlineStore,
	entityBin []byte,
	fv string,
	sortKeyBytes []byte,
	ts *timestamppb.Timestamp,
	feat string,
	val string,
) {
	ctx := context.Background()

	// ZSET key = fv + entity_key_bytes
	zkey := utils.BuildZsetKey(fv, entityBin)

	// HASH key = entity_key_bytes + sort_key_bytes
	hkey := utils.BuildHashKey(entityBin, sortKeyBytes)

	tsKey := "_ts:" + fv
	fieldHash := utils.Mmh3FieldHash(fv, feat) // use new mmh3

	tsB, _ := proto.Marshal(ts)
	valB, _ := proto.Marshal(&types.Value{
		Val: &types.Value_StringVal{StringVal: val},
	})

	require.NoError(t, store.client.HSet(
		ctx,
		hkey,
		tsKey, tsB,
		fieldHash, valB,
	).Err())
	require.NoError(t, store.client.ZAdd(ctx, zkey, redis.Z{
		Score:  float64(ts.AsTime().Unix()),
		Member: string(sortKeyBytes),
	}).Err())
}

func TestOnlineReadRange(t *testing.T) {
	ctx := context.Background()
	store, _ := newMiniRedisStore(t)

	entityKey := &types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "D1"}}},
	}

	entityKeyBin, err := SerializeEntityKeyWithProject("test_project", entityKey, 3)
	require.NoError(t, err)

	fv := "driver_fv"
	feat := "trip_count"

	// three records with increasing timestamps
	ts1 := timestamppb.New(time.Unix(1000, 0))
	ts2 := timestamppb.New(time.Unix(2000, 0))
	ts3 := timestamppb.New(time.Unix(3000, 0))

	writeTestRecord(t, store, entityKeyBin, fv, []byte{0x01}, ts1, feat, "one")
	writeTestRecord(t, store, entityKeyBin, fv, []byte{0x02}, ts2, feat, "two")
	writeTestRecord(t, store, entityKeyBin, fv, []byte{0x03}, ts3, feat, "three")

	groupedRefs := &model.GroupedRangeFeatureRefs{
		EntityKeys:         []*types.EntityKey{entityKey},
		FeatureNames:       []string{feat},
		FeatureViewNames:   []string{fv},
		Limit:              10,
		IsReverseSortOrder: false,
		SortKeyFilters: []*model.SortKeyFilter{
			{
				SortKeyName:    "ts",
				RangeStart:     int64(1000),
				RangeEnd:       int64(2500),
				StartInclusive: true,
				EndInclusive:   true,
			},
		},
	}

	res, err := store.OnlineReadRange(ctx, groupedRefs)
	require.NoError(t, err)

	require.Len(t, res, 1)
	require.Len(t, res[0], 1)

	out := res[0][0]
	require.Len(t, out.Values, 2)
	assert.Equal(t, "one", out.Values[0].(*types.Value).GetStringVal())
	assert.Equal(t, "two", out.Values[1].(*types.Value).GetStringVal())
}

func TestOnlineReadRange_Reverse(t *testing.T) {
	ctx := context.Background()
	store, _ := newMiniRedisStore(t)

	entityKey := &types.EntityKey{
		JoinKeys:     []string{"user"},
		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "U1"}}},
	}

	entityKeyBin, _ := SerializeEntityKeyWithProject("test_project", entityKey, 3)

	fv := "rev_fv"
	feat := "rev"
	ts1 := timestamppb.New(time.Unix(10, 0))
	ts2 := timestamppb.New(time.Unix(20, 0))

	writeTestRecord(t, store, entityKeyBin, fv, []byte{0x01}, ts1, feat, "A")
	writeTestRecord(t, store, entityKeyBin, fv, []byte{0x02}, ts2, feat, "B")

	groupedRefs := &model.GroupedRangeFeatureRefs{
		EntityKeys:         []*types.EntityKey{entityKey},
		FeatureNames:       []string{feat},
		FeatureViewNames:   []string{fv},
		Limit:              10,
		IsReverseSortOrder: true,
		SortKeyFilters: []*model.SortKeyFilter{
			{SortKeyName: "ts", RangeStart: int64(0), RangeEnd: int64(30)},
		},
	}

	res, err := store.OnlineReadRange(ctx, groupedRefs)
	require.NoError(t, err)

	out := res[0][0]
	require.Len(t, out.Values, 2)
	assert.Equal(t, "B", out.Values[0].(*types.Value).GetStringVal())
	assert.Equal(t, "A", out.Values[1].(*types.Value).GetStringVal())
}

func TestOnlineReadRange_EmptyResult(t *testing.T) {
	ctx := context.Background()
	store, _ := newMiniRedisStore(t)

	entityKey := &types.EntityKey{
		JoinKeys:     []string{"missing"},
		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "none"}}},
	}

	groupedRefs := &model.GroupedRangeFeatureRefs{
		EntityKeys:         []*types.EntityKey{entityKey},
		FeatureNames:       []string{"feat"},
		FeatureViewNames:   []string{"fv"},
		Limit:              10,
		IsReverseSortOrder: false,
		SortKeyFilters: []*model.SortKeyFilter{
			{SortKeyName: "ts", RangeStart: int64(0), RangeEnd: int64(10)},
		},
	}

	res, err := store.OnlineReadRange(ctx, groupedRefs)
	require.NoError(t, err)

	assert.Equal(t, serving.FieldStatus_NOT_FOUND, res[0][0].Statuses[0])
}
