package feast

import (
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

//func TestNewRedisOnlineStore1(t *testing.T) {
//	onlineStoreConfig := map[string]interface{}{
//		"type": "redis",
//	}
//	r, err := NewRedisOnlineStore("feature_repo", onlineStoreConfig)
//	assert.Nil(t, err)
//	assert.Equal(t, &RedisOnlineStore{
//		t:        redisNode,
//		addrs:    []string{"localhost:6379"},
//		password: "",
//		ssl:      false,
//		project:  "feature_repo",
//	}, r)
//}
//
//func TestNewRedisOnlineStore2(t *testing.T) {
//	onlineStoreConfig := map[string]interface{}{
//		"type":       "redis",
//		"redis_type": "redis",
//	}
//	r, err := NewRedisOnlineStore("feature_repo", onlineStoreConfig)
//	assert.Nil(t, err)
//	assert.Equal(t, &RedisOnlineStore{
//		t:        redisNode,
//		addrs:    []string{"localhost:6379"},
//		password: "",
//		ssl:      false,
//		project:  "feature_repo",
//	}, r)
//}
//
//func TestNewRedisOnlineStore3(t *testing.T) {
//	onlineStoreConfig := map[string]interface{}{
//		"type":       "redis",
//		"redis_type": "redis_cluster",
//	}
//	r, err := NewRedisOnlineStore("feature_repo", onlineStoreConfig)
//	assert.Nil(t, err)
//	assert.Equal(t, &RedisOnlineStore{
//		t:        redisCluster,
//		addrs:    []string{"localhost:6379"},
//		password: "",
//		ssl:      false,
//		project:  "feature_repo",
//	}, r)
//}
//
//func TestNewRedisOnlineStore4(t *testing.T) {
//	onlineStoreConfig := map[string]interface{}{
//		"type":              "redis_cluster",
//		"connection_string": "localhost:6379,localhost:6380,password=123456,ssl=true",
//	}
//	r, err := NewRedisOnlineStore("feature_repo", onlineStoreConfig)
//	assert.Nil(t, err)
//	assert.Equal(t, &RedisOnlineStore{
//		t:        redisNode,
//		addrs:    []string{"localhost:6379", "localhost:6380"},
//		password: "123456",
//		ssl:      true,
//		project:  "feature_repo",
//	}, r)
//}

func TestNewRedisOnlineStore5(t *testing.T) {
	onlineStoreConfig := map[string]interface{}{
		"type":              "redis_cluster",
		"connection_string": "localhost:6379,foo=bar",
	}
	_, err := NewRedisOnlineStore("feature_repo", onlineStoreConfig)
	assert.NotNil(t, err)
}

func TestNewRedisOnlineStore6(t *testing.T) {
	onlineStoreConfig := map[string]interface{}{
		"type":              "redis_cluster",
		"connection_string": "localhost:6379,test",
	}
	_, err := NewRedisOnlineStore("feature_repo", onlineStoreConfig)
	assert.NotNil(t, err)
}

func TestRedisOnlineStoreRead(t *testing.T) {
	t.Skip("Skipping this test until it's fixed")
	onlineStoreConfig := map[string]interface{}{
		"type":              "redis",
		"connection_string": "localhost:6379",
	}
	// TODO (woop): Add setup/teardown
	// TODO (woop): Remove hardcoded values
	r, err := NewRedisOnlineStore("test_repo", onlineStoreConfig)

	assert.Nil(t, err)

	keys := []types.EntityKey{
		{
			JoinKeys: []string{"driver_id", "driver_id"},
			EntityValues: []*types.Value{
				{
					Val: &types.Value_Int64Val{Int64Val: 1001},
				},
				{
					Val: &types.Value_Int64Val{Int64Val: 1004},
				},
			},
		},
	}
	res, err := r.OnlineRead(keys, "driver_hourly_stats", []string{"avg_daily_trips", "conv_rate"})

	if err != nil {
		t.Fatalf(`could not read from online store for project %v with client config %v`, r.project, r.client.String())
	}

	assert.NotNil(t, res)

	// TODO: Get the right response values
	assert.Equal(t,
		[][]int64{{-1, 1}, {2, 2}},
		res,
	)
}
