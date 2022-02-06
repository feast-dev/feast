package feast

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewRedisOnlineStore1(t *testing.T) {
	onlineStoreConfig := map[string]interface{}{
		"type": "redis",
	}
	r, err := NewRedisOnlineStore(onlineStoreConfig)
	assert.Nil(t, err)
	assert.Equal(t, []string{"localhost:6379"}, r.addrs)
	assert.Empty(t, r.password)
	assert.False(t, r.ssl)
	assert.Equal(t, redisNode, r.t)
}

func TestNewRedisOnlineStore2(t *testing.T) {
	onlineStoreConfig := map[string]interface{}{
		"type":       "redis",
		"redis_type": "redis",
	}
	r, err := NewRedisOnlineStore(onlineStoreConfig)
	assert.Nil(t, err)
	assert.Equal(t, []string{"localhost:6379"}, r.addrs)
	assert.Empty(t, r.password)
	assert.False(t, r.ssl)
	assert.Equal(t, redisNode, r.t)
}

func TestNewRedisOnlineStore3(t *testing.T) {
	onlineStoreConfig := map[string]interface{}{
		"type":       "redis",
		"redis_type": "redis_cluster",
	}
	r, err := NewRedisOnlineStore(onlineStoreConfig)
	assert.Nil(t, err)
	assert.Equal(t, []string{"localhost:6379"}, r.addrs)
	assert.Empty(t, r.password)
	assert.False(t, r.ssl)
	assert.Equal(t, redisCluster, r.t)
}

func TestNewRedisOnlineStore4(t *testing.T) {
	onlineStoreConfig := map[string]interface{}{
		"type":              "redis_cluster",
		"connection_string": "localhost:6379,localhost:6380,password=123456,ssl=true",
	}
	r, err := NewRedisOnlineStore(onlineStoreConfig)
	assert.Nil(t, err)
	assert.Equal(t, []string{"localhost:6379", "localhost:6380"}, r.addrs)
	assert.Equal(t, "123456", r.password)
	assert.True(t, r.ssl)
	assert.Equal(t, redisNode, r.t)
}

func TestNewRedisOnlineStore5(t *testing.T) {
	onlineStoreConfig := map[string]interface{}{
		"type":              "redis_cluster",
		"connection_string": "localhost:6379,foo=bar",
	}
	_, err := NewRedisOnlineStore(onlineStoreConfig)
	assert.NotNil(t, err)
}

func TestNewRedisOnlineStore6(t *testing.T) {
	onlineStoreConfig := map[string]interface{}{
		"type":              "redis_cluster",
		"connection_string": "localhost:6379,test",
	}
	_, err := NewRedisOnlineStore(onlineStoreConfig)
	assert.NotNil(t, err)
}
