package feast

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewFeatureStore(t *testing.T) {
	config := map[string]interface{}{
		"project":  "feature_repo_redis",
		"registry": "data/registry.db",
		"provider": "local",
		"online_store": map[string]interface{}{
			"type": "redis",
		},
	}
	fs, err := NewFeatureStore(config)
	assert.Nil(t, err)
	assert.IsType(t, &RedisOnlineStore{}, fs.onlineStore)
}
