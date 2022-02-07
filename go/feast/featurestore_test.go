package feast

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewFeatureStore(t *testing.T) {
	config := RepoConfig{
		Project:  "feature_repo_redis",
		Registry: "data/registry.db",
		Provider: "local",
		OnlineStore: map[string]interface{}{
			"type": "redis",
		},
	}
	fs, err := NewFeatureStore(&config)
	assert.Nil(t, err)
	assert.IsType(t, &RedisOnlineStore{}, fs.onlineStore)
}
