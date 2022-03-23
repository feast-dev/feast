package feast

import (
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqliteSetup(t *testing.T) {
	dir := "../test/selected_tetra"
	config, err := NewRepoConfigFromFile(dir)
	assert.Nil(t, err)
	assert.Equal(t, "selected_tetra", config.Project)
	assert.Equal(t, "data/registry.db", config.GetRegistryConfig().Path)
	assert.Equal(t, "local", config.Provider)
	assert.Equal(t, map[string]interface{}{
		"path": "data/online_store.db",
	}, config.OnlineStore)
	assert.Empty(t, config.OfflineStore)
	assert.Empty(t, config.FeatureServer)
	assert.Empty(t, config.Flags)
}

func TestSqliteUpdate(t *testing.T) {
	dir := "../test/selected_tetra"
	config, err := NewRepoConfigFromFile(dir)
	assert.Nil(t, err)
	store, err := NewSqliteOnlineStore("test", config.OnlineStore)
	defer store.Destruct()
	assert.Nil(t, err)
	show_tables := `SELECT
    	*
	FROM
		selected_tetra_driver_hourly_stats;`
	rows, err := store.db.Query(show_tables)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		fmt.Println("in")
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, name)
	}
	assert.False(t, true)

}
