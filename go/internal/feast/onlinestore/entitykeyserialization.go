package onlinestore

import (
	"sort"
	"strings"
	"sync"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/rs/zerolog/log"
)

// resolveEntityKeySerializationVersion returns the entity key serialization version that an
// online store should use for reads. It mirrors utils.SerializeEntityKey (and Python's
// default): a 0/unset version resolves to v3. It is nil-safe so it can be called on stores
// constructed without a config (e.g. in tests).
func resolveEntityKeySerializationVersion(config *registry.RepoConfig) int64 {
	if config != nil && config.EntityKeySerializationVersion != 0 {
		return config.EntityKeySerializationVersion
	}
	return 3
}

// warnPotentialEntityKeyVersionMismatch emits a single, de-duplicated warning when an online
// read returned no data for every requested feature view. A complete miss across the whole
// request is a *potential* (not guaranteed) symptom of an entity_key_serialization_version
// mismatch between the write (materialization) and read (feature server) paths. It can also
// legitimately mean the requested entities simply are not present (e.g. not yet materialized
// or expired), so the message is intentionally non-alarming.
//
// De-duplication is keyed on the sorted set of views (tracked in warned) so each distinct
// request shape warns at most once per process lifetime. storeName is used only to make the
// message specific (e.g. "Cassandra", "Valkey"). configuredVersion is the effective read
// version, surfaced in the message to help operators compare against materialization.
func warnPotentialEntityKeyVersionMismatch(warned *sync.Map, storeName string, configuredVersion int64, views []string, numKeys int) {
	if len(views) == 0 {
		return
	}
	sortedViews := append([]string(nil), views...)
	sort.Strings(sortedViews)
	dedupKey := strings.Join(sortedViews, ",")
	if _, alreadyWarned := warned.LoadOrStore(dedupKey, struct{}{}); alreadyWarned {
		return
	}
	log.Warn().Msgf(
		"%s online read returned no data for all %d entity key(s) across every requested "+
			"feature view %v. This is not necessarily a problem — it can simply mean the requested "+
			"entities are not present (e.g. not yet materialized or expired). However, if data was "+
			"expected, it may indicate an entity_key_serialization_version mismatch between "+
			"materialization (write) and the feature server (read). If so, verify that "+
			"entity_key_serialization_version in feature_store.yaml (current configured read version: "+
			"%d) matches the version used to materialize these feature views.",
		storeName, numKeys, sortedViews, configuredVersion)
}
