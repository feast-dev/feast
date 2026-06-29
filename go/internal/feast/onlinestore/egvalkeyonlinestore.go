package onlinestore

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/utils"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/feast-dev/feast/go/internal/feast/registry"

	"github.com/spaolacci/murmur3"
	"github.com/valkey-io/valkey-go"

	valkeytrace "github.com/DataDog/dd-trace-go/contrib/valkey-io/valkey-go/v2"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/rs/zerolog/log"
)

const defaultConnectionString = "localhost:6379"

type valkeyType int

const (
	valkeyNode    valkeyType = 0
	valkeyCluster valkeyType = 1
)

type ValkeyOnlineStore struct {

	// Feast project name
	project string

	// Valkey database type, either a single node server (ValkeyType.Valkey) or a cluster (ValkeyType.ValkeyCluster)
	t valkeyType

	// Valkey client connector
	client valkey.Client

	config *registry.RepoConfig

	// Number of keys to read in a batch
	ReadBatchSize int

	// Tracks request shapes for which a potential serialization-version-mismatch warning has
	// already been emitted, so the warning fires at most once per shape per process lifetime.
	versionMismatchWarned sync.Map
}

func parseConnectionString(onlineStoreConfig map[string]interface{}, valkeyStoreType valkeyType) (valkey.ClientOption, error) {
	var clientOption valkey.ClientOption

	clientOption.SendToReplicas = func(cmd valkey.Completed) bool {
		return cmd.IsReadOnly()
	}

	if valkeyStoreType == valkeyNode {
		replicaAddressJsonValue, ok := onlineStoreConfig["replica_address"]
		if !ok {
			log.Warn().Msg("define replica_address or reader endpoint to read from cluster replicas")
		} else {
			replicaAddress, ok := replicaAddressJsonValue.(string)
			if !ok {
				return clientOption, fmt.Errorf("failed to convert replica_address to string: %+v", replicaAddressJsonValue)
			}

			parts := strings.Split(replicaAddress, ",")
			for _, part := range parts {
				if strings.Contains(part, ":") {
					clientOption.Standalone.ReplicaAddress = append(clientOption.Standalone.ReplicaAddress, part)
				} else {
					return clientOption, fmt.Errorf("unable to parse part of replica_address: %s", part)
				}
			}
		}
	}

	valkeyConnJsonValue, ok := onlineStoreConfig["connection_string"]
	if !ok {
		valkeyConnJsonValue = defaultConnectionString
	}

	valkeyConnStr, ok := valkeyConnJsonValue.(string)
	if !ok {
		return clientOption, fmt.Errorf("failed to convert connection_string to string: %+v", valkeyConnJsonValue)
	}

	parts := strings.Split(valkeyConnStr, ",")
	for _, part := range parts {
		if strings.Contains(part, ":") {
			clientOption.InitAddress = append(clientOption.InitAddress, part)
		} else if strings.Contains(part, "=") {
			kv := strings.SplitN(part, "=", 2)
			switch kv[0] {
			case "password":
				clientOption.Password = kv[1]
			case "ssl":
				result, err := strconv.ParseBool(kv[1])
				if err != nil {
					return clientOption, err
				}
				if result {
					clientOption.TLSConfig = &tls.Config{}
				}
			case "db":
				db, err := strconv.Atoi(kv[1])
				if err != nil {
					return clientOption, err
				}
				clientOption.SelectDB = db
			default:
				return clientOption, fmt.Errorf("unrecognized option in connection_string: %s", kv[0])
			}
		} else {
			return clientOption, fmt.Errorf("unable to parse part of connection_string: %s", part)
		}
	}
	return clientOption, nil
}

func getValkeyTraceServiceName() string {
	datadogServiceName := os.Getenv("DD_SERVICE")
	var valkeyTraceServiceName string
	if datadogServiceName != "" {
		valkeyTraceServiceName = datadogServiceName + "-valkey"
	} else {
		valkeyTraceServiceName = "valkey.client" // Default service name
	}
	return valkeyTraceServiceName
}

func initializeValkeyClient(clientOption valkey.ClientOption, serviceName string) (valkey.Client, error) {
	if strings.ToLower(os.Getenv("ENABLE_ONLINE_STORE_TRACING")) == "true" {
		return valkeytrace.NewClient(clientOption, valkeytrace.WithService(serviceName))
	}

	return valkey.NewClient(clientOption)
}

func NewValkeyOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*ValkeyOnlineStore, error) {
	store := ValkeyOnlineStore{
		project: project,
		config:  config,
	}

	// Parse Valkey type
	valkeyStoreType, err := getValkeyType(onlineStoreConfig)
	if err != nil {
		return nil, err
	}
	store.t = valkeyStoreType

	// Parse connection string
	clientOption, err := parseConnectionString(onlineStoreConfig, valkeyStoreType)
	if err != nil {
		return nil, err
	}

	// Initialize Valkey client
	store.client, err = initializeValkeyClient(clientOption, getValkeyTraceServiceName())
	if err != nil {
		return nil, err
	}

	// Parse read batch size
	var readBatchSize float64
	if readBatchSizeJsonValue, ok := onlineStoreConfig["read_batch_size"]; !ok {
		readBatchSize = 100.0 // Default to 100 Keys Per Batch
	} else if readBatchSize, ok = readBatchSizeJsonValue.(float64); !ok {
		return nil, fmt.Errorf("failed to convert read_batch_size: %+v", readBatchSizeJsonValue)
	}
	store.ReadBatchSize = int(readBatchSize)

	if store.ReadBatchSize >= 1 {
		log.Info().Msgf("Reads will be done in key batches of size: %d", store.ReadBatchSize)
	}

	log.Info().Msgf("Using Valkey: %s", clientOption.InitAddress)
	return &store, nil
}

func getValkeyType(onlineStoreConfig map[string]interface{}) (valkeyType, error) {
	var t valkeyType

	valkeyTypeJsonValue, ok := onlineStoreConfig["valkey_type"]
	if !ok {
		// Default to "valkey"
		valkeyTypeJsonValue = "valkey"
	} else if valkeyTypeStr, ok := valkeyTypeJsonValue.(string); !ok {
		return -1, fmt.Errorf("failed to convert valkey_type to string: %+v", valkeyTypeJsonValue)
	} else {
		if valkeyTypeStr == "valkey" {
			t = valkeyNode
		} else if valkeyTypeStr == "valkey_cluster" {
			t = valkeyCluster
		} else {
			return -1, fmt.Errorf("failed to convert valkey_type to enum: %s. Must be one of 'valkey', 'valkey_cluster'", valkeyTypeStr)
		}
	}
	return t, nil
}

func (v *ValkeyOnlineStore) buildFeatureViewIndices(featureViewNames []string, featureNames []string) (map[string]int, map[int]string, int) {
	featureViewIndices := make(map[string]int)
	indicesFeatureView := make(map[int]string)
	index := len(featureNames)
	for _, featureViewName := range featureViewNames {
		if _, ok := featureViewIndices[featureViewName]; !ok {
			featureViewIndices[featureViewName] = index
			indicesFeatureView[index] = featureViewName
			index += 1
		}
	}
	return featureViewIndices, indicesFeatureView, index
}

func (v *ValkeyOnlineStore) buildHsetKeys(featureViewNames []string, featureNames []string, indicesFeatureView map[int]string, index int) ([]string, []string) {
	featureCount := len(featureNames)
	var hsetKeys = make([]string, index)
	h := murmur3.New32()
	intBuffer := h.Sum32()
	byteBuffer := make([]byte, 4)

	for i := 0; i < featureCount; i++ {
		h.Write([]byte(fmt.Sprintf("%s:%s", featureViewNames[i], featureNames[i])))
		intBuffer = h.Sum32()
		binary.LittleEndian.PutUint32(byteBuffer, intBuffer)
		hsetKeys[i] = string(byteBuffer)
		h.Reset()
	}
	for i := featureCount; i < index; i++ {
		view := indicesFeatureView[i]
		tsKey := fmt.Sprintf("_ts:%s", view)
		hsetKeys[i] = tsKey
		featureNames = append(featureNames, tsKey)
	}
	return hsetKeys, featureNames
}

func (v *ValkeyOnlineStore) buildValkeyKeys(entityKeys []*types.EntityKey) ([]*[]byte, error) {
	valkeyKeys := make([]*[]byte, len(entityKeys))
	for i := 0; i < len(entityKeys); i++ {
		var key, err = buildValkeyKey(v.project, entityKeys[i], v.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, err
		}
		valkeyKeys[i] = key
	}
	return valkeyKeys, nil
}

func (v *ValkeyOnlineStore) resolvedEntityKeySerializationVersion() int64 {
	return resolveEntityKeySerializationVersion(v.config)
}

func (v *ValkeyOnlineStore) warnPotentialVersionMismatch(views []string, numKeys int) {
	warnPotentialEntityKeyVersionMismatch(
		&v.versionMismatchWarned, "Valkey", v.resolvedEntityKeySerializationVersion(), views, numKeys)
}

func (v *ValkeyOnlineStore) OnlineReadV2(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	return v.OnlineRead(ctx, entityKeys, featureViewNames, featureNames)
}

func (v *ValkeyOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "OnlineRead")
	defer span.Finish()

	featureCount := len(featureNames)
	featureViewIndices, indicesFeatureView, index := v.buildFeatureViewIndices(featureViewNames, featureNames)
	hsetKeys, featureNamesWithTimeStamps := v.buildHsetKeys(featureViewNames, featureNames, indicesFeatureView, index)
	valkeyKeys, err := v.buildValkeyKeys(entityKeys)
	if err != nil {
		return nil, err
	}

	results := make([][]FeatureData, len(entityKeys))
	cmds := make(valkey.Commands, 0, len(entityKeys))

	for _, valkeyKey := range valkeyKeys {
		keyString := string(*valkeyKey)
		cmds = append(cmds, v.client.B().Hmget().Key(keyString).Field(hsetKeys...).Build())
	}

	var resContainsNonNil bool
	// viewsWithData records which feature views returned at least one non-nil value across all
	// entity keys. Used after the read completes to detect a complete miss (possible version mismatch).
	viewsWithData := make(map[string]bool)
	for entityIndex, values := range v.client.DoMulti(ctx, cmds...) {

		if err := values.Error(); err != nil {
			return nil, err
		}
		resContainsNonNil = false

		results[entityIndex] = make([]FeatureData, featureCount)

		res, err := values.ToArray()
		if err != nil {
			return nil, err
		}

		var value *types.Value
		var resString interface{}
		timeStampMap := make(map[string]*timestamppb.Timestamp, 1)

		for featureIndex, featureValue := range res {
			if featureIndex == featureCount {
				break
			}

			featureName := featureNamesWithTimeStamps[featureIndex]
			featureViewName := featureViewNames[featureIndex]
			value = &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}}
			resString = nil

			if !featureValue.IsNil() {
				resString, err = featureValue.ToString()
				if err != nil {
					return nil, err
				}

				valueString, ok := resString.(string)
				if !ok {
					return nil, errors.New("error parsing Value from valkey")
				}
				resContainsNonNil = true
				if value, _, err = utils.UnmarshalStoredProto([]byte(valueString)); err != nil {
					return nil, errors.New("error converting parsed valkey Value to types.Value")
				}
				viewsWithData[featureViewName] = true
			}

			if _, ok := timeStampMap[featureViewName]; !ok {
				timeStamp := timestamppb.Timestamp{}
				timeStampIndex := featureViewIndices[featureViewName]

				if !res[timeStampIndex].IsNil() {
					timeStampString, err := res[timeStampIndex].ToString()
					if err != nil {
						return nil, err
					}
					if err := proto.Unmarshal([]byte(timeStampString), &timeStamp); err != nil {
						return nil, errors.New("error converting parsed valkey Value to timestamppb.Timestamp")
					}
				}
				timeStampMap[featureViewName] = &timestamppb.Timestamp{Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos}
			}

			results[entityIndex][featureIndex] = FeatureData{Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
				Timestamp: timestamppb.Timestamp{Seconds: timeStampMap[featureViewName].Seconds, Nanos: timeStampMap[featureViewName].Nanos},
				Value:     types.Value{Val: value.Val},
			}
		}

		if !resContainsNonNil {
			results[entityIndex] = nil
		}
	}

	// Only warn if EVERY requested feature view returned zero data. A single sparse view is
	// normal; a complete miss across the whole request is a stronger — though not conclusive —
	// signal of a possible serialization version mismatch. Warn once (deduped) per request shape.
	requestedViews := make(map[string]struct{})
	for _, viewName := range featureViewNames {
		requestedViews[viewName] = struct{}{}
	}
	missedViews := make([]string, 0, len(requestedViews))
	for viewName := range requestedViews {
		if !viewsWithData[viewName] {
			missedViews = append(missedViews, viewName)
		}
	}
	if len(requestedViews) > 0 && len(missedViews) == len(requestedViews) {
		v.warnPotentialVersionMismatch(missedViews, len(entityKeys))
	}

	return results, nil
}

func valkeyBatchHMGET(
	ctx context.Context,
	client valkey.Client,
	entityKeyBin []byte,
	members [][]byte,
	fields []string,
	fv string,
	grp *utils.FvGroup,
	results [][]RangeFeatureData,
	eIdx int,
	batchSize int,
) error {

	if len(members) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = utils.DefaultBatchSize
	}

	nBatches := (len(members) + batchSize - 1) / batchSize

	// Results array to store results from each member
	batchResults := make([]*utils.MgetBatchResult, len(members))

	var wg sync.WaitGroup
	errChan := make(chan error, nBatches)

	for b := 0; b < nBatches; b++ {

		select {
		case <-ctx.Done():
			wg.Wait()
			close(errChan)
			return ctx.Err()
		default:
		}

		startIdx := b * batchSize
		end := startIdx + batchSize
		if end > len(members) {
			end = len(members)
		}

		batch := members[startIdx:end]

		// launch a goroutine for each batch
		wg.Add(1)
		go func(startIdx int, batch [][]byte) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			cmds := make([]valkey.Completed, 0, len(batch))
			for _, sortKeyBytes := range batch {
				hashKey := utils.BuildHashKey(entityKeyBin, sortKeyBytes)
				cmds = append(cmds, client.B().Hmget().Key(hashKey).Field(fields...).Build())
			}

			multi := client.DoMulti(ctx, cmds...)

			for i, sortKeyBytes := range batch {
				memberIdx := startIdx + i
				memberKey := base64.StdEncoding.EncodeToString(sortKeyBytes)
				cmdRes := multi[i]

				if err := cmdRes.Error(); err != nil {
					continue
				}

				arr, err := cmdRes.ToArray()
				if err != nil || len(arr) == 0 {
					continue
				}

				featureCount := len(grp.FeatNames)
				allNil := true
				for fi := 0; fi < featureCount && fi < len(arr)-1; fi++ {
					if !arr[fi].IsNil() {
						allNil = false
						break
					}
				}
				if allNil {
					continue
				}

				var eventTS timestamppb.Timestamp
				tsVal := arr[len(arr)-1]
				if !tsVal.IsNil() {
					tsStr, err := tsVal.ToString()
					if err == nil {
						eventTS = utils.DecodeTimestamp(tsStr)
					}
				}

				res := &utils.MgetBatchResult{
					MemberIdx: memberIdx,
					MemberKey: memberKey,
					Values:    make(map[int]interface{}),
					Statuses:  make(map[int]serving.FieldStatus),
					Timestamp: eventTS,
				}

				for localIdx, col := range grp.ColumnIndexes {
					if localIdx >= len(arr)-1 {
						continue
					}

					fvResp := arr[localIdx]

					var val interface{}
					var status serving.FieldStatus

					if fvResp.IsNil() {
						val = nil
						status = serving.FieldStatus_NULL_VALUE
					} else {
						strVal, err := fvResp.ToString()
						if err != nil {
							continue
						}

						decoded, st := utils.DecodeFeatureValue(
							strVal, fv, grp.FeatNames[localIdx], memberKey,
						)

						if st == serving.FieldStatus_NULL_VALUE {
							val = nil
						} else {
							val = decoded
						}

						status = st
					}

					_ = col
					res.Values[localIdx] = val
					res.Statuses[localIdx] = status
				}

				batchResults[memberIdx] = res
			}
		}(startIdx, batch)
	}

	wg.Wait()
	close(errChan)

	var allErrors []error
	for e := range errChan {
		if e != nil {
			allErrors = append(allErrors, e)
		}
	}
	if len(allErrors) > 0 {
		return errors.Join(allErrors...)
	}

	for _, result := range batchResults {
		if result == nil {
			continue
		}
		for localIdx, col := range grp.ColumnIndexes {
			results[eIdx][col].Values = append(results[eIdx][col].Values, result.Values[localIdx])
			results[eIdx][col].Statuses = append(results[eIdx][col].Statuses, result.Statuses[localIdx])
			results[eIdx][col].EventTimestamps = append(results[eIdx][col].EventTimestamps, result.Timestamp)
		}
	}
	return nil
}

// processEntityKey processes a single entity key.
//
// It performs the following steps:
//   - Generates the Valkey/zset key.
//   - Executes one ZRANGE per feature view.
//   - For each feature view, calls valkeyBatchHMGET.
//   - Applies the limit.
//   - Writes the output into results[eIdx].
func (v *ValkeyOnlineStore) processEntityKey(
	ctx context.Context,
	eIdx int,
	entityKey *types.EntityKey,
	fvGroups map[string]*utils.FvGroup,
	effectiveReverse bool,
	minScore, maxScore string,
	limit int64,
	results [][]RangeFeatureData,
	featNames, fvNames []string,
	sawAnyRow *atomic.Bool,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	entityKeyBin, err := SerializeEntityKeyWithProject(
		v.project,
		entityKey,
		v.config.EntityKeySerializationVersion,
	)
	if err != nil {
		return fmt.Errorf("failed to serialize entity key: %w", err)
	}

	// Initialize results row
	results[eIdx] = make([]RangeFeatureData, len(featNames))
	for i := range featNames {
		results[eIdx][i] = RangeFeatureData{
			FeatureView:     fvNames[i],
			FeatureName:     featNames[i],
			Values:          []interface{}{},
			Statuses:        []serving.FieldStatus{},
			EventTimestamps: []timestamppb.Timestamp{},
		}
	}
	zCmds := make([]valkey.Completed, 0, len(fvGroups))
	fvOrder := make([]string, 0, len(fvGroups))

	for fv := range fvGroups {
		zkey := utils.BuildZsetKey(fv, entityKeyBin)
		var cmd valkey.Completed

		if effectiveReverse {
			cmd = v.client.B().
				Zrange().
				Key(zkey).
				Min(maxScore).Max(minScore).Byscore().Rev().
				Build()
		} else {
			cmd = v.client.B().
				Zrange().
				Key(zkey).
				Min(minScore).Max(maxScore).Byscore().
				Build()
		}

		zCmds = append(zCmds, cmd)
		fvOrder = append(fvOrder, fv)
	}

	zResults := v.client.DoMulti(ctx, zCmds...)

	// Parse ZRANGE results by reading each result into zMembers
	zMembers := map[string][][]byte{}
	for i, fv := range fvOrder {
		raw := zResults[i]
		if err := raw.Error(); err != nil {
			log.Warn().
				Int("entity_index", eIdx).
				Str("feature_view", fv).
				Err(err).
				Msg("ZRANGE error")
			zMembers[fv] = nil
			continue
		}

		arr, err := raw.ToArray()
		if err != nil {
			zMembers[fv] = nil
			continue
		}

		out := make([][]byte, 0, len(arr))
		for _, itm := range arr {
			if itm.IsNil() {
				continue
			}
			s, err := itm.ToString()
			if err != nil {
				continue
			}
			out = append(out, []byte(s))
		}
		zMembers[fv] = out
	}

	// HMGET feature values for each feature view in parallel batches
	for fv, grp := range fvGroups {
		members := zMembers[fv]

		if len(members) == 0 {
			for _, col := range grp.ColumnIndexes {
				results[eIdx][col].Values = append(results[eIdx][col].Values, nil)
				results[eIdx][col].Statuses = append(results[eIdx][col].Statuses, serving.FieldStatus_NOT_FOUND)
				results[eIdx][col].EventTimestamps = append(results[eIdx][col].EventTimestamps, timestamppb.Timestamp{})
			}
			continue
		}
		// At least one stored member exists for this entity/feature view — record that the
		// request returned data so OnlineReadRange can detect a complete miss across all keys.
		if sawAnyRow != nil {
			sawAnyRow.Store(true)
		}
		// build list of hash fields to retrieve
		fields := append(append([]string{}, grp.FieldHashes...), grp.TsKey)
		if err := valkeyBatchHMGET(
			ctx,
			v.client,
			entityKeyBin,
			members,
			fields,
			fv,
			grp,
			results,
			eIdx,
			v.ReadBatchSize,
		); err != nil {
			return err
		}
		// apply limit if set by truncating each feature's result lists after HMGET
		if limit > 0 {
			for _, col := range grp.ColumnIndexes {
				r := &results[eIdx][col]
				if len(r.Values) > int(limit) {
					r.Values = r.Values[:limit]
					r.Statuses = r.Statuses[:limit]
					r.EventTimestamps = r.EventTimestamps[:limit]
				}
			}
		}
	}
	return nil
}

// OnlineReadRange performs the online read for range querying
//
// It executes the following steps:
//   - Groups features by feature view (FV).
//   - Spawns a goroutine per entity key.
//   - For each entity, runs processEntityKey.
//   - Waits for all entity goroutines to complete.
//   - Returns the final matrix.
func (v *ValkeyOnlineStore) OnlineReadRange(
	ctx context.Context,
	groupedRefs *model.GroupedRangeFeatureRefs,
) ([][]RangeFeatureData, error) {

	if groupedRefs == nil || len(groupedRefs.EntityKeys) == 0 {
		log.Warn().Msg("OnlineReadRange: no entity keys provided")
		return nil, fmt.Errorf("no entity keys provided")
	}

	featNames := groupedRefs.FeatureNames
	fvNames := groupedRefs.FeatureViewNames
	limit := int64(groupedRefs.Limit)

	effectiveReverse := utils.ComputeEffectiveReverse(
		groupedRefs.SortKeyFilters,
		groupedRefs.IsReverseSortOrder,
	)

	minScore, maxScore := "-inf", "+inf"
	if len(groupedRefs.SortKeyFilters) != 0 {
		minScore, maxScore = utils.GetScoreRange(groupedRefs.SortKeyFilters)
		if len(groupedRefs.SortKeyFilters) > 1 {
			log.Warn().
				Int("sort_key_count", len(groupedRefs.SortKeyFilters)).
				Msg("OnlineReadRange: more than one sort key filter provided; only the first will be used")
		}
	}

	fvGroups := map[string]*utils.FvGroup{}
	for i := range featNames {
		fv, fn := fvNames[i], featNames[i]
		g := fvGroups[fv]
		if g == nil {
			g = &utils.FvGroup{
				View:          fv,
				TsKey:         fmt.Sprintf("_ts:%s", fv),
				FeatNames:     []string{},
				FieldHashes:   []string{},
				ColumnIndexes: []int{},
			}
			fvGroups[fv] = g
		}
		g.FeatNames = append(g.FeatNames, fn)
		g.FieldHashes = append(g.FieldHashes, utils.Mmh3FieldHash(fv, fn))
		g.ColumnIndexes = append(g.ColumnIndexes, i)
	}

	results := make([][]RangeFeatureData, len(groupedRefs.EntityKeys))

	// sawAnyRow is set by the first entity goroutine that finds at least one stored member.
	// If it remains false after all goroutines finish, every entity key got a complete miss —
	// a possible (not guaranteed) indicator of a serialization version mismatch.
	var sawAnyRow atomic.Bool

	var wg sync.WaitGroup
	errChan := make(chan error, len(groupedRefs.EntityKeys))

	// Run each entity key in parallel
	for eIdx, entityKey := range groupedRefs.EntityKeys {
		wg.Add(1)
		go func(idx int, ek *types.EntityKey) {
			defer wg.Done()
			if err := v.processEntityKey(
				ctx,
				idx,
				ek,
				fvGroups,
				effectiveReverse,
				minScore, maxScore,
				limit,
				results,
				featNames, fvNames,
				&sawAnyRow,
			); err != nil {
				errChan <- err
			}
		}(eIdx, entityKey)
	}

	wg.Wait()
	close(errChan)

	var allErrors []error
	for err := range errChan {
		if err != nil {
			allErrors = append(allErrors, err)
		}
	}

	if len(allErrors) > 0 {
		return nil, errors.Join(allErrors...)
	}

	// Complete miss across all entity keys — warn once if this looks like a version mismatch.
	if !sawAnyRow.Load() {
		requestedViews := make([]string, 0, len(fvGroups))
		for fv := range fvGroups {
			requestedViews = append(requestedViews, fv)
		}
		v.warnPotentialVersionMismatch(requestedViews, len(groupedRefs.EntityKeys))
	}

	return results, nil
}

// Dummy destruct function to conform with plugin OnlineStore interface
func (v *ValkeyOnlineStore) Destruct() {

}

func buildValkeyKey(project string, entityKey *types.EntityKey, entityKeySerializationVersion int64) (*[]byte, error) {
	serKey, err := utils.SerializeEntityKey(entityKey, entityKeySerializationVersion)
	if err != nil {
		return nil, err
	}
	fullKey := append(*serKey, []byte(project)...)
	return &fullKey, nil
}

func (v *ValkeyOnlineStore) GetDataModelType() OnlineStoreDataModel {
	return EntityLevel
}

func (v *ValkeyOnlineStore) GetReadBatchSize() int {
	return v.ReadBatchSize
}
