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

	return results, nil
}

// valkeyBatchHMGET executes HMGET in pipelined batches for a single feature view.
func valkeyBatchHMGET(
	ctx context.Context,
	client valkey.Client,
	entityKeyBin []byte,
	members [][]byte,
	fields []string,
	fv string,
	grp *fvGroup,
	results [][]RangeFeatureData,
	eIdx int,
) error {
	for start := 0; start < len(members); start += PIPELINE_BATCH_SIZE {
		end := min(start+PIPELINE_BATCH_SIZE, len(members))
		batch := members[start:end]

		// Build all HMGET commands for this batch
		cmds := make([]valkey.Completed, 0, len(batch))
		for _, sortKeyBytes := range batch {
			hashKey := utils.BuildHashKey(entityKeyBin, sortKeyBytes)
			cmds = append(cmds, client.B().Hmget().Key(hashKey).Field(fields...).Build())
		}

		multi := client.DoMulti(ctx, cmds...)

		// Decode each HMGET result
		for i, sortKeyBytes := range batch {
			memberKey := base64.StdEncoding.EncodeToString(sortKeyBytes)
			cmdRes := multi[i]

			// If hash key is missing or HMGET failed: skip this ZSET member entirely.
			if err := cmdRes.Error(); err != nil {
				continue
			}

			arr, err := cmdRes.ToArray()
			if err != nil || len(arr) == 0 {
				continue
			}

			featureFieldCount := len(grp.featNames)

			allNil := true
			for fi := 0; fi < featureFieldCount && fi < len(arr)-1; fi++ {
				if !arr[fi].IsNil() {
					allNil = false
					break
				}
			}
			if allNil {
				continue
			}

			// Decode timestamp (last field)
			var eventTS timestamppb.Timestamp
			if len(arr) > 0 {
				tsVal := arr[len(arr)-1]
				if !tsVal.IsNil() {
					tsStr, err := tsVal.ToString()
					if err == nil {
						eventTS = utils.DecodeTimestamp(tsStr)
					}
				}
			}

			// Decode each feature
			for iCol, col := range grp.columnIndexes {
				fieldIdx := iCol

				if fieldIdx >= len(arr)-1 {
					continue
				}

				fvResp := arr[fieldIdx]

				var (
					val    interface{}
					status serving.FieldStatus
				)

				if fvResp.IsNil() {
					val = nil
					status = serving.FieldStatus_NULL_VALUE
				} else {
					strVal, err := fvResp.ToString()
					if err != nil {
						continue
					}
					raw := interface{}(strVal)
					val, status = utils.DecodeFeatureValue(raw, fv, grp.featNames[iCol], memberKey)

					if status == serving.FieldStatus_NULL_VALUE {
						val = nil
					}
				}

				results[eIdx][col].Values = append(results[eIdx][col].Values, val)
				results[eIdx][col].Statuses = append(results[eIdx][col].Statuses, status)
				results[eIdx][col].EventTimestamps = append(results[eIdx][col].EventTimestamps, eventTS)
			}
		}
	}
	return nil
}

func (v *ValkeyOnlineStore) OnlineReadRange(
	ctx context.Context,
	groupedRefs *model.GroupedRangeFeatureRefs,
) ([][]RangeFeatureData, error) {

	if groupedRefs == nil || len(groupedRefs.EntityKeys) == 0 {
		return nil, fmt.Errorf("no entity keys provided")
	}

	featureNames := groupedRefs.FeatureNames
	featureViewNames := groupedRefs.FeatureViewNames
	limit := int64(groupedRefs.Limit)

	effectiveReverse := utils.ComputeEffectiveReverse(
		groupedRefs.SortKeyFilters,
		groupedRefs.IsReverseSortOrder,
	)
	var minScore, maxScore string
	if len(groupedRefs.SortKeyFilters) == 0 {
		// No predicate on sort key: fetch all, subject to Limit.
		minScore = "-inf"
		maxScore = "+inf"
	} else {
		minScore, maxScore = utils.GetScoreRange(groupedRefs.SortKeyFilters)
		if len(groupedRefs.SortKeyFilters) > 1 {
			log.Warn().
				Int("sort_key_count", len(groupedRefs.SortKeyFilters)).
				Msg("OnlineReadRange: more than one sort key filter provided; only the first will be used")
		}
	}

	//group features by feature view
	fvGroups := map[string]*fvGroup{}
	for i := range featureNames {
		fv, fn := featureViewNames[i], featureNames[i]
		g := fvGroups[fv]
		if g == nil {
			g = &fvGroup{
				view:          fv,
				tsKey:         fmt.Sprintf("_ts:%s", fv),
				featNames:     []string{},
				fieldHashes:   []string{},
				columnIndexes: []int{},
			}
			fvGroups[fv] = g
		}
		g.featNames = append(g.featNames, fn)
		g.fieldHashes = append(g.fieldHashes, utils.Mmh3FieldHash(fv, fn))
		g.columnIndexes = append(g.columnIndexes, i)
	}

	results := make([][]RangeFeatureData, len(groupedRefs.EntityKeys))

	// process each entity key
	for eIdx, entityKey := range groupedRefs.EntityKeys {

		entityKeyBin, err := SerializeEntityKeyWithProject(
			v.project,
			entityKey,
			v.config.EntityKeySerializationVersion,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize entity key: %w", err)
		}

		results[eIdx] = make([]RangeFeatureData, len(featureNames))
		for i := range featureNames {
			results[eIdx][i] = RangeFeatureData{
				FeatureView:     featureViewNames[i],
				FeatureName:     featureNames[i],
				Values:          []interface{}{},
				Statuses:        []serving.FieldStatus{},
				EventTimestamps: []timestamppb.Timestamp{},
			}
		}

		type zrangeRes struct {
			view    string
			members [][]byte
			err     error
		}

		zResponses := make(map[string]zrangeRes)
		zCmds := make([]valkey.Completed, 0, len(fvGroups))
		fvOrder := make([]string, 0, len(fvGroups))

		for fv := range fvGroups {

			zkey := utils.BuildZsetKey(fv, entityKeyBin)

			var cmd valkey.Completed

			if effectiveReverse {
				// Reverse sort order: use BYSCORE + REV and swap min/max
				zr := v.client.B().
					Zrange().
					Key(zkey).
					Min(maxScore).
					Max(minScore).
					Byscore().
					Rev()

				if limit > 0 {
					cmd = zr.Limit(0, limit).Build()
				} else {
					cmd = zr.Build()
				}
			} else {
				// Forward sort order: normal min/max BYSCORE
				zr := v.client.B().
					Zrange().
					Key(zkey).
					Min(minScore).
					Max(maxScore).
					Byscore()

				if limit > 0 {
					cmd = zr.Limit(0, limit).Build()
				} else {
					cmd = zr.Build()
				}
			}

			fvOrder = append(fvOrder, fv)
			zCmds = append(zCmds, cmd)
		}

		// Execute batch ZRANGE
		zResults := v.client.DoMulti(ctx, zCmds...)

		// Decode ZRANGE MEMBERS
		for i, fv := range fvOrder {
			res := zResults[i]
			if err := res.Error(); err != nil {
				zResponses[fv] = zrangeRes{view: fv, members: nil, err: err}
				continue
			}

			arr, err := res.ToArray()
			if err != nil {
				zResponses[fv] = zrangeRes{view: fv, members: nil, err: err}
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

			zResponses[fv] = zrangeRes{view: fv, members: out, err: nil}
		}

		//HMGET batching per FV
		for fv, grp := range fvGroups {
			zr := zResponses[fv]

			if zr.err != nil || len(zr.members) == 0 {
				for _, col := range grp.columnIndexes {
					results[eIdx][col].Values = append(results[eIdx][col].Values, nil)
					results[eIdx][col].Statuses = append(results[eIdx][col].Statuses, serving.FieldStatus_NOT_FOUND)
					results[eIdx][col].EventTimestamps = append(results[eIdx][col].EventTimestamps, timestamppb.Timestamp{})
				}
				continue
			}

			// HMGET fields: feature hashes + tsKey
			fields := append(append([]string{}, grp.fieldHashes...), grp.tsKey)

			if err := valkeyBatchHMGET(
				ctx,
				v.client,
				entityKeyBin,
				zr.members,
				fields,
				fv,
				grp,
				results,
				eIdx,
			); err != nil {
				return nil, err
			}
		}
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
