package onlinestore

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/model"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/utils"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/gocql/gocql"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"google.golang.org/protobuf/types/known/timestamppb"
	gocqltrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/gocql/gocql"
)

type CassandraOnlineStore struct {
	project string

	// Cluster configurations for Cassandra/ScyllaDB
	clusterConfigs *gocql.ClusterConfig

	// Session object that holds information about the connection to the cluster
	session *gocql.Session

	config *registry.RepoConfig

	// The number of keys to include in a single CQL query for retrieval from the database
	KeyBatchSize int

	// The version of the table name format
	tableNameFormatVersion int

	// Caches table names instead of generating the table name every time
	tableNameCache sync.Map
}

type CassandraConfig struct {
	hosts                   []string
	username                string
	password                string
	keyspace                string
	protocolVersion         int
	loadBalancingPolicy     gocql.HostSelectionPolicy
	connectionTimeoutMillis int64
	requestTimeoutMillis    int64
	readBatchSize           int
}

const (
	V2_TABLE_NAME_FORMAT_MAX_LENGTH = 48
	BASE62_CHAR_SET                 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

// toBase62 converts a big integer to a base62 string.
func toBase62(num *big.Int) string {
	if num.Sign() == 0 {
		return "0"
	}

	base := big.NewInt(62)
	result := []string{}
	zero := big.NewInt(0)
	remainder := new(big.Int)

	for num.Cmp(zero) > 0 {
		num.DivMod(num, base, remainder)
		result = append([]string{string(BASE62_CHAR_SET[remainder.Int64()])}, result...)
	}

	return strings.Join(result, "")
}

// base62Encode converts a byte slice to a Base62 string.
func base62Encode(data []byte) string {
	num := new(big.Int).SetBytes(data)
	return toBase62(num)
}

func parseStringField(config map[string]any, fieldName string, defaultValue string) (string, error) {
	rawValue, ok := config[fieldName]
	if !ok {
		return defaultValue, nil
	}
	stringValue, ok := rawValue.(string)
	if !ok {
		return "", fmt.Errorf("failed to convert field %s to string: %v", fieldName, rawValue)
	}
	return stringValue, nil
}

func extractCassandraConfig(onlineStoreConfig map[string]any) (*CassandraConfig, error) {
	cassandraConfig := CassandraConfig{}

	// parse hosts
	cassandraHosts, ok := onlineStoreConfig["hosts"]
	if !ok {
		cassandraConfig.hosts = []string{"127.0.0.1"}
		log.Warn().Msg("host not provided: Using 127.0.0.1 instead")
	} else {
		var rawCassandraHosts []any
		if rawCassandraHosts, ok = cassandraHosts.([]any); !ok {
			return nil, fmt.Errorf("didn't pass a list of hosts in the 'hosts' field")
		}
		var cassandraHostsStr = make([]string, len(rawCassandraHosts))
		for i, rawHost := range rawCassandraHosts {
			hostStr, ok := rawHost.(string)
			if !ok {
				return nil, fmt.Errorf("failed to convert a host to a string: %+v", rawHost)
			}
			cassandraHostsStr[i] = hostStr
		}
		cassandraConfig.hosts = cassandraHostsStr
	}

	// parse username
	username, err := parseStringField(onlineStoreConfig, "username", "")
	if err != nil {
		return nil, err
	}
	cassandraConfig.username = username

	// parse password
	password, err := parseStringField(onlineStoreConfig, "password", "")
	if err != nil {
		return nil, err
	}
	cassandraConfig.password = password

	// parse keyspace
	keyspace, err := parseStringField(onlineStoreConfig, "keyspace", "feast_keyspace")
	if err != nil {
		return nil, err
	}
	cassandraConfig.keyspace = keyspace

	// parse protocolVersion
	protocolVersion, ok := onlineStoreConfig["protocol_version"]
	if !ok {
		protocolVersion = 4.0
		log.Warn().Msg("protocol_version not specified: Using 4 instead")
	}
	cassandraConfig.protocolVersion = int(protocolVersion.(float64))

	// parse loadBalancing
	loadBalancingDict, ok := onlineStoreConfig["load_balancing"]
	if !ok {
		cassandraConfig.loadBalancingPolicy = gocql.RoundRobinHostPolicy()
		log.Warn().Msg("no load balancing policy selected, defaulted to RoundRobinHostPolicy")
	} else {
		loadBalancingProps := loadBalancingDict.(map[string]any)
		policy := loadBalancingProps["load_balancing_policy"].(string)
		switch policy {
		case "TokenAwarePolicy(DCAwareRoundRobinPolicy)":
			rawLocalDC, ok := loadBalancingProps["local_dc"]
			if !ok {
				return nil, fmt.Errorf("a local_dc is needed for policy DCAwareRoundRobinPolicy")
			}
			localDc := rawLocalDC.(string)
			cassandraConfig.loadBalancingPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(localDc))
		case "DCAwareRoundRobinPolicy":
			rawLocalDC, ok := loadBalancingProps["local_dc"]
			if !ok {
				return nil, fmt.Errorf("a local_dc is needed for policy DCAwareRoundRobinPolicy")
			}
			localDc := rawLocalDC.(string)
			cassandraConfig.loadBalancingPolicy = gocql.DCAwareRoundRobinPolicy(localDc)
		default:
			log.Warn().Msg("defaulted to using RoundRobinHostPolicy")
			cassandraConfig.loadBalancingPolicy = gocql.RoundRobinHostPolicy()
		}
	}

	// parse connectionTimeoutMillis
	connectionTimeoutMillis, ok := onlineStoreConfig["connection_timeout_millis"]
	if !ok {
		connectionTimeoutMillis = 0.0
		log.Warn().Msg("connection_timeout_millis not specified, using gocql default")
	}
	cassandraConfig.connectionTimeoutMillis = int64(connectionTimeoutMillis.(float64))

	// parse requestTimeoutMillis
	requestTimeoutMillis, ok := onlineStoreConfig["request_timeout_millis"]
	if !ok {
		requestTimeoutMillis = 0.0
		log.Warn().Msg("request_timeout_millis not specified, using gocql default")
	}
	cassandraConfig.requestTimeoutMillis = int64(requestTimeoutMillis.(float64))

	readBatchSize, ok := onlineStoreConfig["read_batch_size"]
	if !ok {
		if legacyBatchSize, ok := onlineStoreConfig["key_batch_size"]; ok {
			readBatchSize = legacyBatchSize
			log.Warn().Msg("key_batch_size is deprecated, please use read_batch_size instead")
		} else {
			readBatchSize = 100.0
			log.Warn().Msg("read_batch_size not specified, defaulting to batches of size 100")
		}
	}
	cassandraConfig.readBatchSize = int(readBatchSize.(float64))

	return &cassandraConfig, nil
}

func NewCassandraOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]any) (*CassandraOnlineStore, error) {
	store := CassandraOnlineStore{
		project: project,
		config:  config,
	}

	cassandraConfig, configError := extractCassandraConfig(onlineStoreConfig)
	if configError != nil {
		return nil, configError
	}

	store.clusterConfigs = gocql.NewCluster(cassandraConfig.hosts...)
	store.clusterConfigs.ProtoVersion = cassandraConfig.protocolVersion
	store.clusterConfigs.Keyspace = cassandraConfig.keyspace

	store.clusterConfigs.PoolConfig.HostSelectionPolicy = cassandraConfig.loadBalancingPolicy

	if cassandraConfig.username == "" || cassandraConfig.password == "" {
		log.Warn().Msg("username and/or password not defined, will not be using authentication")
	} else {
		store.clusterConfigs.Authenticator = gocql.PasswordAuthenticator{
			Username: cassandraConfig.username,
			Password: cassandraConfig.password,
		}
	}

	if cassandraConfig.connectionTimeoutMillis != 0 {
		store.clusterConfigs.ConnectTimeout = time.Millisecond * time.Duration(cassandraConfig.connectionTimeoutMillis)
	}
	if cassandraConfig.requestTimeoutMillis != 0 {
		store.clusterConfigs.Timeout = time.Millisecond * time.Duration(cassandraConfig.requestTimeoutMillis)
	}

	store.clusterConfigs.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
	store.clusterConfigs.Consistency = gocql.LocalOne

	cassandraTraceServiceName := os.Getenv("DD_SERVICE") + "-cassandra"
	if cassandraTraceServiceName == "" {
		cassandraTraceServiceName = "cassandra.client" // default service name if DD_SERVICE is not set
	}
	createdSession, err := gocqltrace.CreateTracedSession(store.clusterConfigs, gocqltrace.WithServiceName(cassandraTraceServiceName))
	if err != nil {
		return nil, fmt.Errorf("unable to connect to the ScyllaDB database")
	}
	store.session = createdSession

	if cassandraConfig.readBatchSize <= 0 || cassandraConfig.readBatchSize > 100 {
		return nil, fmt.Errorf("read_batch_size must be greater than zero and less than or equal to 100")
	} else if cassandraConfig.readBatchSize == 1 {
		log.Info().Msg("key batching is disabled")
	} else {
		log.Info().Msgf("key batching is enabled with a batch size of %d", cassandraConfig.readBatchSize)
	}
	store.KeyBatchSize = cassandraConfig.readBatchSize

	// parse tableNameFormatVersion
	tableNameFormatVersion, ok := onlineStoreConfig["table_name_format_version"]
	if !ok {
		tableNameFormatVersion = 1.0
		log.Warn().Msg("table_name_format_version not specified: Using 1 instead")
	}
	store.tableNameFormatVersion = int(tableNameFormatVersion.(float64))

	return &store, nil
}

// fqTableNameV2 generates a fully qualified table name with Base62 hashing.
func getFqTableNameV2(keyspace string, project string, featureViewName string) string {
	dbTableName := fmt.Sprintf("%s_%s", project, featureViewName)

	if len(dbTableName) <= V2_TABLE_NAME_FORMAT_MAX_LENGTH {
		return dbTableName
	}

	// Truncate project & feature view name
	prjPrefixMaxLen := 5
	fvPrefixMaxLen := 5
	truncatedProject := project[:min(len(project), prjPrefixMaxLen)]
	truncatedFv := featureViewName[:min(len(featureViewName), fvPrefixMaxLen)]

	projectToHash := project[len(truncatedProject):]
	fvToHash := featureViewName[len(truncatedFv):]

	projectHashBytes := md5.Sum([]byte(projectToHash))
	fvHashBytes := md5.Sum([]byte(fvToHash))

	// Compute MD5 hash and encode to Base62
	projectHash := base62Encode(projectHashBytes[:])
	fvHash := base62Encode(fvHashBytes[:])

	// Format final table name (48 - 3 underscores - 5 prj prefix - 5 fv prefix) / 2 = ~17 each
	dbTableName = fmt.Sprintf("%s_%s_%s_%s",
		truncatedProject, projectHash[:17], truncatedFv, fvHash[:18])

	return dbTableName
}

func (c *CassandraOnlineStore) getFqTableName(keySpace string, project string, featureViewName string, tableNameVersion int) (string, error) {
	var dbTableName string

	tableName := fmt.Sprintf("%s_%s", project, featureViewName)

	if cacheValue, found := c.tableNameCache.Load(tableName); found {
		return fmt.Sprintf(`"%s"."%s"`, keySpace, cacheValue.(string)), nil
	}

	if tableNameVersion == 1 {
		dbTableName = tableName
	} else if tableNameVersion == 2 {
		dbTableName = getFqTableNameV2(keySpace, project, featureViewName)
	} else {
		return "", fmt.Errorf("unknown table name format version: %d", tableNameVersion)
	}

	c.tableNameCache.Store(tableName, dbTableName)

	return fmt.Sprintf(`"%s"."%s"`, keySpace, dbTableName), nil
}

func (c *CassandraOnlineStore) getSingleKeyCQLStatement(tableName string, featureNames []string) string {
	// this prevents fetching unnecessary features
	quotedFeatureNames := make([]string, len(featureNames))
	for i, featureName := range featureNames {
		quotedFeatureNames[i] = fmt.Sprintf(`'%s'`, featureName)
	}

	return fmt.Sprintf(
		`SELECT "entity_key", "feature_name", "event_ts", "value" FROM %s WHERE "entity_key" = ? AND "feature_name" IN (%s)`,
		tableName,
		strings.Join(quotedFeatureNames, ","),
	)
}

func (c *CassandraOnlineStore) getMultiKeyCQLStatement(tableName string, featureNames []string, nkeys int) string {
	// this prevents fetching unnecessary features
	quotedFeatureNames := make([]string, len(featureNames))
	for i, featureName := range featureNames {
		quotedFeatureNames[i] = fmt.Sprintf(`'%s'`, featureName)
	}

	keyPlaceholders := make([]string, nkeys)
	for i := 0; i < nkeys; i++ {
		keyPlaceholders[i] = "?"
	}
	return fmt.Sprintf(
		`SELECT "entity_key", "feature_name", "event_ts", "value" FROM %s WHERE "entity_key" IN (%s) AND "feature_name" IN (%s)`,
		tableName,
		strings.Join(keyPlaceholders, ","),
		strings.Join(quotedFeatureNames, ","),
	)
}

func (c *CassandraOnlineStore) buildCassandraEntityKeys(entityKeys []*types.EntityKey) ([]any, map[string]int, error) {
	cassandraKeys := make([]any, len(entityKeys))
	cassandraKeyToEntityIndex := make(map[string]int)
	for i := 0; i < len(entityKeys); i++ {
		var key, err = utils.SerializeEntityKey(entityKeys[i], 2)
		if err != nil {
			return nil, nil, err
		}
		encodedKey := hex.EncodeToString(*key)
		cassandraKeys[i] = encodedKey
		cassandraKeyToEntityIndex[encodedKey] = i
	}
	return cassandraKeys, cassandraKeyToEntityIndex, nil
}

func (c *CassandraOnlineStore) validateUniqueFeatureNames(featureViewNames []string) error {
	uniqueNames := make(map[string]int32)
	for _, fvName := range featureViewNames {
		uniqueNames[fvName] = 0
	}
	if len(uniqueNames) != 1 {
		return fmt.Errorf("rejecting OnlineRead as more than 1 feature view was tried to be read at once")
	}
	return nil
}

func (c *CassandraOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	if err := c.validateUniqueFeatureNames(featureViewNames); err != nil {
		return nil, err
	}

	serializedEntityKeys, serializedEntityKeyToIndex, err := c.buildCassandraEntityKeys(entityKeys)

	if err != nil {
		return nil, fmt.Errorf("error when serializing entity keys for Cassandra: %v", err)
	}
	results := make([][]FeatureData, len(entityKeys))
	for i := range results {
		results[i] = make([]FeatureData, len(featureNames))
	}

	featureNamesToIdx := make(map[string]int)
	for idx, name := range featureNames {
		featureNamesToIdx[name] = idx
	}

	featureViewName := featureViewNames[0]

	// Prepare the query
	tableName, err := c.getFqTableName(c.clusterConfigs.Keyspace, c.project, featureViewName, c.tableNameFormatVersion)
	if err != nil {
		return nil, err
	}

	// Key batching
	nKeys := len(serializedEntityKeys)
	batchSize := c.KeyBatchSize
	nBatches := int(math.Ceil(float64(nKeys) / float64(batchSize)))
	batches := make([][]any, nBatches)
	nAssigned := 0
	for i := 0; i < nBatches; i++ {
		thisBatchSize := int(math.Min(float64(batchSize), float64(nKeys-nAssigned)))
		nAssigned += thisBatchSize
		batches[i] = make([]any, thisBatchSize)
		for j := 0; j < thisBatchSize; j++ {
			batches[i][j] = serializedEntityKeys[i*batchSize+j]
		}
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(nBatches)

	errorsChannel := make(chan error, nBatches)
	var currentBatchLength int
	var prevBatchLength int
	var cqlStatement string
	for _, batch := range batches {
		currentBatchLength = len(batch)
		if currentBatchLength != prevBatchLength {
			cqlStatement = c.getMultiKeyCQLStatement(tableName, featureNames, currentBatchLength)
			prevBatchLength = currentBatchLength
		}
		go func(keyBatch []any, statement string) {
			defer waitGroup.Done()
			iter := c.session.Query(statement, keyBatch...).WithContext(ctx).Iter()

			scanner := iter.Scanner()
			var entityKey string
			var featureName string
			var eventTs time.Time
			var valueStr []byte
			var deserializedValue types.Value
			// key 1: entityKey - key 2: featureName
			batchFeatures := make(map[string]map[string]*FeatureData)
			for scanner.Next() {
				err := scanner.Scan(&entityKey, &featureName, &eventTs, &valueStr)
				if err != nil {
					errorsChannel <- errors.New("could not read row in query for (entity key, feature name, value, event ts)")
					return
				}
				if err := proto.Unmarshal(valueStr, &deserializedValue); err != nil {
					errorsChannel <- errors.New("error converting parsed Cassandra Value to types.Value")
					return
				}

				if deserializedValue.Val != nil {
					if batchFeatures[entityKey] == nil {
						batchFeatures[entityKey] = make(map[string]*FeatureData)
					}
					batchFeatures[entityKey][featureName] = &FeatureData{
						Reference: serving.FeatureReferenceV2{
							FeatureViewName: featureViewName,
							FeatureName:     featureName,
						},
						Timestamp: timestamppb.Timestamp{Seconds: eventTs.Unix(), Nanos: int32(eventTs.Nanosecond())},
						Value: types.Value{
							Val: deserializedValue.Val,
						},
					}
				}
			}

			if err := scanner.Err(); err != nil {
				errorsChannel <- errors.New("failed to scan features: " + err.Error())
				return
			}

			for _, serializedEntityKey := range keyBatch {
				for _, featName := range featureNames {
					keyString := serializedEntityKey.(string)

					if featureData, exists := batchFeatures[keyString][featName]; exists {
						results[serializedEntityKeyToIndex[keyString]][featureNamesToIdx[featName]] = FeatureData{
							Reference: serving.FeatureReferenceV2{
								FeatureViewName: featureData.Reference.FeatureViewName,
								FeatureName:     featureData.Reference.FeatureName,
							},
							Timestamp: timestamppb.Timestamp{Seconds: featureData.Timestamp.Seconds, Nanos: featureData.Timestamp.Nanos},
							Value: types.Value{
								Val: featureData.Value.Val,
							},
						}
					} else {
						results[serializedEntityKeyToIndex[keyString]][featureNamesToIdx[featName]] = FeatureData{
							Reference: serving.FeatureReferenceV2{
								FeatureViewName: featureViewName,
								FeatureName:     featName,
							},
							Value: types.Value{
								Val: &types.Value_NullVal{
									NullVal: types.Null_NULL,
								},
							},
						}
					}
				}
			}
		}(batch, cqlStatement)
	}
	// wait until all concurrent single-key queries are done
	waitGroup.Wait()
	close(errorsChannel)

	var collectedErrors []error
	for err := range errorsChannel {
		if err != nil {
			collectedErrors = append(collectedErrors, err)
		}
	}
	if len(collectedErrors) > 0 {
		return nil, errors.Join(collectedErrors...)
	}

	return results, nil
}

func (c *CassandraOnlineStore) rangeFilterToCQL(filter *model.SortKeyFilter) (string, []interface{}) {
	rangeParams := make([]interface{}, 0)

	equality := ""
	if filter.Equals != nil {
		equality = fmt.Sprintf(`"%s" = ?`, filter.SortKeyName)
		rangeParams = append(rangeParams, filter.Equals)
		return equality, rangeParams
	}

	rangeStart := ""
	if filter.RangeStart != nil {
		if filter.StartInclusive {
			rangeStart = fmt.Sprintf(`"%s" >= ?`, filter.SortKeyName)
		} else {
			rangeStart = fmt.Sprintf(`"%s" > ?`, filter.SortKeyName)
		}
		rangeParams = append(rangeParams, filter.RangeStart)
	}
	rangeEnd := ""
	if filter.RangeEnd != nil {
		if filter.EndInclusive {
			rangeEnd = fmt.Sprintf(`"%s" <= ?`, filter.SortKeyName)
		} else {
			rangeEnd = fmt.Sprintf(`"%s" < ?`, filter.SortKeyName)
		}
		rangeParams = append(rangeParams, filter.RangeEnd)
	}

	if rangeStart != "" && rangeEnd != "" {
		return fmt.Sprintf(`%s AND %s`, rangeStart, rangeEnd), rangeParams
	} else if rangeStart != "" {
		return rangeStart, rangeParams
	} else if rangeEnd != "" {
		return rangeEnd, rangeParams
	} else {
		return "", rangeParams
	}
}

func (c *CassandraOnlineStore) buildRangeQueryCQL(
	tableName string,
	featureNames []string,
	numKeys int,
	sortKeyFilters []*model.SortKeyFilter,
	limit int32,
	isReverseSortOrder bool,
) (string, []interface{}) {
	quotedFeatures := make([]string, len(featureNames))
	for i, name := range featureNames {
		quotedFeatures[i] = fmt.Sprintf(`"%s"`, name)
	}

	keyPlaceholders := make([]string, numKeys)
	for i := range keyPlaceholders {
		keyPlaceholders[i] = "?"
	}

	whereClause := ""
	orderByClause := ""
	params := make([]interface{}, 0)

	if len(sortKeyFilters) > 0 {
		rangeFilters := make([]string, 0)
		orderBy := make([]string, 0)

		for _, f := range sortKeyFilters {
			filterStr, filterParams := c.rangeFilterToCQL(f)
			if filterStr != "" {
				rangeFilters = append(rangeFilters, filterStr)
			}
			params = append(params, filterParams...)
			if f.Order != nil {
				orderBy = append(orderBy,
					fmt.Sprintf(`"%s" %s`, f.SortKeyName, f.Order.String()))
			}
		}

		if len(rangeFilters) > 0 {
			whereClause = " AND " + strings.Join(rangeFilters, " AND ")
		}

		// Only add ORDER BY if IsReverseSortOrder is true
		if isReverseSortOrder && len(orderBy) > 0 {
			orderByClause = " ORDER BY " + strings.Join(orderBy, ", ")
		}
	}

	limitClause := ""
	if limit > 0 {
		limitClause = " PER PARTITION LIMIT ?"
		params = append(params, limit)
	}

	var keyCondition string
	if numKeys == 1 {
		keyCondition = `"entity_key" = ?`
	} else {
		keyCondition = fmt.Sprintf(`"entity_key" IN (%s)`, strings.Join(keyPlaceholders, ", "))
	}

	cql := fmt.Sprintf(
		`SELECT "entity_key", "event_ts", %s FROM %s WHERE %s%s%s%s`,
		strings.Join(quotedFeatures, ", "),
		tableName,
		keyCondition,
		whereClause,
		orderByClause,
		limitClause,
	)

	return cql, params
}

type preparedRangeReadContext struct {
	serializedEntityKeys       []any
	serializedEntityKeyToIndex map[string]int
	featureNamesToIdx          map[string]int
	tableName                  string
	featureViewName            string
}

func (c *CassandraOnlineStore) prepareOnlineRangeRead(
	entityKeys []*types.EntityKey,
	featureViewNames []string,
	featureNames []string,
) (*preparedRangeReadContext, error) {
	if err := c.validateUniqueFeatureNames(featureViewNames); err != nil {
		return nil, err
	}

	serializedEntityKeys, serializedEntityKeyToIndex, err := c.buildCassandraEntityKeys(entityKeys)
	if err != nil {
		return nil, fmt.Errorf("error when serializing entity keys for Cassandra: %v", err)
	}

	featureViewName := featureViewNames[0]
	tableName, err := c.getFqTableName(c.clusterConfigs.Keyspace, c.project, featureViewName, c.tableNameFormatVersion)
	if err != nil {
		return nil, err
	}

	featureNamesToIdx := make(map[string]int)
	for idx, name := range featureNames {
		featureNamesToIdx[name] = idx
	}

	return &preparedRangeReadContext{
		serializedEntityKeys:       serializedEntityKeys,
		serializedEntityKeyToIndex: serializedEntityKeyToIndex,
		featureNamesToIdx:          featureNamesToIdx,
		tableName:                  tableName,
		featureViewName:            featureViewName,
	}, nil
}

func (c *CassandraOnlineStore) OnlineReadRange(ctx context.Context, groupedRefs *model.GroupedRangeFeatureRefs) ([][]RangeFeatureData, error) {
	prepCtx, err := c.prepareOnlineRangeRead(groupedRefs.EntityKeys, groupedRefs.FeatureViewNames, groupedRefs.FeatureNames)
	if err != nil {
		return nil, err
	}

	results := make([][]RangeFeatureData, len(groupedRefs.EntityKeys))
	for i := range results {
		results[i] = make([]RangeFeatureData, len(groupedRefs.FeatureNames))
	}

	batchSize := c.KeyBatchSize
	if groupedRefs.IsReverseSortOrder {
		if batchSize > 1 && len(prepCtx.serializedEntityKeys) > 1 {
			log.Warn().Msg("Reverse sort order is enabled, overriding read batch size to 1. It is not recommended to use reverse sort order for common use cases.")
		}
		batchSize = 1 // Reverse order only supports a batch size of 1
	}
	nBatches := int(math.Ceil(float64(len(prepCtx.serializedEntityKeys)) / float64(batchSize)))

	var waitGroup sync.WaitGroup
	errorsChannel := make(chan error, nBatches)

	for i := 0; i < nBatches; i++ {
		start := i * batchSize
		end := int(math.Min(float64(start+batchSize), float64(len(prepCtx.serializedEntityKeys))))
		keyBatch := prepCtx.serializedEntityKeys[start:end]

		cqlStatement, rangeParams := c.buildRangeQueryCQL(
			prepCtx.tableName, groupedRefs.FeatureNames, len(keyBatch), groupedRefs.SortKeyFilters, groupedRefs.Limit, groupedRefs.IsReverseSortOrder,
		)

		queryParams := append([]interface{}{}, keyBatch...)
		queryParams = append(queryParams, rangeParams...)
		seenKeys := make(map[string]bool)

		waitGroup.Add(1)
		go func(stmt string, params []interface{}, batchKeys []any) {
			defer waitGroup.Done()

			iter := c.session.Query(stmt, params...).WithContext(ctx).Iter()
			readValues := make(map[string]interface{})

			for iter.MapScan(readValues) {
				entityKey := readValues["entity_key"].(string)
				eventTs := readValues["event_ts"].(time.Time)

				rowIdx, ok := prepCtx.serializedEntityKeyToIndex[entityKey]
				if !ok {
					continue
				}

				rowData := results[rowIdx]

				for _, featName := range groupedRefs.FeatureNames {
					idx := prepCtx.featureNamesToIdx[featName]
					var val interface{}
					var status serving.FieldStatus

					if _, isSortKey := groupedRefs.SortKeyNames[featName]; isSortKey {
						var exists bool
						val, exists = readValues[featName]
						if !exists {
							status = serving.FieldStatus_NOT_FOUND
							val = nil
						} else if val == nil {
							status = serving.FieldStatus_NULL_VALUE
						} else {
							status = serving.FieldStatus_PRESENT
						}
					} else {
						if valueStr, ok := readValues[featName]; ok {
							var message types.Value
							if err := proto.Unmarshal(valueStr.([]byte), &message); err != nil {
								errorsChannel <- errors.New("error converting parsed Cassandra Value to types.Value")
								return
							}
							if message.Val == nil {
								val = nil
								status = serving.FieldStatus_NULL_VALUE
							} else {
								switch message.Val.(type) {
								case *types.Value_UnixTimestampVal:
									// null timestamps are read as min int64, so we convert them to nil
									if message.Val.(*types.Value_UnixTimestampVal).UnixTimestampVal == math.MinInt64 {
										val = nil
										status = serving.FieldStatus_NULL_VALUE
									} else {
										val = &types.Value{Val: message.Val}
										status = serving.FieldStatus_PRESENT
									}
								default:
									val = &types.Value{Val: message.Val}
									status = serving.FieldStatus_PRESENT
								}
							}
						} else {
							val = nil
							status = serving.FieldStatus_NOT_FOUND
						}
					}

					appendRangeFeature(&rowData[idx], featName, prepCtx.featureViewName, val, status, eventTs)
				}

				seenKeys[entityKey] = true
				for k := range readValues {
					delete(readValues, k)
				}
			}

			if err := iter.Close(); err != nil {
				errorsChannel <- fmt.Errorf("query error: %w", err)
				return
			}

			for _, serializedEntityKey := range batchKeys {
				keyString := serializedEntityKey.(string)
				rowIdx := prepCtx.serializedEntityKeyToIndex[keyString]

				if !seenKeys[keyString] {
					for _, featName := range groupedRefs.FeatureNames {
						results[rowIdx][prepCtx.featureNamesToIdx[featName]] = RangeFeatureData{
							FeatureView: prepCtx.featureViewName,
							FeatureName: featName,
							Values:      []interface{}{nil},
							Statuses:    []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
							EventTimestamps: []timestamppb.Timestamp{
								{Seconds: 0, Nanos: 0},
							},
						}
					}
				}
			}

		}(cqlStatement, queryParams, keyBatch)
	}

	waitGroup.Wait()
	close(errorsChannel)

	var allErrors []error
	for err := range errorsChannel {
		if err != nil {
			allErrors = append(allErrors, err)
		}
	}

	if len(allErrors) > 0 {
		return nil, errors.Join(allErrors...)
	}

	return results, nil
}

func appendRangeFeature(row *RangeFeatureData, featName, view string, val interface{}, status serving.FieldStatus, ts time.Time) {
	row.FeatureView = view
	row.FeatureName = featName

	// Ensure nil values stay as nil, not converted to empty values
	if status == serving.FieldStatus_NOT_FOUND || status == serving.FieldStatus_NULL_VALUE {
		row.Values = append(row.Values, nil)
	} else {
		row.Values = append(row.Values, val)
	}

	row.Statuses = append(row.Statuses, status)
	row.EventTimestamps = append(row.EventTimestamps, timestamppb.Timestamp{
		Seconds: ts.Unix(),
		Nanos:   int32(ts.Nanosecond()),
	})
}

func (c *CassandraOnlineStore) Destruct() {
	c.session.Close()
}
