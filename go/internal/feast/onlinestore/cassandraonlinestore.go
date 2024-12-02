package onlinestore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/utils"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog/log"

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
}

func parseStringField(config map[string]any, fieldName string, defaultValue string) (string, error) {
	rawValue, ok := config[fieldName]
	if !ok {
		return defaultValue, nil
	}
	stringValue, ok := rawValue.(string)
	if !ok {
		return "", fmt.Errorf("failed to convert %s to string: %v", fieldName, rawValue)
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

	if cassandraConfig.username != "" && cassandraConfig.password != "" {
		log.Warn().Msg("username/password not defined, will not be using authentication")
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
	return &store, nil
}

func (c *CassandraOnlineStore) getFqTableName(tableName string) string {
	return fmt.Sprintf(`"%s"."%s_%s"`, c.clusterConfigs.Keyspace, c.project, tableName)
}

func (c *CassandraOnlineStore) getCQLStatement(tableName string, featureNames []string) string {
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

func (c *CassandraOnlineStore) buildCassandraEntityKeys(entityKeys []*types.EntityKey) ([]any, map[string]int, error) {
	cassandraKeys := make([]any, len(entityKeys))
	cassandraKeyToEntityIndex := make(map[string]int)
	for i := 0; i < len(entityKeys); i++ {
		var key, err = utils.SerializeEntityKey(entityKeys[i], c.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, nil, err
		}
		encodedKey := hex.EncodeToString(*key)
		cassandraKeys[i] = encodedKey
		cassandraKeyToEntityIndex[encodedKey] = i
	}
	return cassandraKeys, cassandraKeyToEntityIndex, nil
}
func (c *CassandraOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	uniqueNames := make(map[string]int32)
	for _, fvName := range featureViewNames {
		uniqueNames[fvName] = 0
	}
	if len(uniqueNames) != 1 {
		return nil, fmt.Errorf("rejecting OnlineRead as more than 1 feature view was tried to be read at once")
	}

	serializedEntityKeys, serializedEntityKeyToIndex, err := c.buildCassandraEntityKeys(entityKeys)

	if err != nil {
		return nil, fmt.Errorf("error when serializing entity keys for Cassandra")
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
	tableName := c.getFqTableName(featureViewName)
	cqlStatement := c.getCQLStatement(tableName, featureNames)

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(serializedEntityKeys))

	errorsChannel := make(chan error, len(serializedEntityKeys))
	for _, serializedEntityKey := range serializedEntityKeys {
		go func(serEntityKey any) {
			defer waitGroup.Done()

			iter := c.session.Query(cqlStatement, serEntityKey).WithContext(ctx).Iter()

			rowIdx := serializedEntityKeyToIndex[serializedEntityKey.(string)]

			// fill the row with nulls if not found
			if iter.NumRows() == 0 {
				for _, featName := range featureNames {
					results[rowIdx][featureNamesToIdx[featName]] = FeatureData{
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
				return
			}

			scanner := iter.Scanner()
			var entityKey string
			var featureName string
			var eventTs time.Time
			var valueStr []byte
			var deserializedValue types.Value
			rowFeatures := make(map[string]FeatureData)
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
					// Convert the value to a FeatureData struct
					rowFeatures[featureName] = FeatureData{
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

			for _, featName := range featureNames {
				featureData, ok := rowFeatures[featName]
				if !ok {
					featureData = FeatureData{
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
				results[rowIdx][featureNamesToIdx[featName]] = featureData
			}
		}(serializedEntityKey)
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

func (c *CassandraOnlineStore) Destruct() {
	c.session.Close()
}
