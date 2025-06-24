package onlinestore

import (
	"context"
	"encoding/hex"
	"fmt"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/roberson-io/mmh3"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"runtime"
	"sync"
	"time"
)

type batchResult struct {
	index    int
	response *dynamodb.BatchGetItemOutput
	err      error
}

type DynamodbOnlineStore struct {
	// Feast project name
	// TODO: Should we remove project as state that is tracked at the store level?
	project string

	client *dynamodb.Client

	config *registry.RepoConfig

	// dynamodb configuration
	consistentRead *bool
	batchSize      *int
}

func NewDynamodbOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*DynamodbOnlineStore, error) {
	store := DynamodbOnlineStore{
		project: project,
		config:  config,
	}

	// aws configuration
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg, err := awsConfig.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	store.client = dynamodb.NewFromConfig(cfg)

	// dynamodb configuration
	consistentRead, ok := onlineStoreConfig["consistent_reads"].(bool)
	if !ok {
		consistentRead = false
	}
	store.consistentRead = &consistentRead

	var batchSize int
	if batchSizeFloat, ok := onlineStoreConfig["batch_size"].(float64); ok {
		batchSize = int(batchSizeFloat)
	} else {
		batchSize = 40
	}
	store.batchSize = &batchSize

	return &store, nil
}

func (d *DynamodbOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	// prevent resource waste in case context is canceled earlier
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	results := make([][]FeatureData, len(entityKeys))

	// serialize entity key into entity hash id
	entityIndexMap := make(map[string]int)
	entityIds := make([]string, 0, len(entityKeys))
	unprocessedEntityIds := make(map[string]bool)
	for i, entityKey := range entityKeys {
		serKey, err := serializeEntityKey(entityKey, d.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, err
		}
		entityId := hex.EncodeToString(mmh3.Hashx64_128(*serKey, 0))
		entityIds = append(entityIds, entityId)
		entityIndexMap[entityId] = i
		unprocessedEntityIds[entityId] = false
	}

	// metadata from feature views, feature names
	featureMap, featureNamesIndex, err := makeFeatureMeta(featureViewNames, featureNames)
	if err != nil {
		return nil, err
	}

	// initialize `FeatureData` slice
	featureCount := len(featureNamesIndex)
	for i := 0; i < len(results); i++ {
		results[i] = make([]FeatureData, featureCount)
	}

	// controls the maximum number of concurrent goroutines sending requests to DynamoDB using a semaphore
	cpuCount := runtime.NumCPU()
	sem := semaphore.NewWeighted(int64(cpuCount * 2))

	var mu sync.Mutex
	for featureViewName, featureNames := range featureMap {
		tableName := fmt.Sprintf("%s.%s", d.project, featureViewName)

		var batchGetItemInputs []*dynamodb.BatchGetItemInput
		batchSize := *d.batchSize
		for i := 0; i < len(entityIds); i += batchSize {
			end := i + batchSize
			if end > len(entityIds) {
				end = len(entityIds)
			}
			batchEntityIds := entityIds[i:end]
			entityIdBatch := make([]map[string]dtypes.AttributeValue, len(batchEntityIds))
			for i, entityId := range batchEntityIds {
				entityIdBatch[i] = map[string]dtypes.AttributeValue{
					"entity_id": &dtypes.AttributeValueMemberS{Value: entityId},
				}
			}
			batchGetItemInput := &dynamodb.BatchGetItemInput{
				RequestItems: map[string]dtypes.KeysAndAttributes{
					tableName: {
						Keys:           entityIdBatch,
						ConsistentRead: d.consistentRead,
					},
				},
			}
			batchGetItemInputs = append(batchGetItemInputs, batchGetItemInput)
		}

		// goroutines sending requests to DynamoDB
		errGroup, ctx := errgroup.WithContext(ctx)
		for i, batchGetItemInput := range batchGetItemInputs {
			_, batchGetItemInput := i, batchGetItemInput
			errGroup.Go(func() error {
				if err := sem.Acquire(ctx, 1); err != nil {
					return err
				}
				defer sem.Release(1)

				resp, err := d.client.BatchGetItem(ctx, batchGetItemInput)
				if err != nil {
					return err
				}

				// in case there is no entity id of a feature view in dynamodb
				batchSize := len(resp.Responses[tableName])
				if batchSize == 0 {
					return nil
				}

				// process response from dynamodb
				for j := 0; j < batchSize; j++ {
					entityId := resp.Responses[tableName][j]["entity_id"].(*dtypes.AttributeValueMemberS).Value
					timestampString := resp.Responses[tableName][j]["event_ts"].(*dtypes.AttributeValueMemberS).Value
					t, err := time.Parse("2006-01-02 15:04:05-07:00", timestampString)
					if err != nil {
						return err
					}
					timeStamp := timestamppb.New(t)

					featureValues := resp.Responses[tableName][j]["values"].(*dtypes.AttributeValueMemberM).Value
					entityIndex := entityIndexMap[entityId]

					for _, featureName := range featureNames {
						featureValue := featureValues[featureName].(*dtypes.AttributeValueMemberB).Value
						var value types.Value
						if err := proto.Unmarshal(featureValue, &value); err != nil {
							return err
						}
						featureIndex := featureNamesIndex[featureName]

						mu.Lock()
						results[entityIndex][featureIndex] = FeatureData{Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
							Timestamp: timestamppb.Timestamp{Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos},
							Value:     types.Value{Val: value.Val},
						}
						mu.Unlock()
					}

					mu.Lock()
					delete(unprocessedEntityIds, entityId)
					mu.Unlock()
				}
				return nil
			})
		}
		if err := errGroup.Wait(); err != nil {
			return nil, err
		}

		// process null imputation for entity ids that don't exist in dynamodb
		currentTime := timestamppb.Now() // TODO: should use a different timestamp?
		for entityId, _ := range unprocessedEntityIds {
			entityIndex := entityIndexMap[entityId]
			for _, featureName := range featureNames {
				featureIndex := featureNamesIndex[featureName]
				results[entityIndex][featureIndex] = FeatureData{Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
					Timestamp: timestamppb.Timestamp{Seconds: currentTime.Seconds, Nanos: currentTime.Nanos},
					Value:     types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
				}
			}
		}
	}

	return results, nil
}

func (d *DynamodbOnlineStore) Destruct() {

}

func makeFeatureMeta(featureViewNames []string, featureNames []string) (map[string][]string, map[string]int, error) {
	if len(featureViewNames) != len(featureNames) {
		return nil, nil, fmt.Errorf("the lengths of featureViewNames and featureNames must be the same. got=%d, %d", len(featureViewNames), len(featureNames))
	}
	featureMap := make(map[string][]string)
	featureNamesIndex := make(map[string]int)
	for i := 0; i < len(featureViewNames); i++ {
		featureViewName := featureViewNames[i]
		featureName := featureNames[i]

		featureMap[featureViewName] = append(featureMap[featureViewName], featureName)
		featureNamesIndex[featureName] = i
	}
	return featureMap, featureNamesIndex, nil
}
