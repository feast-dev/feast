package feast

import (
	"context"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/protos/feast/third_party/grpc/connector"
)

// GRPCClient is an implementation of KV that talks over RPC.
type GRPCClient struct{ client connector.OnlineStoreClient
						destructor func()	}

func (m *GRPCClient) OnlineRead(entityKeys []types.EntityKey, view string, features []string) ([][]FeatureData, error) {
	entityKeysRef := make([]*types.EntityKey, len(entityKeys))
	for i := 0; i < len(entityKeys); i++ {
		entityKeysRef[i] = &entityKeys[i]
	}
	results, err := m.client.OnlineRead(context.Background(), &connector.OnlineReadRequest{
		EntityKeys:   entityKeysRef,
		View: view,
		Features: features,
	})
	if err != nil {
		return nil, err
	}
	feature2D := results.GetResults()
	featureResults := make([][]FeatureData, len(feature2D))
	for entityIndex, featureList := range feature2D {
		connectorList := featureList.GetFeatureList()
		featureResults[entityIndex] = make([]FeatureData, len(connectorList))
		for featureIndex, feature := range connectorList {
			featureResults[entityIndex][featureIndex] = FeatureData{ 	reference: *feature.GetReference(),
																		timestamp: *feature.GetTimestamp(),
																		value: *feature.GetValue() }
		}
	}
	return featureResults, nil
}

func (m *GRPCClient) Destruct() {
	m.destructor()
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	// This is the real implementation
	Impl OnlineStore
	connector.UnimplementedOnlineStoreServer
}

func (m *GRPCServer) OnlineRead(
	ctx context.Context,
	req *connector.OnlineReadRequest) (*connector.OnlineReadResponse, error) {
	numEntityKeys := len(req.EntityKeys)
	entityKeys := make([]types.EntityKey, numEntityKeys)
	for i := 0; i < numEntityKeys; i++ {
		entityKeys[i] = *req.EntityKeys[i]
	}
	features, err := m.Impl.OnlineRead(entityKeys, req.View, req.Features)
	if err != nil {
		return nil, err
	}
	response := connector.OnlineReadResponse{Results: make([]*connector.ConnectorFeatureList, len(features))}

	for entityIndex, featureList := range features {
		response.Results[entityIndex] = &connector.ConnectorFeatureList{FeatureList: make([]*connector.ConnectorFeature, len(featureList))}
		for featureIndex, feature := range featureList {
			reference := feature.reference
			value := feature.value
			timestamp := feature.timestamp
			response.Results[entityIndex].FeatureList[featureIndex] = &connector.ConnectorFeature{	Reference: &reference,
																									Value:	&value,
																									Timestamp: &timestamp}
		}
	}
	return &response, nil
}