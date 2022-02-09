package main

import (
	"net/http"
	// "encoding/json"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/stretchr/testify/assert"
	"testing"
	"fmt"
	"io"
	"bytes"
	"google.golang.org/protobuf/encoding/protojson"
)

type reqHttp struct {
	Features []string
	Entities map[string][]int64
}

func TestServerHttp(t *testing.T) {
	featureViewNames := []string{"driver_hourly_stats:conv_rate",
		"driver_hourly_stats:acc_rate",
		"driver_hourly_stats:avg_daily_trips"}
	featureList := serving.FeatureList{Val: featureViewNames}
	featureListRequest := serving.GetOnlineFeaturesRequest_Features{Features: &featureList}
	entities := map[string]*types.RepeatedValue{"driver_id": {Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}},
		{Val: &types.Value_Int64Val{Int64Val: 1002}},
		{Val: &types.Value_Int64Val{Int64Val: 1003}}}}}
	request := serving.GetOnlineFeaturesRequest{Kind: &featureListRequest, Entities: entities, FullFeatureNames: true}
	// request := {	"Kind" : {
	// 					"Features" : {
	// 						"Val" : []string{	"driver_hourly_stats:conv_rate",
	// 											"driver_hourly_stats:acc_rate",
	// 											"driver_hourly_stats:avg_daily_trips"}
	// 					}
	// 				},
	// 				"Entities" : {
	// 					"driver_id": [1001, 1002, 1003]
	// 				},
	// 			}
	// request := reqHttp{ Features: []string{	"driver_hourly_stats:conv_rate",
	// 								"driver_hourly_stats:acc_rate",
	// 								"driver_hourly_stats:avg_daily_trips"},
	// 					Entities: map[string][]int64{"driver_id": []int64{1001, 1002, 1003} }}
	// requestBody, err := json.Marshal(request)
	requestBody, err := protojson.Marshal(&request)
	fmt.Println(string(requestBody))
	assert.Nil(t, err)
	resp, err := http.Post("http://localhost:8081/get-online-features", "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		panic(err)
	}
	assert.Nil(t, err)
	defer resp.Body.Close()
	fmt.Println("response status", resp.StatusCode)
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		assert.Nil(t, err)
		bodyString := string(bodyBytes)
		fmt.Println(bodyString)
		var response serving.GetOnlineFeaturesResponse
		if err = protojson.Unmarshal(bodyBytes, &response); err != nil {
			// panic(err)
		} else {
			// for _, featureVector := range response.Results {

			// 	values := featureVector.GetValues()
			// 	statuses := featureVector.GetStatuses()
			// 	timestamps := featureVector.GetEventTimestamps()
			// 	lenValues := len(values)
			// 	for i := 0; i < lenValues; i++ {
			// 		fmt.Println(values[i].String(), statuses[i], timestamps[i].String())
			// 	}
			// }
			fmt.Println("Passed server_http_test")
		}
	} else {
		fmt.Println("response status", resp.StatusCode)
	}
	
	assert.Nil(t, err)
}