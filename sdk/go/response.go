package feast

import (
	"github.com/gojek/feast/sdk/go/protos/feast/serving"
)

type OnlineFeaturesResponse struct {
	Features  []string
	RawResponse *serving.GetOnlineFeaturesResponse
}

func (res OnlineFeaturesResponse) ToInt64Array(missingVals map[string]int64) {
	// convert the values here
	//for _, fields := range res.FieldOrder {
	//
	//}
}