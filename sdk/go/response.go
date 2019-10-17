package feast

import (
	"fmt"
	"github.com/gojek/feast/sdk/go/protos/feast/serving"
)

var (
	ErrLengthMismatch = "Length mismatch; number of na values (%d) not equal to number of features requested (%d)."
	ErrFeatureNotFound = "Feature %s not found in response."
)

// OnlineFeaturesResponse is a wrapper around serving.GetOnlineFeaturesResponse.
type OnlineFeaturesResponse struct {
	RawResponse *serving.GetOnlineFeaturesResponse
}

// Rows retrieves the result of the request as a list of Rows.
func (r OnlineFeaturesResponse) Rows() []Row {
	rows := make([]Row, len(r.RawResponse.FieldValues))
	for i, val :=  range r.RawResponse.FieldValues {
		rows[i] = val.Fields
	}
	return rows
}

// Int64Arrays retrieves the result of the request as a list of int64 slices. Any missing values will be filled
// with the missing values provided.
func (r OnlineFeaturesResponse) Int64Arrays(order []string, fillNa []int64) ([][]int64, error) {
	rows := make([][]int64, len(r.RawResponse.FieldValues))
	if len(fillNa) != len(order) {
		return nil, fmt.Errorf(ErrLengthMismatch, len(fillNa), len(order))
	}
	for i, val :=  range r.RawResponse.FieldValues {
		rows[i] = make([]int64, len(order))
		for j, fname := range order {
			fValue, exists := val.Fields[fname]
			if !exists {
				return nil, fmt.Errorf(ErrFeatureNotFound, fname)
			}
			if fValue.GetVal() == nil {
				rows[i][j] = fillNa[j]
			} else {
				rows[i][j] = fValue.GetInt64Val()
			}
		}
	}
	return rows, nil
}
