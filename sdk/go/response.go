package feast

import (
	"fmt"
	"github.com/gojek/feast/sdk/go/protos/feast/serving"
	"github.com/gojek/feast/sdk/go/protos/feast/types"
)

var (
	// ErrLengthMismatch indicates that the number of values returned is not the same as the number of values requested
	ErrLengthMismatch = "Length mismatch; number of na values (%d) not equal to number of features requested (%d)."

	// ErrFeatureNotFound indicates that the a requested feature was not found in the response
	ErrFeatureNotFound = "Feature %s not found in response."

	// ErrTypeMismatch indicates that the there was a type mismatch in the returned values
	ErrTypeMismatch = "Requested output of type %s does not match type of feature value returned."
)

// OnlineFeaturesResponse is a wrapper around serving.GetOnlineFeaturesResponse.
type OnlineFeaturesResponse struct {
	RawResponse *serving.GetOnlineFeaturesResponse
}

// Rows retrieves the result of the request as a list of Rows.
func (r OnlineFeaturesResponse) Rows() []Row {
	rows := make([]Row, len(r.RawResponse.FieldValues))
	for i, val := range r.RawResponse.FieldValues {
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
	for i, val := range r.RawResponse.FieldValues {
		rows[i] = make([]int64, len(order))
		for j, fname := range order {
			fValue, exists := val.Fields[fname]
			if !exists {
				return nil, fmt.Errorf(ErrFeatureNotFound, fname)
			}
			val := fValue.GetVal()
			if val == nil {
				rows[i][j] = fillNa[j]
			} else if int64Val, ok := val.(*types.Value_Int64Val); ok {
				rows[i][j] = int64Val.Int64Val
			} else {
				return nil, fmt.Errorf(ErrTypeMismatch, "int64")
			}
		}
	}
	return rows, nil
}

// Float64Arrays retrieves the result of the request as a list of float64 slices. Any missing values will be filled
// with the missing values provided.
func (r OnlineFeaturesResponse) Float64Arrays(order []string, fillNa []float64) ([][]float64, error) {
	rows := make([][]float64, len(r.RawResponse.FieldValues))
	if len(fillNa) != len(order) {
		return nil, fmt.Errorf(ErrLengthMismatch, len(fillNa), len(order))
	}
	for i, val := range r.RawResponse.FieldValues {
		rows[i] = make([]float64, len(order))
		for j, fname := range order {
			fValue, exists := val.Fields[fname]
			if !exists {
				return nil, fmt.Errorf(ErrFeatureNotFound, fname)
			}
			val := fValue.GetVal()
			if val == nil {
				rows[i][j] = fillNa[j]
			} else if doubleVal, ok := val.(*types.Value_DoubleVal); ok {
				rows[i][j] = doubleVal.DoubleVal
			} else {
				return nil, fmt.Errorf(ErrTypeMismatch, "float64")
			}
		}
	}
	return rows, nil
}
