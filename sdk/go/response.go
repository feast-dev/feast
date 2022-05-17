package feast

import (
	"fmt"
	"github.com/feast-dev/feast/sdk/go/protos/feast/serving"
	"github.com/feast-dev/feast/sdk/go/protos/feast/types"
)

var (
	// ErrLengthMismatch indicates that the number of values returned is not the same as the number of values requested
	ErrLengthMismatch = "Length mismatch; number of na values (%d) not equal to number of features requested (%d)."

	// ErrFeatureNotFound indicates that the requested feature was not found in the response
	ErrFeatureNotFound = "Feature %s not found in response."

	// ErrTypeMismatch indicates that the there was a type mismatch in the returned values
	ErrTypeMismatch = "Requested output of type %s does not match type of feature value returned."
)

// internal func to find index of element in array
func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 // element not found
}

// OnlineFeaturesResponse is a wrapper around serving.GetOnlineFeaturesResponseV2.
type OnlineFeaturesResponse struct {
	RawResponse *serving.GetOnlineFeaturesResponseV2
}

// Rows retrieves the result of the request as a list of Rows.
func (r OnlineFeaturesResponse) Rows() []Row {
	rows := make([]Row, len(r.RawResponse.Results))
	fieldNames := r.RawResponse.Metadata.FieldNames.Val
	for i, fieldVector := range r.RawResponse.Results {
		row := make(Row)
		for idx, fieldName := range fieldNames {
			row[fieldName] = fieldVector.Values[idx]
		}
		rows[i] = row
	}
	return rows
}

// Statuses retrieves field level status metadata for each row in Rows().
// Each status map returned maps status 1:1 to each returned row from Rows()
func (r OnlineFeaturesResponse) Statuses() []map[string]serving.FieldStatus {
	statuses := make([]map[string]serving.FieldStatus, len(r.RawResponse.Results))
	fieldNames := r.RawResponse.Metadata.FieldNames.Val
	for i, fieldVector := range r.RawResponse.Results {
		status := make(map[string]serving.FieldStatus)
		for idx, fieldName := range fieldNames {
			status[fieldName] = fieldVector.Statuses[idx]
		}
		statuses[i] = status
	}
	return statuses
}

// Int64Arrays retrieves the result of the request as a list of int64 slices. Any missing values will be filled
// with the missing values provided.
func (r OnlineFeaturesResponse) Int64Arrays(order []string, fillNa []int64) ([][]int64, error) {
	rows := make([][]int64, len(r.RawResponse.Results))
	if len(fillNa) != len(order) {
		return nil, fmt.Errorf(ErrLengthMismatch, len(fillNa), len(order))
	}
	fieldNames := r.RawResponse.Metadata.FieldNames.Val
	for i, fieldVector := range r.RawResponse.Results {
		rows[i] = make([]int64, len(order))
		for j, fname := range order {
			findex := indexOf(fname, fieldNames)
			if findex == -1 {
				return nil, fmt.Errorf(ErrFeatureNotFound, fname)
			}
			value := fieldVector.Values[findex]
			valType := value.GetVal()
			if valType == nil {
				rows[i][j] = fillNa[j]
			} else if int64Val, ok := valType.(*types.Value_Int64Val); ok {
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
	rows := make([][]float64, len(r.RawResponse.Results))
	if len(fillNa) != len(order) {
		return nil, fmt.Errorf(ErrLengthMismatch, len(fillNa), len(order))
	}
	fieldNames := r.RawResponse.Metadata.FieldNames.Val
	for i, fieldVector := range r.RawResponse.Results {
		rows[i] = make([]float64, len(order))
		for j, fname := range order {
			findex := indexOf(fname, fieldNames)
			if findex == -1 {
				return nil, fmt.Errorf(ErrFeatureNotFound, fname)
			}
			value := fieldVector.Values[findex]
			valType := value.GetVal()
			if valType == nil {
				rows[i][j] = fillNa[j]
			} else if doubleVal, ok := valType.(*types.Value_DoubleVal); ok {
				rows[i][j] = doubleVal.DoubleVal
			} else {
				return nil, fmt.Errorf(ErrTypeMismatch, "float64")
			}
		}
	}
	return rows, nil
}
