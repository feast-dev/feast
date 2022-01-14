package feast

import (
	"fmt"
	"github.com/feast-dev/feast/sdk/go/protos/feast/serving"
	"github.com/feast-dev/feast/sdk/go/protos/feast/types"
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
	if len(r.RawResponse.Results) == 0 {
		return []Row{}
	}

	rowsCount := len(r.RawResponse.Results[0].Values)
	rows := make([]Row, rowsCount)
	for rowIdx := 0; rowIdx < rowsCount; rowIdx++ {
		row := make(map[string]*types.Value)
		for featureIdx := 0; featureIdx < len(r.RawResponse.Results); featureIdx++ {
			row[r.RawResponse.Metadata.FeatureNames.Val[featureIdx]] = r.RawResponse.Results[featureIdx].Values[rowIdx]
		}

		rows[rowIdx] = row
	}
	return rows
}

// Statuses retrieves field level status metadata for each row in Rows().
// Each status map returned maps status 1:1 to each returned row from Rows()
func (r OnlineFeaturesResponse) Statuses() []map[string]serving.FieldStatus {
	if len(r.RawResponse.Results) == 0 {
		return []map[string]serving.FieldStatus{}
	}

	rowsCount := len(r.RawResponse.Results[0].Statuses)
	rows := make([]map[string]serving.FieldStatus, rowsCount)

	for rowIdx := 0; rowIdx < rowsCount; rowIdx++ {
		row := make(map[string]serving.FieldStatus)
		for featureIdx := 0; featureIdx < len(r.RawResponse.Results); featureIdx++ {
			row[r.RawResponse.Metadata.FeatureNames.Val[featureIdx]] = r.RawResponse.Results[featureIdx].Statuses[rowIdx]
		}

		rows[rowIdx] = row
	}
	return rows
}

// Int64Arrays retrieves the result of the request as a list of int64 slices. Any missing values will be filled
// with the missing values provided.
func (r OnlineFeaturesResponse) Int64Arrays(order []string, fillNa []int64) ([][]int64, error) {
	if len(fillNa) != len(order) {
		return nil, fmt.Errorf(ErrLengthMismatch, len(fillNa), len(order))
	}

	if len(r.RawResponse.Results) == 0 {
		return [][]int64{}, nil
	}

	rowsCount := len(r.RawResponse.Results[0].Values)
	rows := make([][]int64, rowsCount)

	featureNameToIdx := make(map[string]int)

	for idx, featureName := range r.RawResponse.Metadata.FeatureNames.Val {
		featureNameToIdx[featureName] = idx
	}

	for rowIdx := 0; rowIdx < rowsCount; rowIdx++ {
		row := make([]int64, len(order))
		for idx, feature := range order {
			featureIdx, exists := featureNameToIdx[feature]
			if !exists {
				return nil, fmt.Errorf(ErrFeatureNotFound, feature)
			}

			valType := r.RawResponse.Results[featureIdx].Values[rowIdx].GetVal()
			if valType == nil {
				row[idx] = fillNa[idx]
			} else if int64Val, ok := valType.(*types.Value_Int64Val); ok {
				row[idx] = int64Val.Int64Val
			} else {
				return nil, fmt.Errorf(ErrTypeMismatch, "int64")
			}
		}

		rows[rowIdx] = row
	}
	return rows, nil
}

// Float64Arrays retrieves the result of the request as a list of float64 slices. Any missing values will be filled
// with the missing values provided.
func (r OnlineFeaturesResponse) Float64Arrays(order []string, fillNa []float64) ([][]float64, error) {
	if len(fillNa) != len(order) {
		return nil, fmt.Errorf(ErrLengthMismatch, len(fillNa), len(order))
	}

	if len(r.RawResponse.Results) == 0 {
		return [][]float64{}, nil
	}

	rowsCount := len(r.RawResponse.Results[0].Values)
	rows := make([][]float64, rowsCount)

	featureNameToIdx := make(map[string]int)

	for idx, featureName := range r.RawResponse.Metadata.FeatureNames.Val {
		featureNameToIdx[featureName] = idx
	}

	for rowIdx := 0; rowIdx < rowsCount; rowIdx++ {
		row := make([]float64, len(order))
		for idx, feature := range order {
			featureIdx, exists := featureNameToIdx[feature]
			if !exists {
				return nil, fmt.Errorf(ErrFeatureNotFound, feature)
			}

			valType := r.RawResponse.Results[featureIdx].Values[rowIdx].GetVal()
			if valType == nil {
				row[idx] = fillNa[idx]
			} else if doubleVal, ok := valType.(*types.Value_DoubleVal); ok {
				row[idx] = doubleVal.DoubleVal
			} else {
				return nil, fmt.Errorf(ErrTypeMismatch, "float64")
			}
		}

		rows[rowIdx] = row
	}
	return rows, nil
}
