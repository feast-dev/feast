package transformation

import (
	"errors"
	"fmt"
	"strings"
	"unsafe"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/cdata"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
)

/*
TransformationCallback is a Python callback function's expected signature.
The function should accept name of the on demand feature view and pointers to input & output record batches.
Each record batch is being passed as two pointers: pointer to array (data) and pointer to schema.
Python function is expected to return number of rows added to the output record batch.
*/
type TransformationCallback func(ODFVName string, inputArrPtr, inputSchemaPtr, outArrPtr, outSchemaPtr uintptr, fullFeatureNames bool) int

func AugmentResponseWithOnDemandTransforms(
	onDemandFeatureViews []*model.OnDemandFeatureView,
	requestData map[string]*prototypes.RepeatedValue,
	entityRows map[string]*prototypes.RepeatedValue,
	features []*onlineserving.FeatureVector,
	transformationCallback TransformationCallback,
	arrowMemory memory.Allocator,
	numRows int,
	fullFeatureNames bool,

) ([]*onlineserving.FeatureVector, error) {
	result := make([]*onlineserving.FeatureVector, 0)
	var err error

	for _, odfv := range onDemandFeatureViews {
		requestContextArrow := make(map[string]arrow.Array)
		for name, values := range requestData {
			requestContextArrow[name], err = types.ProtoValuesToArrowArray(values.Val, arrowMemory, numRows)
			if err != nil {
				return nil, err
			}
		}

		for name, values := range entityRows {
			requestContextArrow[name], err = types.ProtoValuesToArrowArray(values.Val, arrowMemory, numRows)
			if err != nil {
				return nil, err
			}
		}

		retrievedFeatures := make(map[string]arrow.Array)
		for _, vector := range features {
			retrievedFeatures[vector.Name] = vector.Values
		}

		onDemandFeatures, err := CallTransformations(
			odfv,
			retrievedFeatures,
			requestContextArrow,
			transformationCallback,
			numRows,
			fullFeatureNames,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, onDemandFeatures...)
	}

	return result, nil
}

func CallTransformations(
	featureView *model.OnDemandFeatureView,
	retrievedFeatures map[string]arrow.Array,
	requestContext map[string]arrow.Array,
	callback TransformationCallback,
	numRows int,
	fullFeatureNames bool,
) ([]*onlineserving.FeatureVector, error) {

	inputArr := cdata.CArrowArray{}
	inputSchema := cdata.CArrowSchema{}

	outArr := cdata.CArrowArray{}
	outSchema := cdata.CArrowSchema{}

	defer cdata.ReleaseCArrowArray(&inputArr)
	defer cdata.ReleaseCArrowArray(&outArr)
	defer cdata.ReleaseCArrowSchema(&inputSchema)
	defer cdata.ReleaseCArrowSchema(&outSchema)

	inputArrPtr := uintptr(unsafe.Pointer(&inputArr))
	inputSchemaPtr := uintptr(unsafe.Pointer(&inputSchema))

	outArrPtr := uintptr(unsafe.Pointer(&outArr))
	outSchemaPtr := uintptr(unsafe.Pointer(&outSchema))

	inputFields := make([]arrow.Field, 0)
	inputColumns := make([]arrow.Array, 0)
	for name, arr := range retrievedFeatures {
		inputFields = append(inputFields, arrow.Field{Name: name, Type: arr.DataType()})
		inputColumns = append(inputColumns, arr)
	}
	for name, arr := range requestContext {
		inputFields = append(inputFields, arrow.Field{Name: name, Type: arr.DataType()})
		inputColumns = append(inputColumns, arr)
	}

	inputRecord := array.NewRecord(arrow.NewSchema(inputFields, nil), inputColumns, int64(numRows))
	defer inputRecord.Release()

	cdata.ExportArrowRecordBatch(inputRecord, &inputArr, &inputSchema)

	ret := callback(featureView.Base.Name, inputArrPtr, inputSchemaPtr, outArrPtr, outSchemaPtr, fullFeatureNames)

	if ret != numRows {
		return nil, errors.New("python transformation callback failed")
	}

	outRecord, err := cdata.ImportCRecordBatch(&outArr, &outSchema)
	if err != nil {
		return nil, err
	}

	result := make([]*onlineserving.FeatureVector, 0)
	for idx, field := range outRecord.Schema().Fields() {
		dropFeature := true

		if featureView.Base.Projection != nil {
			var featureName string
			if fullFeatureNames {
				featureName = strings.Split(field.Name, "__")[1]
			} else {
				featureName = field.Name
			}

			for _, feature := range featureView.Base.Projection.Features {
				if featureName == feature.Name {
					dropFeature = false
				}
			}
		} else {
			dropFeature = false
		}

		if dropFeature {
			continue
		}

		statuses := make([]serving.FieldStatus, numRows)
		timestamps := make([]*timestamppb.Timestamp, numRows)

		for idx := 0; idx < numRows; idx++ {
			statuses[idx] = serving.FieldStatus_PRESENT
			timestamps[idx] = timestamppb.Now()
		}

		result = append(result, &onlineserving.FeatureVector{
			Name:       field.Name,
			Values:     outRecord.Column(idx),
			Statuses:   statuses,
			Timestamps: timestamps,
		})
	}

	return result, nil
}

func EnsureRequestedDataExist(requestedOnDemandFeatureViews []*model.OnDemandFeatureView,
	requestDataFeatures map[string]*prototypes.RepeatedValue) error {

	neededRequestData, err := getNeededRequestData(requestedOnDemandFeatureViews)
	if err != nil {
		return err
	}
	missingFeatures := make([]string, 0)
	for feature := range neededRequestData {
		if _, ok := requestDataFeatures[feature]; !ok {
			missingFeatures = append(missingFeatures, feature)
		}
	}

	if len(missingFeatures) > 0 {
		return fmt.Errorf("requestDataNotFoundInEntityRowsException: %s", strings.Join(missingFeatures, ", "))
	}
	return nil
}

func getNeededRequestData(requestedOnDemandFeatureViews []*model.OnDemandFeatureView) (map[string]struct{}, error) {
	neededRequestData := make(map[string]struct{})

	for _, onDemandFeatureView := range requestedOnDemandFeatureViews {
		requestSchema := onDemandFeatureView.GetRequestDataSchema()
		for fieldName := range requestSchema {
			neededRequestData[fieldName] = struct{}{}
		}
	}

	return neededRequestData, nil
}
