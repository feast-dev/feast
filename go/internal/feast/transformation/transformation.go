package transformation

import (
	"context"
	"github.com/feast-dev/feast/go/internal/feast/errors"
	"runtime"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/rs/zerolog/log"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
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
	ctx context.Context,
	onDemandFeatureViews []*model.OnDemandFeatureView,
	requestData map[string]*prototypes.RepeatedValue,
	entityRows map[string]*prototypes.RepeatedValue,
	features []*onlineserving.FeatureVector,
	transformationCallback TransformationCallback,
	transformationService *GrpcTransformationService,
	arrowMemory memory.Allocator,
	numRows int,
	fullFeatureNames bool,

) ([]*onlineserving.FeatureVector, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "transformation.AugmentResponseWithOnDemandTransforms")
	defer span.Finish()

	result := make([]*onlineserving.FeatureVector, 0)
	var err error

	for _, odfv := range onDemandFeatureViews {
		requestContextArrow := make(map[string]arrow.Array)
		for name, values := range requestData {
			requestContextArrow[name], err = types.ProtoValuesToArrowArray(values.Val, arrowMemory, numRows)
			if err != nil {
				ReleaseArrowContext(requestContextArrow)
				return nil, err
			}
		}

		for name, values := range entityRows {
			requestContextArrow[name], err = types.ProtoValuesToArrowArray(values.Val, arrowMemory, numRows)
			if err != nil {
				ReleaseArrowContext(requestContextArrow)
				return nil, err
			}
		}

		retrievedFeatures := make(map[string]arrow.Array)
		for _, vector := range features {
			retrievedFeatures[vector.Name] = vector.Values
		}

		var onDemandFeatures []*onlineserving.FeatureVector
		if transformationService != nil {
			onDemandFeatures, err = transformationService.GetTransformation(
				ctx,
				odfv,
				retrievedFeatures,
				requestContextArrow,
				numRows,
				fullFeatureNames,
			)
			if err != nil {
				ReleaseArrowContext(requestContextArrow)
				return nil, err
			}
		}
		result = append(result, onDemandFeatures...)

		ReleaseArrowContext(requestContextArrow)
	}

	return result, nil
}

func ReleaseArrowContext(requestContextArrow map[string]arrow.Array) {
	// Release memory used by requestContextArrow
	for _, arrowArray := range requestContextArrow {
		arrowArray.Release()
	}
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
		return errors.GrpcInvalidArgumentErrorf("requestDataNotFoundInEntityRowsException: %s", strings.Join(missingFeatures, ", "))
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

func logStackTrace() {
	// Create a buffer for storing the stack trace
	const size = 4096
	buf := make([]byte, size)

	// Retrieve the stack trace and write it to the buffer
	stackSize := runtime.Stack(buf, false)

	// Log the stack trace using zerolog
	log.Error().Str("stack_trace", string(buf[:stackSize])).Msg("")
}
