package logging

import (
	"testing"
	"time"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLoggingChannelTimeout(t *testing.T) {
	loggingService, err := NewLoggingService(nil, 1, false)
	assert.Nil(t, err)
	assert.Empty(t, loggingService.memoryBuffer.logs)
	ts := timestamppb.New(time.Now())
	newLog := Log{
		FeatureNames:    []string{"feature1", "feature2"},
		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		EventTimestamps: []*timestamppb.Timestamp{ts, ts},
	}
	loggingService.EmitLog(&newLog)
	// Wait for memory buffer flush
	time.Sleep(20 * time.Millisecond)
	newTs := timestamppb.New(time.Now())

	newLog2 := Log{
		FeatureNames:    []string{"feature4", "feature5"},
		FeatureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		EventTimestamps: []*timestamppb.Timestamp{newTs, newTs},
	}
	err = loggingService.EmitLog(&newLog2)
	// The channel times out and doesn't hang.
	time.Sleep(20 * time.Millisecond)
	assert.NotNil(t, err)
}
