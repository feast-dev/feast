package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLoggingChannelToMemoryBuffer(t *testing.T) {
	//Feature store is still not checked in so we can't create one.
	loggingService := NewLoggingService(nil, 10, true)
	assert.Empty(t, loggingService.memoryBuffer.logs)
	ts := timestamppb.New(time.Now())
	newLog := Log{
		featureNames:    []string{"feature1", "feature2"},
		featureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		eventTimestamps: []*timestamppb.Timestamp{ts},
	}
	err := loggingService.emitLog(&newLog)
	// Wait for memory buffer flush
	time.Sleep(20 * time.Millisecond)
	assert.Len(t, loggingService.memoryBuffer.logs, 1)
	assert.Len(t, loggingService.logChannel, 0)
	assert.True(t, reflect.DeepEqual(loggingService.memoryBuffer.logs[0].featureNames, []string{"feature1", "feature2"}))
	assert.True(t, reflect.DeepEqual(loggingService.memoryBuffer.logs[0].featureStatuses, []serving.FieldStatus{serving.FieldStatus_PRESENT}))
	assert.Nil(t, err)
}

func TestLoggingChannelTiemout(t *testing.T) {
	//Feature store is still not checked in so we can't create one.
	loggingService := NewLoggingService(nil, 1, false)
	assert.Empty(t, loggingService.memoryBuffer.logs)
	ts := timestamppb.New(time.Now())
	newLog := Log{
		featureNames:    []string{"feature1", "feature2"},
		featureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		eventTimestamps: []*timestamppb.Timestamp{ts, ts},
	}
	loggingService.emitLog(&newLog)
	// Wait for memory buffer flush
	time.Sleep(20 * time.Millisecond)
	newTs := timestamppb.New(time.Now())

	newLog2 := Log{
		featureNames:    []string{"feature4", "feature5"},
		featureStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		eventTimestamps: []*timestamppb.Timestamp{newTs, newTs},
	}
	err := loggingService.emitLog(&newLog2)
	time.Sleep(20 * time.Millisecond)
	assert.NotNil(t, err)
}
