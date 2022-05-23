package logging

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type Log struct {
	// Example: val{int64_val: 5017}, val{int64_val: 1003}
	EntityValue []*types.Value
	RequestData []*types.Value

	FeatureValues   []*types.Value
	FeatureStatuses []serving.FieldStatus
	EventTimestamps []*timestamppb.Timestamp

	RequestId    string
	LogTimestamp time.Time
}

type LogSink interface {
	// Write is used to unload logs from memory buffer.
	// Logs are not guaranteed to be flushed to sink on this point.
	// The data can just be written to local disk (depending on implementation).
	Write(data []arrow.Record) error

	// Flush actually send data to a sink.
	// We want to control amount to interaction with sink, since it could be a costly operation.
	// Also, some sinks like BigQuery might have quotes and physically limit amount of write requests per day.
	Flush(featureServiceName string) error
}

type Logger interface {
	Log(joinKeyToEntityValues map[string]*types.RepeatedValue, featureVectors []*serving.GetOnlineFeaturesResponse_FeatureVector, featureNames []string, requestData map[string]*types.RepeatedValue, requestId string) error
}

type LoggerImpl struct {
	featureServiceName string

	buffer *MemoryBuffer
	schema *FeatureServiceSchema

	logCh    chan *Log
	signalCh chan interface{}

	sink   LogSink
	config LoggerConfig

	isStopped bool
	cond      *sync.Cond
}

type LoggerConfig struct {
	LoggingOptions

	SampleRate float32
}

func NewLoggerConfig(sampleRate float32, opts LoggingOptions) LoggerConfig {
	return LoggerConfig{
		LoggingOptions: opts,
		SampleRate:     sampleRate,
	}
}

func NewLogger(schema *FeatureServiceSchema, featureServiceName string, sink LogSink, config LoggerConfig) (*LoggerImpl, error) {
	buffer, err := NewMemoryBuffer(schema)
	if err != nil {
		return nil, err
	}
	logger := &LoggerImpl{
		featureServiceName: featureServiceName,

		logCh:    make(chan *Log, config.ChannelCapacity),
		signalCh: make(chan interface{}, 2),
		sink:     sink,

		buffer: buffer,
		schema: schema,
		config: config,

		isStopped: false,
		cond:      sync.NewCond(&sync.Mutex{}),
	}

	logger.startLoggerLoop()
	return logger, nil
}

func (l *LoggerImpl) EmitLog(log *Log) error {
	select {
	case l.logCh <- log:
		return nil
	case <-time.After(l.config.EmitTimeout):
		return fmt.Errorf("could not add to log channel with capacity %d. Operation timed out. Current log channel length is %d", cap(l.logCh), len(l.logCh))
	}
}

func (l *LoggerImpl) startLoggerLoop() {
	go func() {
		for {
			if err := l.loggerLoop(); err != nil {
				log.Printf("LoggerImpl[%s] recovered from panic: %+v", l.featureServiceName, err)

				// Sleep for a couple of milliseconds to avoid CPU load from a potential infinite panic-recovery loop
				time.Sleep(5 * time.Millisecond)
				continue // try again
			}

			// graceful stop
			return
		}
	}()
}

// Select that either ingests new logs that are added to the logging channel, one at a time to add
// to the in-memory buffer or flushes all of them synchronously to the OfflineStorage on a time interval.
func (l *LoggerImpl) loggerLoop() (lErr error) {
	defer func() {
		// Recover from panic in the logger loop, so that it doesn't bring down the entire feature server
		if r := recover(); r != nil {
			rErr, ok := r.(error)
			if !ok {
				rErr = fmt.Errorf("%v", r)
			}
			lErr = errors.WithStack(rErr)
		}
	}()

	writeTicker := time.NewTicker(l.config.WriteInterval)
	flushTicker := time.NewTicker(l.config.FlushInterval)

	for {
		shouldStop := false

		select {
		case <-l.signalCh:
			err := l.buffer.writeBatch(l.sink)
			if err != nil {
				log.Printf("Log write failed: %+v", err)
			}
			err = l.sink.Flush(l.featureServiceName)
			if err != nil {
				log.Printf("Log flush failed: %+v", err)
			}
			shouldStop = true
		case <-writeTicker.C:
			err := l.buffer.writeBatch(l.sink)
			if err != nil {
				log.Printf("Log write failed: %+v", err)
			}
		case <-flushTicker.C:
			err := l.sink.Flush(l.featureServiceName)
			if err != nil {
				log.Printf("Log flush failed: %+v", err)
			}
		case logItem := <-l.logCh:
			err := l.buffer.Append(logItem)
			if err != nil {
				log.Printf("Append log failed: %+v", err)
			}
		}

		if shouldStop {
			break
		}
	}

	writeTicker.Stop()
	flushTicker.Stop()

	// Notify all waiters for graceful stop
	l.cond.L.Lock()
	l.isStopped = true
	l.cond.Broadcast()
	l.cond.L.Unlock()
	return nil
}

// Stop the loop goroutine gracefully
func (l *LoggerImpl) Stop() {
	select {
	case l.signalCh <- nil:
	default:
	}
}

func (l *LoggerImpl) WaitUntilStopped() {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()
	for !l.isStopped {
		l.cond.Wait()
	}
}

func getFullFeatureName(featureViewName string, featureName string) string {
	return fmt.Sprintf("%s__%s", featureViewName, featureName)
}

func (l *LoggerImpl) Log(joinKeyToEntityValues map[string]*types.RepeatedValue, featureVectors []*serving.GetOnlineFeaturesResponse_FeatureVector, featureNames []string, requestData map[string]*types.RepeatedValue, requestId string) error {
	if len(featureVectors) == 0 {
		return nil
	}

	if rand.Float32() > l.config.SampleRate {
		return nil
	}

	numFeatures := len(l.schema.Features)
	// Should be equivalent to how many entities there are(each feature row has (entity) number of features)
	numRows := len(featureVectors[0].Values)

	featureNameToVectorIdx := make(map[string]int)
	for idx, name := range featureNames {
		featureNameToVectorIdx[name] = idx
	}

	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		featureValues := make([]*types.Value, numFeatures)
		featureStatuses := make([]serving.FieldStatus, numFeatures)
		eventTimestamps := make([]*timestamppb.Timestamp, numFeatures)

		for idx, featureName := range l.schema.Features {
			featureIdx, ok := featureNameToVectorIdx[featureName]
			if !ok {
				featureNameParts := strings.Split(featureName, "__")
				featureIdx, ok = featureNameToVectorIdx[featureNameParts[1]]
				if !ok {
					return errors.Errorf("Missing feature %s in log data", featureName)
				}
			}
			featureValues[idx] = featureVectors[featureIdx].Values[rowIdx]
			featureStatuses[idx] = featureVectors[featureIdx].Statuses[rowIdx]
			eventTimestamps[idx] = featureVectors[featureIdx].EventTimestamps[rowIdx]
		}

		entityValues := make([]*types.Value, len(l.schema.JoinKeys))
		for idx, joinKey := range l.schema.JoinKeys {
			rows, ok := joinKeyToEntityValues[joinKey]
			if !ok {
				return errors.Errorf("Missing join key %s in log data", joinKey)
			}
			entityValues[idx] = rows.Val[rowIdx]
		}

		requestDataValues := make([]*types.Value, len(l.schema.RequestData))
		for idx, requestParam := range l.schema.RequestData {
			rows, ok := requestData[requestParam]
			if !ok {
				return errors.Errorf("Missing request parameter %s in log data", requestParam)
			}
			requestDataValues[idx] = rows.Val[rowIdx]
		}

		newLog := Log{
			EntityValue: entityValues,
			RequestData: requestDataValues,

			FeatureValues:   featureValues,
			FeatureStatuses: featureStatuses,
			EventTimestamps: eventTimestamps,

			RequestId:    requestId,
			LogTimestamp: time.Now().UTC(),
		}
		err := l.EmitLog(&newLog)
		if err != nil {
			return err
		}
	}
	return nil
}

type DummyLoggerImpl struct{}

func (l *DummyLoggerImpl) Log(joinKeyToEntityValues map[string]*types.RepeatedValue, featureVectors []*serving.GetOnlineFeaturesResponse_FeatureVector, featureNames []string, requestData map[string]*types.RepeatedValue, requestId string) error {
	return nil
}
