package logging

import (
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/feast-dev/feast/go/internal/feast/model"
)

type FeatureStore interface {
	GetFcosMap() (map[string]*model.Entity, map[string]*model.FeatureView, map[string]*model.OnDemandFeatureView, error)
	GetFeatureService(name string) (*model.FeatureService, error)
}

type LoggingOptions struct {
	// How many log items can be buffered in channel
	ChannelCapacity int

	// Waiting time when inserting new log into the channel
	EmitTimeout time.Duration

	// Interval on which logs buffered in memory will be written to sink
	WriteInterval time.Duration

	// Interval on which sink will be flushed
	// (see LogSink interface for better explanation on differences with Write)
	FlushInterval time.Duration
}

type LoggingService struct {
	// feature service name -> LoggerImpl
	loggers map[string]*LoggerImpl

	fs   FeatureStore
	sink LogSink
	opts LoggingOptions

	creationLock *sync.Mutex
}

var (
	DefaultOptions = LoggingOptions{
		ChannelCapacity: 100000,
		FlushInterval:   10 * time.Minute,
		WriteInterval:   10 * time.Second,
		EmitTimeout:     10 * time.Millisecond,
	}
)

func NewLoggingService(fs FeatureStore, sink LogSink, opts ...LoggingOptions) (*LoggingService, error) {
	if len(opts) == 0 {
		opts = append(opts, DefaultOptions)
	}

	return &LoggingService{
		fs:           fs,
		loggers:      make(map[string]*LoggerImpl),
		sink:         sink,
		opts:         opts[0],
		creationLock: &sync.Mutex{},
	}, nil
}

func (s *LoggingService) GetOrCreateLogger(featureService *model.FeatureService) (Logger, error) {
	if logger, ok := s.loggers[featureService.Name]; ok {
		return logger, nil
	}

	if featureService.LoggingConfig == nil {
		return nil, errors.New("Only feature services with configured logging can be used")
	}

	s.creationLock.Lock()
	defer s.creationLock.Unlock()

	// could be created by another go-routine on this point
	if logger, ok := s.loggers[featureService.Name]; ok {
		return logger, nil
	}

	if s.sink == nil {
		return &DummyLoggerImpl{}, nil
	}

	config := NewLoggerConfig(featureService.LoggingConfig.SampleRate, s.opts)
	schema, err := GenerateSchemaFromFeatureService(s.fs, featureService.Name)
	if err != nil {
		return nil, err
	}

	logger, err := NewLogger(schema, featureService.Name, s.sink, config)
	if err != nil {
		return nil, err
	}
	s.loggers[featureService.Name] = logger

	return logger, nil
}

func (s *LoggingService) Stop() {
	for _, logger := range s.loggers {
		logger.Stop()
		logger.WaitUntilStopped()
	}
}
