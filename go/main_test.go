package main

import (
	"testing"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockServerStarter is a mock of ServerStarter interface for testing
type MockServerStarter struct {
	mock.Mock
}

func (m *MockServerStarter) StartHttpServer(fs *feast.FeatureStore, host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts *logging.LoggingOptions) error {
	args := m.Called(fs, host, port, writeLoggedFeaturesCallback, loggingOpts)
	return args.Error(0)
}

func (m *MockServerStarter) StartGrpcServer(fs *feast.FeatureStore, host string, port int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts *logging.LoggingOptions) error {
	args := m.Called(fs, host, port, writeLoggedFeaturesCallback, loggingOpts)
	return args.Error(0)
}

// TestStartHttpServer tests the StartHttpServer function
func TestStartHttpServer(t *testing.T) {
	mockServerStarter := new(MockServerStarter)
	fs := &feast.FeatureStore{}
	host := "localhost"
	port := 8080
	var writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback

	loggingOpts := &logging.LoggingOptions{}

	mockServerStarter.On("StartHttpServer", fs, host, port, mock.AnythingOfType("logging.OfflineStoreWriteCallback"), loggingOpts).Return(nil)

	err := mockServerStarter.StartHttpServer(fs, host, port, writeLoggedFeaturesCallback, loggingOpts)
	assert.NoError(t, err)
	mockServerStarter.AssertExpectations(t)
}

// TestStartGrpcServer tests the StartGrpcServer function
func TestStartGrpcServer(t *testing.T) {
	mockServerStarter := new(MockServerStarter)
	fs := &feast.FeatureStore{}
	host := "localhost"
	port := 9090
	var writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback
	loggingOpts := &logging.LoggingOptions{}

	mockServerStarter.On("StartGrpcServer", fs, host, port, mock.AnythingOfType("logging.OfflineStoreWriteCallback"), loggingOpts).Return(nil)

	err := mockServerStarter.StartGrpcServer(fs, host, port, writeLoggedFeaturesCallback, loggingOpts)
	assert.NoError(t, err)
	mockServerStarter.AssertExpectations(t)
}

// TestConstructLoggingService tests the constructLoggingService function
func TestConstructLoggingService(t *testing.T) {
	fs := &feast.FeatureStore{}
	var writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback
	loggingOpts := &logging.LoggingOptions{}

	_, err := constructLoggingService(fs, writeLoggedFeaturesCallback, loggingOpts)
	assert.NoError(t, err)
	// Further assertions can be added here based on the expected behavior of constructLoggingService
}

// Note: Additional tests can be written for other functions and error scenarios.
