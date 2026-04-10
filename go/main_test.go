package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockServerStarter is a mock of ServerStarter interface for testing
type MockServerStarter struct {
	mock.Mock
}

func (m *MockServerStarter) StartHttpServer(fs *feast.FeatureStore, host string, port int, metricsPort int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts *logging.LoggingOptions) error {
	args := m.Called(fs, host, port, metricsPort, writeLoggedFeaturesCallback, loggingOpts)
	return args.Error(0)
}

func (m *MockServerStarter) StartGrpcServer(fs *feast.FeatureStore, host string, port int, metricsPort int, writeLoggedFeaturesCallback logging.OfflineStoreWriteCallback, loggingOpts *logging.LoggingOptions) error {
	args := m.Called(fs, host, port, metricsPort, writeLoggedFeaturesCallback, loggingOpts)
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

	mockServerStarter.On("StartHttpServer", fs, host, port, 9090, mock.AnythingOfType("logging.OfflineStoreWriteCallback"), loggingOpts).Return(nil)

	err := mockServerStarter.StartHttpServer(fs, host, port, 9090, writeLoggedFeaturesCallback, loggingOpts)
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

	mockServerStarter.On("StartGrpcServer", fs, host, port, 9090, mock.AnythingOfType("logging.OfflineStoreWriteCallback"), loggingOpts).Return(nil)

	err := mockServerStarter.StartGrpcServer(fs, host, port, 9090, writeLoggedFeaturesCallback, loggingOpts)
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

func TestStartHttpsServerHealthEndpoint(t *testing.T) {
	certPath, keyPath := createSelfSignedTLSFiles(t)
	host := "127.0.0.1"
	port := getFreePort(t)
	metricsPort := getFreePort(t)

	stop := make(chan os.Signal, 1)
	prevNewSignalStopChannel := newSignalStopChannel
	newSignalStopChannel = func() (chan os.Signal, func()) {
		return stop, func() {}
	}
	t.Cleanup(func() {
		newSignalStopChannel = prevNewSignalStopChannel
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- StartHttpsServer(&feast.FeatureStore{}, host, port, metricsPort, certPath, keyPath, nil, &logging.LoggingOptions{})
	}()

	httpsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		},
	}
	t.Cleanup(httpsClient.CloseIdleConnections)

	url := fmt.Sprintf("https://%s:%d/health", host, port)

	var (
		resp *http.Response
		err  error
	)
	require.Eventually(t, func() bool {
		resp, err = httpsClient.Get(url)
		if err != nil {
			return false
		}
		return true
	}, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
	})

	body, readErr := io.ReadAll(resp.Body)
	require.NoError(t, readErr)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Healthy", strings.TrimSpace(string(body)))

	stop <- syscall.SIGTERM

	select {
	case startErr := <-errCh:
		require.NoError(t, startErr)
	case <-time.After(5 * time.Second):
		t.Fatal("StartHttpsServer did not shutdown within timeout")
	}
}

func TestStartHttpsServerTLSFilesRequired(t *testing.T) {
	err := StartHttpsServer(&feast.FeatureStore{}, "127.0.0.1", 0, 0, "", "", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS_CERT_FILE and TLS_KEY_FILE must be set")
}

func getFreePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		_ = listener.Close()
	}()

	addr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	return addr.Port
}

func createSelfSignedTLSFiles(t *testing.T) (string, string) {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	certFile, err := os.CreateTemp(t.TempDir(), "feast-test-cert-*.pem")
	require.NoError(t, err)
	defer func() {
		_ = certFile.Close()
	}()

	keyFile, err := os.CreateTemp(t.TempDir(), "feast-test-key-*.pem")
	require.NoError(t, err)
	defer func() {
		_ = keyFile.Close()
	}()

	err = pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	require.NoError(t, err)

	err = pem.Encode(keyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	require.NoError(t, err)

	return certFile.Name(), keyFile.Name()
}
