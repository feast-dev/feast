// These contain configs/methods that are used by the hybrid server function.
package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var defaultCheckTimeout = 2 * time.Second

// Register default HTTP handlers specific to the hybrid server configuration.
func DefaultHybridHandlers(s *httpServer, port int) []Handler {
	return CommonHttpHandlers(s, combinedHealthCheck(port))
}

// This function wraps an http.Handler that is registered during hybrid server creation.
// Calls the grpc.server healthcheck check endpoint
func combinedHealthCheck(port int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), defaultCheckTimeout)
		defer cancel()

		target := fmt.Sprintf("localhost:%d", port)
		conn, err := grpc.DialContext(
			ctx,
			target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)

		if err != nil {
			http.Error(w, fmt.Sprintf("gRPC server connectivity check failed: %v", err), http.StatusServiceUnavailable)
			return
		}
		defer conn.Close()

		hc := healthpb.NewHealthClient(conn)
		resp, err := hc.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
		if err != nil {
			http.Error(w, fmt.Sprintf("gRPC health check failed: %v", err), http.StatusInternalServerError)
			return
		}

		// Use to map servingStatus to httpStatus
		var status int
		switch resp.Status {
		case healthpb.HealthCheckResponse_SERVING:
			status = http.StatusOK
		case healthpb.HealthCheckResponse_NOT_SERVING:
			status = http.StatusServiceUnavailable
		default:
			status = http.StatusInternalServerError
		}

		w.WriteHeader(status)
		w.Write([]byte(resp.Status.String()))
	}
}
