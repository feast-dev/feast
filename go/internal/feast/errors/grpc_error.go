package errors

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
    "net/http"
)

func GrpcErrorf(code codes.Code, format string, args ...interface{}) error {
	return status.Newf(code, format, args...).Err()
}

func GrpcFromError(err error) error {
	if s, ok := status.FromError(err); ok {
		return s.Err()
	}
	return status.Error(codes.Internal, err.Error())
}

func GrpcInternalErrorf(format string, args ...interface{}) error {
	return GrpcErrorf(codes.Internal, format, args...)
}

func GrpcInvalidArgumentErrorf(format string, args ...interface{}) error {
	return GrpcErrorf(codes.InvalidArgument, format, args...)
}

func GrpcNotFoundErrorf(format string, args ...interface{}) error {
	return GrpcErrorf(codes.NotFound, format, args...)
}

func IsGrpcNotFoundError(err error) bool {
    if err == nil {
        return false
    }
    s, ok := status.FromError(err)
    return ok && s.Code() == codes.NotFound
}

func IsHTTPNotFoundError(err error) bool {
    if err == nil {
        return false
    }
    return strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), http.StatusText(http.StatusNotFound))
}