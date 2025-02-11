package feast

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FeastTransformationServiceNotConfigured struct{}

func (FeastTransformationServiceNotConfigured) GRPCStatus() *status.Status {
	errorStatus := status.New(codes.Internal, "No transformation service configured")
	ds, err := errorStatus.WithDetails(&errdetails.LocalizedMessage{Message: "No transformation service configured, required for on-demand feature transformations"})
	if err != nil {
		return errorStatus
	}
	return ds
}

func (e FeastTransformationServiceNotConfigured) Error() string {
	return e.GRPCStatus().Err().Error()
}
