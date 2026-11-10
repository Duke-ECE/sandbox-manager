package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Duke-ECE/sandbox-manager/internal/executor"
	"github.com/Duke-ECE/sandbox-manager/internal/sandbox"
	"github.com/Duke-ECE/sandbox-manager/internal/sessioncube"
)

// toStatus is the only error→status mapping in the service. Domain errors
// (sandbox.Error, executor.Error) carry their caller-facing message, which is
// returned verbatim; anything else is an internal failure.
func toStatus(err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	}
	switch {
	case errors.Is(err, sessioncube.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, sessioncube.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, sessioncube.ErrLimit):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, sessioncube.ErrUnready):
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	var sbErr *sandbox.Error
	if errors.As(err, &sbErr) {
		return status.Error(sandboxCode(sbErr.Kind), sbErr.Message)
	}
	var exErr *executor.Error
	if errors.As(err, &exErr) {
		return status.Error(executorCode(exErr.Kind), exErr.Message)
	}
	return status.Error(codes.Internal, err.Error())
}

func sandboxCode(k sandbox.Kind) codes.Code {
	switch k {
	case sandbox.KindInvalidArgument:
		return codes.InvalidArgument
	case sandbox.KindNotFound:
		return codes.NotFound
	case sandbox.KindResourceExhausted:
		return codes.ResourceExhausted
	default:
		return codes.Internal
	}
}

func executorCode(k executor.Kind) codes.Code {
	switch k {
	case executor.KindInvalidArgument:
		return codes.InvalidArgument
	case executor.KindResourceExhausted:
		return codes.ResourceExhausted
	case executor.KindDeadlineExceeded:
		return codes.DeadlineExceeded
	case executor.KindCanceled:
		return codes.Canceled
	default:
		return codes.Internal
	}
}
