package grpc

import (
	"context"

	v2 "github.com/Duke-ECE/protos/gen/go/sandbox/v2"
	"github.com/Duke-ECE/sandbox-manager/internal/executor"
)

// ExecutorHandler serves sandbox.v2.SandboxExecutorService on top of the
// executor domain slice. Handlers are thin: decode the request, call the
// slice, and map errors via toStatus.
type ExecutorHandler struct {
	v2.UnimplementedSandboxExecutorServiceServer

	exec *executor.Executor
}

// NewExecutorHandler wires the gRPC handler to exec.
func NewExecutorHandler(exec *executor.Executor) *ExecutorHandler {
	return &ExecutorHandler{exec: exec}
}

func (h *ExecutorHandler) Execute(ctx context.Context, req *v2.ExecuteRequest) (*v2.ExecuteResponse, error) {
	res, err := h.exec.Execute(ctx, req.GetTool(), req.GetInput(), req.GetTimeoutSeconds())
	if err != nil {
		return nil, toStatus(err)
	}
	return &v2.ExecuteResponse{
		Stdout:    res.Stdout,
		Stderr:    res.Stderr,
		ExitCode:  res.ExitCode,
		Truncated: res.Truncated,
	}, nil
}
