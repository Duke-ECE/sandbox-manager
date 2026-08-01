package grpc

import (
	"context"

	v1 "github.com/Duke-ECE/protos/gen/go/sandbox/v1"
	"github.com/Duke-ECE/sandbox-manager/internal/sandbox"
)

// SandboxHandler serves sandbox.v1.SandboxManagerService on top of the
// sandbox domain slice. Handlers are thin: decode the request, call the
// slice service, and map errors via toStatus.
type SandboxHandler struct {
	v1.UnimplementedSandboxManagerServiceServer

	svc *sandbox.Service
}

// NewSandboxHandler wires the gRPC handlers to svc.
func NewSandboxHandler(svc *sandbox.Service) *SandboxHandler {
	return &SandboxHandler{svc: svc}
}

func (h *SandboxHandler) CreateSandbox(ctx context.Context, req *v1.CreateSandboxRequest) (*v1.CreateSandboxResponse, error) {
	id, err := h.svc.CreateSandbox(ctx, req.GetImage(), req.GetTtlSeconds())
	if err != nil {
		return nil, toStatus(err)
	}
	return &v1.CreateSandboxResponse{SandboxId: id}, nil
}

func (h *SandboxHandler) DeleteSandbox(ctx context.Context, req *v1.DeleteSandboxRequest) (*v1.DeleteSandboxResponse, error) {
	if err := h.svc.DeleteSandbox(ctx, req.GetSandboxId()); err != nil {
		return nil, toStatus(err)
	}
	return &v1.DeleteSandboxResponse{}, nil
}
