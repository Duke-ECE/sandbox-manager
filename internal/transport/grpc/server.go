// Package grpc exposes the sandbox and executor domain slices over gRPC: it
// adapts sandbox.v1.SandboxManagerService and sandbox.v2.SandboxExecutorService
// to the slices and owns the only error→status mapping (errors.go).
package grpc

import (
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	v1 "github.com/Duke-ECE/protos/gen/go/sandbox/v1"
	v2 "github.com/Duke-ECE/protos/gen/go/sandbox/v2"
	v3 "github.com/Duke-ECE/protos/gen/go/sandbox/v3"
	"github.com/Duke-ECE/sandbox-manager/internal/executor"
	"github.com/Duke-ECE/sandbox-manager/internal/sandbox"
	"github.com/Duke-ECE/sandbox-manager/internal/sessioncube"
)

// NewServer builds a grpc.Server with both sandbox services registered and
// reflection enabled (for grpcurl).
func NewServer(svc *sandbox.Service, exec *executor.Executor, cube ...*sessioncube.Service) *googlegrpc.Server {
	s := googlegrpc.NewServer()
	v1.RegisterSandboxManagerServiceServer(s, NewSandboxHandler(svc))
	v2.RegisterSandboxExecutorServiceServer(s, NewExecutorHandler(exec))
	if len(cube) > 0 && cube[0] != nil {
		v3.RegisterSessionSandboxServiceServer(s, NewSessionHandler(cube[0]))
	}
	reflection.Register(s)
	return s
}
