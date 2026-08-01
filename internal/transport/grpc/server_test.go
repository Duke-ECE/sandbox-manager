package grpc

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	v1 "github.com/Duke-ECE/protos/gen/go/sandbox/v1"
	v2 "github.com/Duke-ECE/protos/gen/go/sandbox/v2"
	"github.com/Duke-ECE/sandbox-manager/internal/executor"
	"github.com/Duke-ECE/sandbox-manager/internal/infrastructure/k8s"
	"github.com/Duke-ECE/sandbox-manager/internal/sandbox"
)

// newTestClient serves the real gRPC services over an in-memory bufconn
// listener, backed by the real k8s PodStore adapter over a fake clientset.
// This exercises the full path — proto contract, handlers, slices, k8s
// client logic — without a cluster.
func newTestClient(t *testing.T, sbCfg sandbox.Config, exCfg executor.Config, execFn executor.ExecFunc, objects ...corev1.Pod) (v1.SandboxManagerServiceClient, v2.SandboxExecutorServiceClient, *fake.Clientset) {
	t.Helper()

	runtimeObjects := make([]runtime.Object, 0, len(objects))
	for i := range objects {
		runtimeObjects = append(runtimeObjects, &objects[i])
	}
	cs := fake.NewSimpleClientset(runtimeObjects...)

	store := k8s.NewClient(cs, nil, sbCfg.Namespace)
	svc := sandbox.NewService(store, sbCfg)
	exec := executor.New(svc, execFn, exCfg)

	lis := bufconn.Listen(1024 * 1024)
	s := NewServer(svc, exec)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return v1.NewSandboxManagerServiceClient(conn), v2.NewSandboxExecutorServiceClient(conn), cs
}

func testSandboxConfig() sandbox.Config {
	return sandbox.Config{
		Namespace:    "pi-sandbox",
		Image:        "default-image:latest",
		MaxSandboxes: 10,
		DefaultTTL:   900,
	}
}

func testExecutorConfig() executor.Config {
	return executor.Config{
		MaxSandboxes: 10,
		ExecTimeout:  60,
		MaxOutput:    1 << 20,
		Tools:        map[string][]string{"shell": {"/bin/sh", "-c"}},
	}
}
