// Package test holds integration tests that assemble the whole service the
// way cmd/server/main.go does and drive its public gRPC API over a real TCP
// connection. The only fake is at the true external boundary: the Kubernetes
// API itself (client-go fake clientset behind the real infrastructure/k8s
// adapter). Unit tests live next to the code under internal/.
package test

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	v1 "github.com/Duke-ECE/protos/gen/go/sandbox/v1"
	v2 "github.com/Duke-ECE/protos/gen/go/sandbox/v2"
	"github.com/Duke-ECE/sandbox-manager/internal/executor"
	"github.com/Duke-ECE/sandbox-manager/internal/infrastructure/k8s"
	"github.com/Duke-ECE/sandbox-manager/internal/sandbox"
	transportgrpc "github.com/Duke-ECE/sandbox-manager/internal/transport/grpc"
)

const testNamespace = "pi-sandbox"

// rig is the fully-assembled service under test.
type rig struct {
	v1   v1.SandboxManagerServiceClient
	v2   v2.SandboxExecutorServiceClient
	cs   *fake.Clientset
	svc  *sandbox.Service
	exec *recordingExec
}

// recordingExec is the canned executor.ExecFunc: it echoes the command's
// final argument to stdout, exits 0, and records the pod it ran in.
type recordingExec struct {
	mu      sync.Mutex
	podName string
	cmd     []string
}

func (r *recordingExec) run(_ context.Context, podName string, cmd []string, stdout, _ io.Writer) error {
	r.mu.Lock()
	r.podName = podName
	r.cmd = append([]string{}, cmd...)
	r.mu.Unlock()
	_, err := fmt.Fprintln(stdout, cmd[len(cmd)-1])
	return err
}

func (r *recordingExec) last() (string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.podName, r.cmd
}

// newRig wires the service exactly like cmd/server/main.go: k8s client
// adapter over a (fake) clientset → sandbox service → pool controller →
// executor → transport/grpc server — but listens on a real loopback TCP port
// and dials it with an insecure gRPC client.
func newRig(t *testing.T) *rig {
	t.Helper()

	cs := fake.NewSimpleClientset()

	store := k8s.NewClient(cs, nil, testNamespace)
	svc := sandbox.NewService(store, sandbox.Config{
		Namespace:    testNamespace,
		Image:        "busybox:1.36",
		MaxSandboxes: 10,
		DefaultTTL:   900,
		PoolSize:     2,
	})
	rec := &recordingExec{}
	execSvc := executor.New(svc, rec.run, executor.Config{
		MaxSandboxes: 10,
		ExecTimeout:  60,
		MaxOutput:    1 << 20,
		Tools:        map[string][]string{"shell": {"/bin/sh", "-c"}},
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go svc.RunPoolController(ctx)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := transportgrpc.NewServer(svc, execSvc)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", lis.Addr(), err)
	}
	t.Cleanup(func() { conn.Close() })

	return &rig{
		v1:   v1.NewSandboxManagerServiceClient(conn),
		v2:   v2.NewSandboxExecutorServiceClient(conn),
		cs:   cs,
		svc:  svc,
		exec: rec,
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (r *rig) listPods(t *testing.T, selector string) []corev1.Pod {
	t.Helper()
	pods, err := r.cs.CoreV1().Pods(testNamespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		t.Fatalf("list pods (%s): %v", selector, err)
	}
	return pods.Items
}

// promotePendingToReady simulates the kubelet: fake-clientset pods never go
// Running on their own, so we set their phase to Running and then run one
// pool reconcile pass to promote them pending → ready.
func (r *rig) promotePendingToReady(t *testing.T) {
	t.Helper()
	for _, pod := range r.listPods(t, "pool=pending") {
		pod.Status.Phase = corev1.PodRunning
		if _, err := r.cs.CoreV1().Pods(testNamespace).UpdateStatus(context.Background(), &pod, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("promote %s to Running: %v", pod.Name, err)
		}
	}
	r.svc.ReconcilePool(context.Background())
}

// waitForReadyPool blocks until the pool has at least one ready pod, driving
// the fake kubelet promotion between reconcile passes.
func (r *rig) waitForReadyPool(t *testing.T) {
	t.Helper()
	waitFor(t, "a ready pool pod", func() bool {
		r.promotePendingToReady(t)
		return len(r.listPods(t, "pool=ready")) > 0
	})
}

func TestV1SandboxLifecycle(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	resp, err := r.v1.CreateSandbox(ctx, &v1.CreateSandboxRequest{})
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	id := resp.GetSandboxId()
	if id == "" {
		t.Fatal("CreateSandbox returned empty sandbox id")
	}

	pods := r.listPods(t, "sandbox-id="+id)
	if len(pods) != 1 {
		t.Fatalf("expected 1 pod for %s, got %d", id, len(pods))
	}
	pod := pods[0]
	if pod.Labels["app"] != "sandbox" || pod.Labels["sandbox-id"] != id {
		t.Errorf("pod labels = %v, want app=sandbox sandbox-id=%s", pod.Labels, id)
	}
	if _, hasPoolLabel := pod.Labels["pool"]; hasPoolLabel {
		t.Errorf("v1 sandbox must not carry a pool label, got %v", pod.Labels)
	}
	if pod.Spec.Containers[0].Image != "busybox:1.36" {
		t.Errorf("image = %q, want busybox:1.36", pod.Spec.Containers[0].Image)
	}

	if _, err := r.v1.DeleteSandbox(ctx, &v1.DeleteSandboxRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteSandbox: %v", err)
	}
	waitFor(t, "sandbox pod to be deleted", func() bool {
		return len(r.listPods(t, "sandbox-id="+id)) == 0
	})

	// Deleting it again must surface the domain NotFound as gRPC NotFound.
	if _, err := r.v1.DeleteSandbox(ctx, &v1.DeleteSandboxRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Errorf("second DeleteSandbox = %v, want NotFound", err)
	}
}

func TestV2ExecuteEndToEnd(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	r.waitForReadyPool(t)

	resp, err := r.v2.Execute(ctx, &v2.ExecuteRequest{Tool: "shell", Input: "echo hi"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.GetExitCode() != 0 {
		t.Errorf("exit_code = %d, want 0", resp.GetExitCode())
	}
	if !strings.Contains(resp.GetStdout(), "echo hi") {
		t.Errorf("stdout = %q, want it to contain the echoed command", resp.GetStdout())
	}
	if resp.GetTruncated() {
		t.Error("truncated = true, want false for a tiny output")
	}

	// The canned exec must have been handed the tool prefix + input.
	podName, cmd := r.exec.last()
	if podName == "" {
		t.Fatal("exec never ran")
	}
	wantCmd := []string{"/bin/sh", "-c", "echo hi"}
	if len(cmd) != len(wantCmd) {
		t.Fatalf("exec cmd = %v, want %v", cmd, wantCmd)
	}
	for i := range wantCmd {
		if cmd[i] != wantCmd[i] {
			t.Fatalf("exec cmd = %v, want %v", cmd, wantCmd)
		}
	}

	// Single-use: the claimed pod is destroyed after Execute.
	waitFor(t, "claimed pod to be deleted", func() bool {
		_, err := r.cs.CoreV1().Pods(testNamespace).Get(ctx, podName, metav1.GetOptions{})
		return err != nil
	})
}

func TestV2ExecuteUnknownTool(t *testing.T) {
	r := newRig(t)

	_, err := r.v2.Execute(context.Background(), &v2.ExecuteRequest{Tool: "bogus", Input: "echo hi"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Execute with unknown tool = %v, want InvalidArgument", err)
	}
}
