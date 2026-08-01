package grpc

import (
	"context"
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	utilexec "k8s.io/utils/exec"

	v2 "github.com/Duke-ECE/protos/gen/go/sandbox/v2"
	"github.com/Duke-ECE/sandbox-manager/internal/executor"
	"github.com/Duke-ECE/sandbox-manager/internal/sandbox"
)

// poolPod seeds a pool pod in the given phase and k8s pod phase.
func poolPod(name, poolPhase string, k8sPhase corev1.PodPhase) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "pi-sandbox",
			Labels: map[string]string{
				"app":        "sandbox",
				"sandbox-id": name,
				"pool":       poolPhase,
			},
			Annotations: map[string]string{
				"sandbox.duke-ece/created-at":  "2026-01-01T00:00:00Z",
				"sandbox.duke-ece/ttl-seconds": "900",
			},
		},
		Status: corev1.PodStatus{Phase: k8sPhase},
	}
}

// newExecutorTestClient serves the stack with a canned exec and executor
// config tweaks applied.
func newExecutorTestClient(t *testing.T, execFn executor.ExecFunc, tweak func(*executor.Config), objects ...corev1.Pod) (v2.SandboxExecutorServiceClient, *fake.Clientset) {
	t.Helper()
	exCfg := testExecutorConfig()
	if tweak != nil {
		tweak(&exCfg)
	}
	_, client, cs := newTestClient(t, testSandboxConfig(), exCfg, execFn, objects...)
	return client, cs
}

func TestExecuteHappyPath(t *testing.T) {
	execFn := func(_ context.Context, podName string, cmd []string, stdout, stderr io.Writer) error {
		if podName != "sb-ready1" {
			t.Errorf("exec in pod %q, want sb-ready1", podName)
		}
		want := []string{"/bin/sh", "-c", "echo hi"}
		if len(cmd) != len(want) {
			t.Fatalf("cmd = %v, want %v", cmd, want)
		}
		for i := range want {
			if cmd[i] != want[i] {
				t.Fatalf("cmd = %v, want %v", cmd, want)
			}
		}
		stdout.Write([]byte("hi\n"))
		return nil
	}
	client, k8s := newExecutorTestClient(t, execFn, nil, poolPod("sb-ready1", sandbox.PoolReady, corev1.PodRunning))

	resp, err := client.Execute(context.Background(), &v2.ExecuteRequest{Tool: "shell", Input: "echo hi"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.GetStdout() != "hi\n" {
		t.Errorf("stdout = %q, want %q", resp.GetStdout(), "hi\n")
	}
	if resp.GetExitCode() != 0 {
		t.Errorf("exit_code = %d, want 0", resp.GetExitCode())
	}

	// The sandbox must be destroyed after the call (single-use).
	pods, err := k8s.CoreV1().Pods("pi-sandbox").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("expected sandbox to be destroyed, %d pods left", len(pods.Items))
	}
}

func TestExecuteUnknownTool(t *testing.T) {
	client, _ := newExecutorTestClient(t, nil, nil)

	_, err := client.Execute(context.Background(), &v2.ExecuteRequest{Tool: "ruby", Input: "puts 1"})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestExecutePoolExhausted(t *testing.T) {
	client, _ := newExecutorTestClient(t, nil, func(cfg *executor.Config) { cfg.MaxSandboxes = 1 },
		poolPod("sb-busy", sandbox.PoolInUse, corev1.PodRunning))

	_, err := client.Execute(context.Background(), &v2.ExecuteRequest{Tool: "shell", Input: "true"})
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("code = %v, want ResourceExhausted (err=%v)", status.Code(err), err)
	}
}

func TestExecuteExitCodePassthrough(t *testing.T) {
	execFn := func(_ context.Context, _ string, _ []string, _, stderr io.Writer) error {
		stderr.Write([]byte("boom"))
		return utilexec.CodeExitError{Err: errors.New("command failed"), Code: 3}
	}
	client, _ := newExecutorTestClient(t, execFn, nil, poolPod("sb-ready1", sandbox.PoolReady, corev1.PodRunning))

	resp, err := client.Execute(context.Background(), &v2.ExecuteRequest{Tool: "shell", Input: "exit 3"})
	if err != nil {
		t.Fatalf("Execute: %v (non-zero exit must be a result, not an error)", err)
	}
	if resp.GetExitCode() != 3 {
		t.Errorf("exit_code = %d, want 3", resp.GetExitCode())
	}
	if resp.GetStderr() != "boom" {
		t.Errorf("stderr = %q, want %q", resp.GetStderr(), "boom")
	}
}

func TestExecuteOutputTruncation(t *testing.T) {
	execFn := func(_ context.Context, _ string, _ []string, stdout, _ io.Writer) error {
		stdout.Write([]byte("hello world"))
		return nil
	}
	client, _ := newExecutorTestClient(t, execFn, func(cfg *executor.Config) { cfg.MaxOutput = 4 },
		poolPod("sb-ready1", sandbox.PoolReady, corev1.PodRunning))

	resp, err := client.Execute(context.Background(), &v2.ExecuteRequest{Tool: "shell", Input: "echo hello world"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.GetStdout() != "hell" {
		t.Errorf("stdout = %q, want %q", resp.GetStdout(), "hell")
	}
	if !resp.GetTruncated() {
		t.Error("truncated = false, want true")
	}
}

func TestExecuteMarksSandboxInUse(t *testing.T) {
	seen := make(chan string, 1)
	execFn := func(_ context.Context, podName string, _ []string, _, _ io.Writer) error {
		seen <- podName
		return nil
	}
	client, k8s := newExecutorTestClient(t, execFn, nil, poolPod("sb-ready1", sandbox.PoolReady, corev1.PodRunning))

	if _, err := client.Execute(context.Background(), &v2.ExecuteRequest{Tool: "shell", Input: "true"}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	<-seen
	// Pod was deleted at the end of Execute; verify it was relabeled first by
	// checking the update action the fake client recorded.
	for _, action := range k8s.Actions() {
		if action.GetVerb() == "update" && action.GetResource().Resource == "pods" {
			return
		}
	}
	t.Error("expected an update action marking the pod in-use")
}
