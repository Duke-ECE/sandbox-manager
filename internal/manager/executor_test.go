package manager

import (
	"context"
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilexec "k8s.io/utils/exec"

	v2 "github.com/Duke-ECE/protos/gen/go/sandbox/v2"
)

// poolPod seeds a pool pod in the given phase and k8s pod phase.
func poolPod(name, poolPhase string, k8sPhase corev1.PodPhase) corev1.Pod {
	p := sandboxPod(name, "2026-01-01T00:00:00Z", 900)
	p.Labels[labelPool] = poolPhase
	p.Status.Phase = k8sPhase
	return p
}

// newExecutorManager returns a Manager wired for v2 tests with a canned exec.
func newExecutorManager(execFn execFunc, objects ...corev1.Pod) *Manager {
	m := newManager(objects...)
	m.poolSize = 3
	m.execTimeout = 60
	m.maxOutput = 1 << 20
	m.tools = map[string][]string{"shell": {"/bin/sh", "-c"}}
	m.execFn = execFn
	return m
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
	m := newExecutorManager(execFn, poolPod("sb-ready1", poolReady, corev1.PodRunning))
	_, client, k8s := newTestClient(t, m)

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
	m := newExecutorManager(nil)
	_, client, _ := newTestClient(t, m)

	_, err := client.Execute(context.Background(), &v2.ExecuteRequest{Tool: "ruby", Input: "puts 1"})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestExecutePoolExhausted(t *testing.T) {
	m := newExecutorManager(nil, poolPod("sb-busy", poolInUse, corev1.PodRunning))
	m.maxSandboxes = 1
	_, client, _ := newTestClient(t, m)

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
	m := newExecutorManager(execFn, poolPod("sb-ready1", poolReady, corev1.PodRunning))
	_, client, _ := newTestClient(t, m)

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
	m := newExecutorManager(execFn, poolPod("sb-ready1", poolReady, corev1.PodRunning))
	m.maxOutput = 4
	_, client, _ := newTestClient(t, m)

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
	m := newExecutorManager(execFn, poolPod("sb-ready1", poolReady, corev1.PodRunning))
	_, client, k8s := newTestClient(t, m)

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
