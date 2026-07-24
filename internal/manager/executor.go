package manager

import (
	"context"
	"errors"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilexec "k8s.io/utils/exec"

	v2 "github.com/Duke-ECE/protos/gen/go/sandbox/v2"
)

const (
	maxExecTimeout = 300 // seconds
	acquireWait    = 10 * time.Second
)

// Execute implements sandbox.v2.SandboxExecutorService: run one tool call in
// a fresh sandbox, return the result, destroy the sandbox. The caller never
// sees a sandbox identity — sandboxes are anonymous, stateless, single-use.
func (m *Manager) Execute(ctx context.Context, req *v2.ExecuteRequest) (*v2.ExecuteResponse, error) {
	prefix, ok := m.tools[req.GetTool()]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown tool %q", req.GetTool())
	}
	timeout := req.GetTimeoutSeconds()
	if timeout <= 0 {
		timeout = m.execTimeout
	}
	if timeout > maxExecTimeout {
		timeout = maxExecTimeout
	}

	pod, err := m.acquireSandbox(ctx)
	if err != nil {
		return nil, err
	}
	// Single-use: the sandbox is destroyed no matter how execution goes,
	// and the pool controller replenishes on its next tick.
	defer func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := m.client.CoreV1().Pods(m.namespace).Delete(delCtx, pod, metav1.DeleteOptions{}); err != nil {
			log.Printf("execute: delete sandbox %s: %v", pod, err)
		}
	}()

	execCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	stdout := &truncatedWriter{limit: m.maxOutput}
	stderr := &truncatedWriter{limit: m.maxOutput}
	cmd := append(append([]string{}, prefix...), req.GetInput())
	execErr := m.execFn(execCtx, pod, cmd, stdout, stderr)

	resp := &v2.ExecuteResponse{
		Stdout:    stdout.buf.String(),
		Stderr:    stderr.buf.String(),
		Truncated: stdout.truncated || stderr.truncated,
	}
	if execErr != nil {
		if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			return nil, status.Errorf(codes.DeadlineExceeded, "execution timed out after %ds", timeout)
		}
		// A non-zero process exit is a result, not a transport error.
		var exitErr utilexec.ExitError
		if errors.As(execErr, &exitErr) {
			resp.ExitCode = int32(exitErr.ExitStatus())
			return resp, nil
		}
		return nil, status.Errorf(codes.Internal, "exec: %v", execErr)
	}
	return resp, nil
}

// acquireSandbox picks a ready sandbox and marks it in-use. If none is ready
// but the pool is below capacity, it triggers a reconcile and waits briefly
// for one to come up; at capacity it fails fast with RESOURCE_EXHAUSTED.
func (m *Manager) acquireSandbox(ctx context.Context) (string, error) {
	deadline := time.Now().Add(acquireWait)
	for {
		ready, err := m.listPoolPods(ctx, poolReady)
		if err != nil {
			return "", status.Errorf(codes.Internal, "list pool: %v", err)
		}
		if len(ready) > 0 {
			pod := ready[0].DeepCopy()
			pod.Labels[labelPool] = poolInUse
			if _, err := m.client.CoreV1().Pods(m.namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				if apierrors.IsConflict(err) {
					continue // lost the race with another Execute, try again
				}
				return "", status.Errorf(codes.Internal, "acquire sandbox: %v", err)
			}
			return pod.Name, nil
		}

		total := 0
		for _, phase := range []string{poolPending, poolInUse} {
			pods, err := m.listPoolPods(ctx, phase)
			if err != nil {
				return "", status.Errorf(codes.Internal, "list pool: %v", err)
			}
			total += len(pods)
		}
		if total >= m.maxSandboxes {
			return "", status.Error(codes.ResourceExhausted, "sandbox pool exhausted")
		}

		m.reconcilePool(ctx)
		if time.Now().After(deadline) {
			return "", status.Error(codes.ResourceExhausted, "no sandbox became ready in time")
		}
		select {
		case <-ctx.Done():
			return "", status.Error(codes.Canceled, ctx.Err().Error())
		case <-time.After(time.Second):
		}
	}
}
