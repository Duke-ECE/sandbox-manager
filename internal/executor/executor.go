// Package executor is the Execute slice: it runs one tool call in a fresh
// sandbox, returns the result, and destroys the sandbox. The caller never
// sees a sandbox identity — sandboxes are anonymous, stateless, single-use.
//
// The slice depends on the sandbox slice only through the narrow,
// consumer-owned Pool interface below (satisfied by *sandbox.Service) and on
// the ExecFunc seam (production: the SPDY pods/exec call in
// internal/infrastructure/k8s). It never touches a clientset itself.
package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilexec "k8s.io/utils/exec"

	"github.com/Duke-ECE/sandbox-manager/internal/sandbox"
)

const (
	maxExecTimeout = 300 // seconds
	acquireWait    = 10 * time.Second
)

// DefaultTools maps tool names to the command prefix used to run their input.
var DefaultTools = map[string][]string{
	"shell":  {"/bin/sh", "-c"},
	"python": {"python3", "-c"},
}

// Pool is the executor's narrow, consumer-owned view of the sandbox slice's
// pool. *sandbox.Service implements it.
type Pool interface {
	// ListPoolPods returns pool pods in the given phase (sandbox.PoolReady,
	// sandbox.PoolPending, sandbox.PoolInUse).
	ListPoolPods(ctx context.Context, phase string) ([]corev1.Pod, error)
	// ClaimSandbox marks a ready pool pod in-use; update conflicts are
	// returned raw so the caller can retry.
	ClaimSandbox(ctx context.Context, pod *corev1.Pod) error
	// ReconcilePool runs one pool controller pass (promote + top up).
	ReconcilePool(ctx context.Context)
	// DeletePod deletes a pod by name.
	DeletePod(ctx context.Context, name string) error
}

// ExecFunc runs cmd inside a pod, writing the process's stdout/stderr.
// It is a seam for tests; production wires k8s.Client.Exec (SPDY pods/exec).
type ExecFunc func(ctx context.Context, podName string, cmd []string, stdout, stderr io.Writer) error

// Config holds the slice's settings (from env in cmd/server).
type Config struct {
	MaxSandboxes int   // total sandbox cap (same value the pool uses)
	ExecTimeout  int64 // seconds; default per-call timeout, capped at 300
	MaxOutput    int64 // bytes; per-stream output truncation
	Tools        map[string][]string
}

// Executor runs tool calls in single-use pool sandboxes.
type Executor struct {
	pool Pool
	exec ExecFunc

	maxSandboxes int
	execTimeout  int64 // seconds
	maxOutput    int64 // bytes
	tools        map[string][]string
}

// New wires the slice to the pool and the exec seam.
func New(pool Pool, exec ExecFunc, cfg Config) *Executor {
	return &Executor{
		pool:         pool,
		exec:         exec,
		maxSandboxes: cfg.MaxSandboxes,
		execTimeout:  cfg.ExecTimeout,
		maxOutput:    cfg.MaxOutput,
		tools:        cfg.Tools,
	}
}

// Result is the outcome of one Execute call.
type Result struct {
	Stdout    string
	Stderr    string
	ExitCode  int32
	Truncated bool
}

// Execute runs one tool call in a fresh sandbox, returns the result, and
// destroys the sandbox.
func (e *Executor) Execute(ctx context.Context, tool, input string, timeoutSeconds int64) (*Result, error) {
	prefix, ok := e.tools[tool]
	if !ok {
		return nil, invalidArgument(fmt.Sprintf("unknown tool %q", tool))
	}
	timeout := timeoutSeconds
	if timeout <= 0 {
		timeout = e.execTimeout
	}
	if timeout > maxExecTimeout {
		timeout = maxExecTimeout
	}

	pod, err := e.acquireSandbox(ctx)
	if err != nil {
		return nil, err
	}
	// Single-use: the sandbox is destroyed no matter how execution goes,
	// and the pool controller replenishes on its next tick.
	defer func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := e.pool.DeletePod(delCtx, pod); err != nil {
			log.Printf("execute: delete sandbox %s: %v", pod, err)
		}
	}()

	execCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	stdout := &truncatedWriter{limit: e.maxOutput}
	stderr := &truncatedWriter{limit: e.maxOutput}
	cmd := append(append([]string{}, prefix...), input)
	execErr := e.exec(execCtx, pod, cmd, stdout, stderr)

	res := &Result{
		Stdout:    stdout.buf.String(),
		Stderr:    stderr.buf.String(),
		Truncated: stdout.truncated || stderr.truncated,
	}
	if execErr != nil {
		if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			return nil, deadlineExceeded(fmt.Sprintf("execution timed out after %ds", timeout))
		}
		// A non-zero process exit is a result, not a transport error.
		var exitErr utilexec.ExitError
		if errors.As(execErr, &exitErr) {
			res.ExitCode = int32(exitErr.ExitStatus())
			return res, nil
		}
		return nil, internal(fmt.Sprintf("exec: %v", execErr))
	}
	return res, nil
}

// acquireSandbox picks a ready sandbox and marks it in-use. If none is ready
// but the pool is below capacity, it triggers a reconcile and waits briefly
// for one to come up; at capacity it fails fast.
func (e *Executor) acquireSandbox(ctx context.Context) (string, error) {
	deadline := time.Now().Add(acquireWait)
	for {
		ready, err := e.pool.ListPoolPods(ctx, sandbox.PoolReady)
		if err != nil {
			return "", internal(fmt.Sprintf("list pool: %v", err))
		}
		if len(ready) > 0 {
			pod := ready[0]
			if err := e.pool.ClaimSandbox(ctx, &pod); err != nil {
				if apierrors.IsConflict(err) {
					continue // lost the race with another Execute, try again
				}
				return "", internal(fmt.Sprintf("acquire sandbox: %v", err))
			}
			return pod.Name, nil
		}

		total := 0
		for _, phase := range []string{sandbox.PoolPending, sandbox.PoolInUse} {
			pods, err := e.pool.ListPoolPods(ctx, phase)
			if err != nil {
				return "", internal(fmt.Sprintf("list pool: %v", err))
			}
			total += len(pods)
		}
		if total >= e.maxSandboxes {
			return "", resourceExhausted("sandbox pool exhausted")
		}

		e.pool.ReconcilePool(ctx)
		if time.Now().After(deadline) {
			return "", resourceExhausted("no sandbox became ready in time")
		}
		select {
		case <-ctx.Done():
			return "", canceled(ctx.Err().Error())
		case <-time.After(time.Second):
		}
	}
}

// truncatedWriter caps captured output at limit bytes and records whether
// anything was dropped.
type truncatedWriter struct {
	buf       bytes.Buffer
	limit     int64
	truncated bool
}

func (w *truncatedWriter) Write(p []byte) (int, error) {
	remaining := w.limit - int64(w.buf.Len())
	switch {
	case remaining <= 0:
		w.truncated = true
	case int64(len(p)) > remaining:
		w.buf.Write(p[:remaining])
		w.truncated = true
	default:
		w.buf.Write(p)
	}
	return len(p), nil
}
