package manager

import (
	"bytes"
	"context"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

// execFunc runs cmd inside a pod, writing the process's stdout/stderr.
// It is a seam for tests; production uses k8sExec.
type execFunc func(ctx context.Context, podName string, cmd []string, stdout, stderr io.Writer) error

// k8sExec runs cmd in the pod via the k8s exec subresource (pods/exec).
// Any image with the requested binary works — nothing needs to be installed
// or running inside the sandbox beforehand.
func (m *Manager) k8sExec(ctx context.Context, podName string, cmd []string, stdout, stderr io.Writer) error {
	req := m.client.CoreV1().RESTClient().Post().
		Resource("pods").Name(podName).Namespace(m.namespace).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command: cmd,
			Stdout:  true,
			Stderr:  true,
		}, scheme.ParameterCodec)
	ex, err := remotecommand.NewSPDYExecutor(m.restConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("build executor: %w", err)
	}
	return ex.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: stdout,
		Stderr: stderr,
	})
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
