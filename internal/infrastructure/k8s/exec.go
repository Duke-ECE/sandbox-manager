package k8s

import (
	"context"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

// Exec runs cmd in the pod via the k8s exec subresource (pods/exec).
// Any image with the requested binary works — nothing needs to be installed
// or running inside the sandbox beforehand. Its signature matches
// executor.ExecFunc; cmd/server wires it directly.
func (c *Client) Exec(ctx context.Context, podName string, cmd []string, stdout, stderr io.Writer) error {
	req := c.client.CoreV1().RESTClient().Post().
		Resource("pods").Name(podName).Namespace(c.namespace).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command: cmd,
			Stdout:  true,
			Stderr:  true,
		}, scheme.ParameterCodec)
	ex, err := remotecommand.NewSPDYExecutor(c.restConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("build executor: %w", err)
	}
	return ex.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: stdout,
		Stderr: stderr,
	})
}
