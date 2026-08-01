// Package k8s implements the sandbox.PodStore port against the Kubernetes
// API, plus the pods/exec SPDY transport wired into the executor slice.
// It has no domain imports: interface satisfaction is structural and is
// checked at wiring time (cmd/server and the tests).
package k8s

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Client is a namespace-bound pod store backed by a clientset.
type Client struct {
	client     kubernetes.Interface
	restConfig *rest.Config // nil in tests; only used by the real exec path
	namespace  string
}

// LoadRESTConfig uses the in-cluster config when running in k8s, otherwise
// falls back to $KUBECONFIG / ~/.kube/config for local development.
func LoadRESTConfig() (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
		}
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("load kube config: %w", err)
		}
	}
	return cfg, nil
}

// NewClient returns a pod store backed by client in namespace.
func NewClient(client kubernetes.Interface, restConfig *rest.Config, namespace string) *Client {
	return &Client{client: client, restConfig: restConfig, namespace: namespace}
}

// ListPods returns the pods matching the label selector.
func (c *Client) ListPods(ctx context.Context, selector string) ([]corev1.Pod, error) {
	pods, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// CreatePod creates the pod.
func (c *Client) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	_, err := c.client.CoreV1().Pods(c.namespace).Create(ctx, pod, metav1.CreateOptions{})
	return err
}

// UpdatePod replaces the pod.
func (c *Client) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	_, err := c.client.CoreV1().Pods(c.namespace).Update(ctx, pod, metav1.UpdateOptions{})
	return err
}

// DeletePod deletes the pod by name.
func (c *Client) DeletePod(ctx context.Context, name string) error {
	return c.client.CoreV1().Pods(c.namespace).Delete(ctx, name, metav1.DeleteOptions{})
}
