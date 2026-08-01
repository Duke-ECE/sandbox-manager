package sandbox

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// PodStore is the persistence port: the k8s pod operations the slice needs.
// The implementation in internal/infrastructure/k8s (backed by a clientset)
// is the only production one; tests use it over a fake clientset. Methods
// return raw k8s errors — the slice wraps them in domain Errors.
type PodStore interface {
	// ListPods returns the pods matching the label selector.
	ListPods(ctx context.Context, selector string) ([]corev1.Pod, error)
	// CreatePod creates the pod.
	CreatePod(ctx context.Context, pod *corev1.Pod) error
	// UpdatePod replaces the pod (used for label/phase transitions).
	UpdatePod(ctx context.Context, pod *corev1.Pod) error
	// DeletePod deletes the pod by name.
	DeletePod(ctx context.Context, name string) error
}
