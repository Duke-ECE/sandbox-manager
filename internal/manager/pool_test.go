package manager

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestReconcileCreatesUpToPoolSize(t *testing.T) {
	m := newManager()
	m.poolSize = 2

	m.reconcilePool(context.Background())

	pending, err := m.listPoolPods(context.Background(), poolPending)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending pods, got %d", len(pending))
	}
	pod := pending[0]
	if pod.Spec.Containers[0].Image != "default-image:latest" {
		t.Errorf("image = %q, want default-image:latest", pod.Spec.Containers[0].Image)
	}
	if got := pod.Spec.Containers[0].Command; len(got) != 2 || got[0] != "sleep" || got[1] != "infinity" {
		t.Errorf("command = %v, want [sleep infinity]", got)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Error("automountServiceAccountToken must be explicitly false")
	}
}

func TestReconcilePromotesRunningPendingToReady(t *testing.T) {
	m := newManager(poolPod("sb-p1", poolPending, corev1.PodRunning),
		poolPod("sb-p2", poolPending, corev1.PodPending))
	m.poolSize = 2

	m.reconcilePool(context.Background())

	ready, _ := m.listPoolPods(context.Background(), poolReady)
	pending, _ := m.listPoolPods(context.Background(), poolPending)
	if len(ready) != 1 || ready[0].Name != "sb-p1" {
		t.Errorf("expected sb-p1 ready, got %v", podNames(ready))
	}
	if len(pending) != 1 || pending[0].Name != "sb-p2" {
		t.Errorf("expected sb-p2 still pending, got %v", podNames(pending))
	}
}

func TestReconcileRespectsMaxSandboxes(t *testing.T) {
	m := newManager()
	m.poolSize = 5
	m.maxSandboxes = 2

	m.reconcilePool(context.Background())

	pending, _ := m.listPoolPods(context.Background(), poolPending)
	if len(pending) != 2 {
		t.Errorf("expected 2 pods (max), got %d", len(pending))
	}
}

func TestReconcileTopsUpPartialPool(t *testing.T) {
	m := newManager(poolPod("sb-r1", poolReady, corev1.PodRunning))
	m.poolSize = 3

	m.reconcilePool(context.Background())

	pending, _ := m.listPoolPods(context.Background(), poolPending)
	if len(pending) != 2 {
		t.Errorf("expected 2 new pending pods, got %d", len(pending))
	}
}

func TestRecoverPoolDeletesOrphanedInUse(t *testing.T) {
	m := newManager(poolPod("sb-orphan", poolInUse, corev1.PodRunning),
		poolPod("sb-keep", poolReady, corev1.PodRunning))

	m.recoverPool(context.Background())

	remaining, err := m.client.CoreV1().Pods("pi-sandbox").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(remaining.Items) != 1 || remaining.Items[0].Name != "sb-keep" {
		t.Errorf("expected only sb-keep to survive, got %v", podNames(remaining.Items))
	}
}

func podNames(pods []corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	return names
}
