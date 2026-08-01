package sandbox

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// poolPod seeds a pool pod in the given phase and k8s pod phase.
func poolPod(name, poolPhase string, k8sPhase corev1.PodPhase) corev1.Pod {
	p := sandboxPod(name, "2026-01-01T00:00:00Z", 900)
	p.Labels[labelPool] = poolPhase
	p.Status.Phase = k8sPhase
	return p
}

func TestReconcileCreatesUpToPoolSize(t *testing.T) {
	svc, _ := newTestService()
	svc.poolSize = 2

	svc.ReconcilePool(context.Background())

	pending, err := svc.ListPoolPods(context.Background(), PoolPending)
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
	svc, _ := newTestService(poolPod("sb-p1", PoolPending, corev1.PodRunning),
		poolPod("sb-p2", PoolPending, corev1.PodPending))
	svc.poolSize = 2

	svc.ReconcilePool(context.Background())

	ready, _ := svc.ListPoolPods(context.Background(), PoolReady)
	pending, _ := svc.ListPoolPods(context.Background(), PoolPending)
	if len(ready) != 1 || ready[0].Name != "sb-p1" {
		t.Errorf("expected sb-p1 ready, got %v", podNames(ready))
	}
	if len(pending) != 1 || pending[0].Name != "sb-p2" {
		t.Errorf("expected sb-p2 still pending, got %v", podNames(pending))
	}
}

func TestReconcileRespectsMaxSandboxes(t *testing.T) {
	svc, _ := newTestService()
	svc.poolSize = 5
	svc.maxSandboxes = 2

	svc.ReconcilePool(context.Background())

	pending, _ := svc.ListPoolPods(context.Background(), PoolPending)
	if len(pending) != 2 {
		t.Errorf("expected 2 pods (max), got %d", len(pending))
	}
}

func TestReconcileTopsUpPartialPool(t *testing.T) {
	svc, _ := newTestService(poolPod("sb-r1", PoolReady, corev1.PodRunning))
	svc.poolSize = 3

	svc.ReconcilePool(context.Background())

	pending, _ := svc.ListPoolPods(context.Background(), PoolPending)
	if len(pending) != 2 {
		t.Errorf("expected 2 new pending pods, got %d", len(pending))
	}
}

func TestRecoverPoolDeletesOrphanedInUse(t *testing.T) {
	svc, cs := newTestService(poolPod("sb-orphan", PoolInUse, corev1.PodRunning),
		poolPod("sb-keep", PoolReady, corev1.PodRunning))

	svc.recoverPool(context.Background())

	remaining, err := cs.CoreV1().Pods("pi-sandbox").List(context.Background(), metav1.ListOptions{})
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
