package sandbox

import (
	"context"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/Duke-ECE/sandbox-manager/internal/infrastructure/k8s"
)

// newTestService wires the slice to the real k8s PodStore adapter over a
// fake clientset, so tests exercise the full path without a cluster.
func newTestService(objects ...corev1.Pod) (*Service, *fake.Clientset) {
	runtimeObjects := make([]runtime.Object, 0, len(objects))
	for i := range objects {
		runtimeObjects = append(runtimeObjects, &objects[i])
	}
	cs := fake.NewSimpleClientset(runtimeObjects...)
	svc := NewService(k8s.NewClient(cs, nil, "pi-sandbox"), Config{
		Namespace:    "pi-sandbox",
		Image:        "default-image:latest",
		MaxSandboxes: 10,
		DefaultTTL:   900,
	})
	return svc, cs
}

func sandboxPod(name, createdAt string, ttl int64) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "pi-sandbox",
			Labels: map[string]string{
				labelApp:       labelAppValue,
				labelSandboxID: name,
			},
			Annotations: map[string]string{
				annoCreatedAt: createdAt,
				annoTTL:       strconv.FormatInt(ttl, 10),
			},
		},
	}
}

func TestReaper(t *testing.T) {
	expired := sandboxPod("sb-old", time.Now().Add(-time.Hour).UTC().Format(time.RFC3339), 1)
	fresh := sandboxPod("sb-new", time.Now().UTC().Format(time.RFC3339), 900)
	svc, cs := newTestService(expired, fresh)

	svc.reapOnce()

	pods, err := cs.CoreV1().Pods("pi-sandbox").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Name != "sb-new" {
		names := make([]string, 0, len(pods.Items))
		for _, p := range pods.Items {
			names = append(names, p.Name)
		}
		t.Errorf("expected only sb-new to survive, got %v", names)
	}
}
