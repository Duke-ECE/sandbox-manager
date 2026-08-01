package grpc

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/Duke-ECE/protos/gen/go/sandbox/v1"
)

func TestCreateSandbox(t *testing.T) {
	client, _, k8s := newTestClient(t, testSandboxConfig(), testExecutorConfig(), nil)

	resp, err := client.CreateSandbox(context.Background(), &v1.CreateSandboxRequest{
		Image:      "busybox:1.36",
		TtlSeconds: 600,
	})
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	if resp.GetSandboxId() == "" {
		t.Fatal("expected a sandbox ID")
	}

	pod, err := k8s.CoreV1().Pods("pi-sandbox").Get(context.Background(), resp.GetSandboxId(), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("pod not found in cluster: %v", err)
	}
	if pod.Labels["app"] != "sandbox" || pod.Labels["sandbox-id"] != resp.GetSandboxId() {
		t.Errorf("unexpected labels: %v", pod.Labels)
	}
	if pod.Annotations["sandbox.duke-ece/ttl-seconds"] != "600" {
		t.Errorf("ttl annotation = %q, want 600", pod.Annotations["sandbox.duke-ece/ttl-seconds"])
	}
	if _, err := time.Parse(time.RFC3339, pod.Annotations["sandbox.duke-ece/created-at"]); err != nil {
		t.Errorf("created-at annotation not RFC3339: %v", err)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Error("automountServiceAccountToken must be explicitly false")
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %v, want Never", pod.Spec.RestartPolicy)
	}
	if pod.Spec.Containers[0].Image != "busybox:1.36" {
		t.Errorf("image = %q, want busybox:1.36", pod.Spec.Containers[0].Image)
	}
}

func TestCreateSandboxDefaults(t *testing.T) {
	client, _, k8s := newTestClient(t, testSandboxConfig(), testExecutorConfig(), nil)

	resp, err := client.CreateSandbox(context.Background(), &v1.CreateSandboxRequest{})
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	pod, err := k8s.CoreV1().Pods("pi-sandbox").Get(context.Background(), resp.GetSandboxId(), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("pod not found: %v", err)
	}
	if pod.Spec.Containers[0].Image != "default-image:latest" {
		t.Errorf("image = %q, want default-image:latest", pod.Spec.Containers[0].Image)
	}
	if pod.Annotations["sandbox.duke-ece/ttl-seconds"] != "900" {
		t.Errorf("ttl annotation = %q, want 900", pod.Annotations["sandbox.duke-ece/ttl-seconds"])
	}
}

func TestCreateSandboxLimit(t *testing.T) {
	sbCfg := testSandboxConfig()
	sbCfg.MaxSandboxes = 2
	client, _, _ := newTestClient(t, sbCfg, testExecutorConfig(), nil)

	for i := 0; i < 2; i++ {
		if _, err := client.CreateSandbox(context.Background(), &v1.CreateSandboxRequest{}); err != nil {
			t.Fatalf("CreateSandbox %d: %v", i, err)
		}
	}
	_, err := client.CreateSandbox(context.Background(), &v1.CreateSandboxRequest{})
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("code = %v, want ResourceExhausted (err=%v)", status.Code(err), err)
	}
}

func TestDeleteSandbox(t *testing.T) {
	client, _, k8s := newTestClient(t, testSandboxConfig(), testExecutorConfig(), nil)

	resp, err := client.CreateSandbox(context.Background(), &v1.CreateSandboxRequest{})
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	if _, err := client.DeleteSandbox(context.Background(), &v1.DeleteSandboxRequest{SandboxId: resp.GetSandboxId()}); err != nil {
		t.Fatalf("DeleteSandbox: %v", err)
	}

	pods, err := k8s.CoreV1().Pods("pi-sandbox").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("expected no pods left, got %d", len(pods.Items))
	}
}

func TestDeleteSandboxNotFound(t *testing.T) {
	client, _, _ := newTestClient(t, testSandboxConfig(), testExecutorConfig(), nil)

	_, err := client.DeleteSandbox(context.Background(), &v1.DeleteSandboxRequest{SandboxId: "sb-nope"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

func TestDeleteSandboxEmptyID(t *testing.T) {
	client, _, _ := newTestClient(t, testSandboxConfig(), testExecutorConfig(), nil)

	_, err := client.DeleteSandbox(context.Background(), &v1.DeleteSandboxRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}
