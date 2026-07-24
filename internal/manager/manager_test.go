package manager

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	v1 "github.com/Duke-ECE/protos/gen/go/sandbox/v1"
)

// newTestClient serves the real SandboxManager gRPC service over an in-memory
// bufconn listener, backed by a fake k8s API. This exercises the full path —
// proto contract, server handler, k8s client logic — without a cluster.
func newTestClient(t *testing.T, m *Manager) (v1.SandboxManagerServiceClient, *fake.Clientset) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	v1.RegisterSandboxManagerServiceServer(s, m)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	client, _ := m.client.(*fake.Clientset)
	return v1.NewSandboxManagerServiceClient(conn), client
}

func newManager(objects ...corev1.Pod) *Manager {
	runtimeObjects := make([]runtime.Object, 0, len(objects))
	for i := range objects {
		runtimeObjects = append(runtimeObjects, &objects[i])
	}
	return &Manager{
		client:       fake.NewSimpleClientset(runtimeObjects...),
		namespace:    "pi-sandbox",
		image:        "default-image:latest",
		maxSandboxes: 10,
		defaultTTL:   900,
	}
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

func TestCreateSandbox(t *testing.T) {
	client, k8s := newTestClient(t, newManager())

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
	if pod.Labels[labelApp] != labelAppValue || pod.Labels[labelSandboxID] != resp.GetSandboxId() {
		t.Errorf("unexpected labels: %v", pod.Labels)
	}
	if pod.Annotations[annoTTL] != "600" {
		t.Errorf("ttl annotation = %q, want 600", pod.Annotations[annoTTL])
	}
	if _, err := time.Parse(time.RFC3339, pod.Annotations[annoCreatedAt]); err != nil {
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
	client, k8s := newTestClient(t, newManager())

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
	if pod.Annotations[annoTTL] != "900" {
		t.Errorf("ttl annotation = %q, want 900", pod.Annotations[annoTTL])
	}
}

func TestCreateSandboxLimit(t *testing.T) {
	m := newManager()
	m.maxSandboxes = 2
	client, _ := newTestClient(t, m)

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
	client, k8s := newTestClient(t, newManager())

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
	client, _ := newTestClient(t, newManager())

	_, err := client.DeleteSandbox(context.Background(), &v1.DeleteSandboxRequest{SandboxId: "sb-nope"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

func TestDeleteSandboxEmptyID(t *testing.T) {
	client, _ := newTestClient(t, newManager())

	_, err := client.DeleteSandbox(context.Background(), &v1.DeleteSandboxRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestReaper(t *testing.T) {
	expired := sandboxPod("sb-old", time.Now().Add(-time.Hour).UTC().Format(time.RFC3339), 1)
	fresh := sandboxPod("sb-new", time.Now().UTC().Format(time.RFC3339), 900)
	m := newManager(expired, fresh)

	m.reapOnce()

	pods, err := m.client.CoreV1().Pods("pi-sandbox").List(context.Background(), metav1.ListOptions{})
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
