// Package manager implements the sandbox.v1.SandboxManager gRPC service.
//
// The manager is stateless: sandbox ID, creation time and TTL all live on the
// k8s Pod's labels and annotations, so a restart loses nothing.
package manager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	v1 "github.com/Duke-ECE/protos/gen/go/sandbox/v1"
)

const (
	labelApp       = "app"
	labelAppValue  = "sandbox"
	labelSandboxID = "sandbox-id"

	annoCreatedAt = "sandbox.duke-ece/created-at"
	annoTTL       = "sandbox.duke-ece/ttl-seconds"

	reaperInterval = time.Minute
)

// Manager implements v1.SandboxManagerServiceServer.
type Manager struct {
	v1.UnimplementedSandboxManagerServiceServer

	client       kubernetes.Interface
	namespace    string
	image        string
	maxSandboxes int
	defaultTTL   int64
}

// New builds a Manager from environment configuration:
//
//	SANDBOX_NAMESPACE    (default "pi-sandbox")
//	DEFAULT_IMAGE        (default "ghcr.io/duke-ece/sandbox-worker:latest")
//	MAX_SANDBOXES        (default 10)
//	DEFAULT_TTL_SECONDS  (default 900)
//
// It uses the in-cluster config when running in k8s, otherwise falls back to
// $KUBECONFIG / ~/.kube/config for local development.
func New() (*Manager, error) {
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
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build k8s client: %w", err)
	}
	return &Manager{
		client:       client,
		namespace:    envOr("SANDBOX_NAMESPACE", "pi-sandbox"),
		image:        envOr("DEFAULT_IMAGE", "ghcr.io/duke-ece/sandbox-worker:latest"),
		maxSandboxes: envInt("MAX_SANDBOXES", 10),
		defaultTTL:   int64(envInt("DEFAULT_TTL_SECONDS", 900)),
	}, nil
}

// Namespace returns the namespace sandboxes are created in.
func (m *Manager) Namespace() string { return m.namespace }

// CreateSandbox creates a sandbox Pod and returns its ID without waiting for
// it to become Ready.
func (m *Manager) CreateSandbox(ctx context.Context, req *v1.CreateSandboxRequest) (*v1.CreateSandboxResponse, error) {
	pods, err := m.client.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelApp + "=" + labelAppValue,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list sandboxes: %v", err)
	}
	if len(pods.Items) >= m.maxSandboxes {
		return nil, status.Errorf(codes.ResourceExhausted, "sandbox limit reached (%d)", m.maxSandboxes)
	}

	image := req.GetImage()
	if image == "" {
		image = m.image
	}
	ttl := req.GetTtlSeconds()
	if ttl <= 0 {
		ttl = m.defaultTTL
	}

	id := "sb-" + randomHex(3)
	automount := false
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: m.namespace,
			Labels: map[string]string{
				labelApp:       labelAppValue,
				labelSandboxID: id,
			},
			Annotations: map[string]string{
				annoCreatedAt: time.Now().UTC().Format(time.RFC3339),
				annoTTL:       strconv.FormatInt(ttl, 10),
			},
		},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: &automount,
			RestartPolicy:                corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:  "sandbox",
				Image: image,
			}},
		},
	}
	if _, err := m.client.CoreV1().Pods(m.namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return nil, status.Errorf(codes.Internal, "create sandbox pod: %v", err)
	}
	log.Printf("created sandbox %s (image=%s, ttl=%ds)", id, image, ttl)
	return &v1.CreateSandboxResponse{SandboxId: id}, nil
}

// DeleteSandbox deletes the sandbox Pod with the given ID. It returns as soon
// as the delete is accepted, without waiting for the Pod to disappear.
func (m *Manager) DeleteSandbox(ctx context.Context, req *v1.DeleteSandboxRequest) (*v1.DeleteSandboxResponse, error) {
	id := req.GetSandboxId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	pods, err := m.client.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSandboxID + "=" + id,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "find sandbox: %v", err)
	}
	if len(pods.Items) == 0 {
		return nil, status.Errorf(codes.NotFound, "sandbox %q not found", id)
	}
	if err := m.client.CoreV1().Pods(m.namespace).Delete(ctx, pods.Items[0].Name, metav1.DeleteOptions{}); err != nil {
		return nil, status.Errorf(codes.Internal, "delete sandbox: %v", err)
	}
	log.Printf("deleted sandbox %s", id)
	return &v1.DeleteSandboxResponse{}, nil
}

// RunReaper periodically deletes sandboxes that have outlived their TTL.
// It is a safety net for callers that forget to DeleteSandbox.
func (m *Manager) RunReaper() {
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()
	for range ticker.C {
		m.reapOnce()
	}
}

func (m *Manager) reapOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pods, err := m.client.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelApp + "=" + labelAppValue,
	})
	if err != nil {
		log.Printf("reaper: list sandboxes: %v", err)
		return
	}
	for _, pod := range pods.Items {
		created, err := time.Parse(time.RFC3339, pod.Annotations[annoCreatedAt])
		if err != nil {
			continue
		}
		ttl, err := strconv.ParseInt(pod.Annotations[annoTTL], 10, 64)
		if err != nil || ttl <= 0 {
			continue
		}
		if time.Since(created) > time.Duration(ttl)*time.Second {
			if err := m.client.CoreV1().Pods(m.namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
				log.Printf("reaper: delete %s: %v", pod.Name, err)
			} else {
				log.Printf("reaper: deleted expired sandbox %s", pod.Name)
			}
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
