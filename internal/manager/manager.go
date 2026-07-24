// Package manager implements the sandbox v1 (lifecycle) and v2 (tool-call
// execution) gRPC services.
//
// The manager is stateless: sandbox ID, creation time, TTL and pool phase
// all live on the k8s Pod's labels and annotations, so a restart loses
// nothing.
package manager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"maps"
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
	v2 "github.com/Duke-ECE/protos/gen/go/sandbox/v2"
)

const (
	labelApp       = "app"
	labelAppValue  = "sandbox"
	labelSandboxID = "sandbox-id"
	labelPool      = "pool"

	annoCreatedAt = "sandbox.duke-ece/created-at"
	annoTTL       = "sandbox.duke-ece/ttl-seconds"

	reaperInterval = time.Minute
)

// defaultTools maps tool names to the command prefix used to run their input.
var defaultTools = map[string][]string{
	"shell":  {"/bin/sh", "-c"},
	"python": {"python3", "-c"},
}

// Manager implements v1.SandboxManagerServiceServer and
// v2.SandboxExecutorServiceServer.
type Manager struct {
	v1.UnimplementedSandboxManagerServiceServer
	v2.UnimplementedSandboxExecutorServiceServer

	client     kubernetes.Interface
	restConfig *rest.Config // nil in tests; only used by the real exec path
	execFn     execFunc

	namespace    string
	image        string
	maxSandboxes int
	defaultTTL   int64

	poolSize    int64
	execTimeout int64 // seconds
	maxOutput   int64 // bytes
	tools       map[string][]string
}

// New builds a Manager from environment configuration:
//
//	SANDBOX_NAMESPACE     (default "pi-sandbox")
//	DEFAULT_IMAGE         (default "busybox:1.36" until a sandbox-worker image exists)
//	MAX_SANDBOXES         (default 10)
//	DEFAULT_TTL_SECONDS   (default 900)
//	POOL_SIZE             (default 3)   ready sandboxes kept warm for v2 Execute
//	EXEC_TIMEOUT_SECONDS  (default 60)  default per-call timeout, capped at 300
//	MAX_OUTPUT_BYTES      (default 1048576) per-stream output truncation
//	TOOLS                 (JSON, default {"shell":["/bin/sh","-c"],"python":["python3","-c"]})
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

	tools := maps.Clone(defaultTools)
	if v := os.Getenv("TOOLS"); v != "" {
		if err := json.Unmarshal([]byte(v), &tools); err != nil {
			return nil, fmt.Errorf("parse TOOLS: %w", err)
		}
	}

	m := &Manager{
		client:       client,
		restConfig:   cfg,
		namespace:    envOr("SANDBOX_NAMESPACE", "pi-sandbox"),
		image:        envOr("DEFAULT_IMAGE", "busybox:1.36"),
		maxSandboxes: envInt("MAX_SANDBOXES", 10),
		defaultTTL:   int64(envInt("DEFAULT_TTL_SECONDS", 900)),
		poolSize:     int64(envInt("POOL_SIZE", 3)),
		execTimeout:  int64(envInt("EXEC_TIMEOUT_SECONDS", 60)),
		maxOutput:    int64(envInt("MAX_OUTPUT_BYTES", 1<<20)),
		tools:        tools,
	}
	m.execFn = m.k8sExec
	return m, nil
}

// Namespace returns the namespace sandboxes are created in.
func (m *Manager) Namespace() string { return m.namespace }

// --- v1: explicit lifecycle (ops/debug only; runtimes use v2 Execute) ---

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
	pod := m.buildSandboxPod(id, image, nil, nil)
	pod.Annotations[annoTTL] = strconv.FormatInt(ttl, 10)
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
// It is a safety net for v1 callers that forget to DeleteSandbox. Pool pods
// (managed by the pool controller) are excluded.
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
		LabelSelector: labelApp + "=" + labelAppValue + ",!" + labelPool,
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

// --- shared helpers ---

// buildSandboxPod assembles the Pod spec shared by v1 and pool pods.
// command may be nil (image default) — pool pods pass a keep-alive command.
func (m *Manager) buildSandboxPod(id, image string, extraLabels map[string]string, command []string) *corev1.Pod {
	automount := false
	labels := map[string]string{
		labelApp:       labelAppValue,
		labelSandboxID: id,
	}
	maps.Copy(labels, extraLabels)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: m.namespace,
			Labels:    labels,
			Annotations: map[string]string{
				annoCreatedAt: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: &automount,
			RestartPolicy:                corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "sandbox",
				Image:   image,
				Command: command,
			}},
		},
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
