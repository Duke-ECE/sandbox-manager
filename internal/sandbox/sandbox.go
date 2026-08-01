// Package sandbox is the sandbox-lifecycle slice: v1 create/delete, the TTL
// reaper, and the warm pool that backs v2 Execute.
//
// The slice is stateless: sandbox ID, creation time, TTL and pool phase all
// live on the k8s Pod's labels and annotations, so a restart loses nothing.
// All k8s access goes through the PodStore port (store.go); the production
// implementation lives in internal/infrastructure/k8s.
package sandbox

import (
	"crypto/rand"
	"encoding/hex"
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	labelApp       = "app"
	labelAppValue  = "sandbox"
	labelSandboxID = "sandbox-id"
	labelPool      = "pool"

	annoCreatedAt = "sandbox.duke-ece/created-at"
	annoTTL       = "sandbox.duke-ece/ttl-seconds"
)

// Pool phases, tracked on the pod's "pool" label:
// pending → ready → in-use → deleted.
const (
	PoolPending = "pending"
	PoolReady   = "ready"
	PoolInUse   = "in-use"
)

// buildSandboxPod assembles the Pod spec shared by v1 and pool pods.
// command may be nil (image default) — pool pods pass a keep-alive command.
func (s *Service) buildSandboxPod(id, image string, extraLabels map[string]string, command []string) *corev1.Pod {
	automount := false
	labels := map[string]string{
		labelApp:       labelAppValue,
		labelSandboxID: id,
	}
	maps.Copy(labels, extraLabels)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      id,
			Namespace: s.namespace,
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

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
