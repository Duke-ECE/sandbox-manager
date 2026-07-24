package manager

import (
	"context"
	"log"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	poolPending = "pending"
	poolReady   = "ready"
	poolInUse   = "in-use"

	poolInterval = 5 * time.Second
)

// RunPoolController keeps poolSize ready sandboxes warm at all times, so
// Execute never pays pod scheduling and image pull latency on the request
// path. Pool phase is tracked on the pod's "pool" label:
// pending → ready → in-use → deleted.
func (m *Manager) RunPoolController(ctx context.Context) {
	m.recoverPool(ctx)
	ticker := time.NewTicker(poolInterval)
	defer ticker.Stop()
	for {
		m.reconcilePool(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// recoverPool runs once at startup: sandboxes left in-use by a previous
// manager incarnation are orphans (their Execute caller is gone) and are
// deleted before the pool is rebuilt.
func (m *Manager) recoverPool(ctx context.Context) {
	orphans, err := m.listPoolPods(ctx, poolInUse)
	if err != nil {
		log.Printf("pool: list orphaned sandboxes: %v", err)
		return
	}
	for _, pod := range orphans {
		if err := m.client.CoreV1().Pods(m.namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
			log.Printf("pool: delete orphan %s: %v", pod.Name, err)
		} else {
			log.Printf("pool: deleted orphaned sandbox %s", pod.Name)
		}
	}
}

// reconcilePool promotes Running pending pods to ready and tops the pool up
// to poolSize ready+pending pods, never exceeding maxSandboxes in total.
func (m *Manager) reconcilePool(ctx context.Context) {
	pending, err := m.listPoolPods(ctx, poolPending)
	if err != nil {
		log.Printf("pool: list pending: %v", err)
		return
	}
	for _, pod := range pending {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		pod.Labels[labelPool] = poolReady
		if _, err := m.client.CoreV1().Pods(m.namespace).Update(ctx, &pod, metav1.UpdateOptions{}); err != nil {
			log.Printf("pool: promote %s to ready: %v", pod.Name, err)
		}
	}

	ready, err := m.listPoolPods(ctx, poolReady)
	if err != nil {
		log.Printf("pool: list ready: %v", err)
		return
	}
	inUse, err := m.listPoolPods(ctx, poolInUse)
	if err != nil {
		log.Printf("pool: list in-use: %v", err)
		return
	}

	warm := int64(len(pending) + len(ready))
	total := warm + int64(len(inUse))
	for i := warm; i < m.poolSize && total < int64(m.maxSandboxes); i++ {
		if _, err := m.createPoolPod(ctx); err != nil {
			log.Printf("pool: create sandbox: %v", err)
			return
		}
		total++
	}
}

// createPoolPod creates one pending pool sandbox. The keep-alive command
// holds the pod open for later exec calls; the image default is not
// guaranteed to stay running (e.g. busybox exits immediately).
func (m *Manager) createPoolPod(ctx context.Context) (string, error) {
	id := "sb-" + randomHex(3)
	pod := m.buildSandboxPod(id, m.image, map[string]string{labelPool: poolPending}, []string{"sleep", "infinity"})
	if _, err := m.client.CoreV1().Pods(m.namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return "", err
	}
	return id, nil
}

func (m *Manager) listPoolPods(ctx context.Context, phase string) ([]corev1.Pod, error) {
	pods, err := m.client.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelApp + "=" + labelAppValue + "," + labelPool + "=" + phase,
	})
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}
