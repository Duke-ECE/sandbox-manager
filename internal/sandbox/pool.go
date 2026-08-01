package sandbox

import (
	"context"
	"log"
	"time"

	corev1 "k8s.io/api/core/v1"
)

const poolInterval = 5 * time.Second

// RunPoolController keeps poolSize ready sandboxes warm at all times, so
// Execute never pays pod scheduling and image pull latency on the request
// path. Pool phase is tracked on the pod's "pool" label:
// pending → ready → in-use → deleted.
func (s *Service) RunPoolController(ctx context.Context) {
	s.recoverPool(ctx)
	ticker := time.NewTicker(poolInterval)
	defer ticker.Stop()
	for {
		s.ReconcilePool(ctx)
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
func (s *Service) recoverPool(ctx context.Context) {
	orphans, err := s.ListPoolPods(ctx, PoolInUse)
	if err != nil {
		log.Printf("pool: list orphaned sandboxes: %v", err)
		return
	}
	for _, pod := range orphans {
		if err := s.store.DeletePod(ctx, pod.Name); err != nil {
			log.Printf("pool: delete orphan %s: %v", pod.Name, err)
		} else {
			log.Printf("pool: deleted orphaned sandbox %s", pod.Name)
		}
	}
}

// ReconcilePool promotes Running pending pods to ready and tops the pool up
// to poolSize ready+pending pods, never exceeding maxSandboxes in total.
func (s *Service) ReconcilePool(ctx context.Context) {
	pending, err := s.ListPoolPods(ctx, PoolPending)
	if err != nil {
		log.Printf("pool: list pending: %v", err)
		return
	}
	for _, pod := range pending {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		pod.Labels[labelPool] = PoolReady
		if err := s.store.UpdatePod(ctx, &pod); err != nil {
			log.Printf("pool: promote %s to ready: %v", pod.Name, err)
		}
	}

	ready, err := s.ListPoolPods(ctx, PoolReady)
	if err != nil {
		log.Printf("pool: list ready: %v", err)
		return
	}
	inUse, err := s.ListPoolPods(ctx, PoolInUse)
	if err != nil {
		log.Printf("pool: list in-use: %v", err)
		return
	}

	warm := int64(len(pending) + len(ready))
	total := warm + int64(len(inUse))
	for i := warm; i < s.poolSize && total < int64(s.maxSandboxes); i++ {
		if _, err := s.createPoolPod(ctx); err != nil {
			log.Printf("pool: create sandbox: %v", err)
			return
		}
		total++
	}
}

// createPoolPod creates one pending pool sandbox. The keep-alive command
// holds the pod open for later exec calls; the image default is not
// guaranteed to stay running (e.g. busybox exits immediately).
func (s *Service) createPoolPod(ctx context.Context) (string, error) {
	id := "sb-" + randomHex(3)
	pod := s.buildSandboxPod(id, s.image, map[string]string{labelPool: PoolPending}, []string{"sleep", "infinity"})
	if err := s.store.CreatePod(ctx, pod); err != nil {
		return "", err
	}
	return id, nil
}

// ListPoolPods returns the pool pods in the given phase (PoolPending,
// PoolReady, PoolInUse).
func (s *Service) ListPoolPods(ctx context.Context, phase string) ([]corev1.Pod, error) {
	return s.store.ListPods(ctx, labelApp+"="+labelAppValue+","+labelPool+"="+phase)
}

// --- executor-facing pool operations (the executor slice's Pool port) ---

// ClaimSandbox marks a ready pool pod in-use. The k8s error is returned raw
// so the caller can detect update conflicts (another Execute won the race).
func (s *Service) ClaimSandbox(ctx context.Context, pod *corev1.Pod) error {
	pod = pod.DeepCopy()
	pod.Labels[labelPool] = PoolInUse
	return s.store.UpdatePod(ctx, pod)
}

// DeletePod deletes a pod by name. Used by the executor to destroy a
// single-use sandbox after Execute.
func (s *Service) DeletePod(ctx context.Context, name string) error {
	return s.store.DeletePod(ctx, name)
}
