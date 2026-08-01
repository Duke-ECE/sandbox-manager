package sandbox

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

const reaperInterval = time.Minute

// Config holds the slice's settings (from env in cmd/server).
type Config struct {
	Namespace    string // namespace sandboxes are created in
	Image        string // default sandbox image
	MaxSandboxes int    // total sandbox cap
	DefaultTTL   int64  // seconds, when the request doesn't specify one
	PoolSize     int64  // ready sandboxes kept warm for v2 Execute
}

// Service owns sandbox lifecycle (create/delete, TTL reaper) and the warm
// pool. It implements the executor slice's consumer-owned Pool interface.
type Service struct {
	store PodStore

	namespace    string
	image        string
	maxSandboxes int
	defaultTTL   int64
	poolSize     int64
}

// NewService wires the slice to its PodStore port.
func NewService(store PodStore, cfg Config) *Service {
	return &Service{
		store:        store,
		namespace:    cfg.Namespace,
		image:        cfg.Image,
		maxSandboxes: cfg.MaxSandboxes,
		defaultTTL:   cfg.DefaultTTL,
		poolSize:     cfg.PoolSize,
	}
}

// --- v1: explicit lifecycle (ops/debug only; runtimes use v2 Execute) ---

// CreateSandbox creates a sandbox Pod and returns its ID without waiting for
// it to become Ready. Empty image/ttl fall back to the configured defaults.
func (s *Service) CreateSandbox(ctx context.Context, image string, ttl int64) (string, error) {
	pods, err := s.store.ListPods(ctx, labelApp+"="+labelAppValue)
	if err != nil {
		return "", internal(fmt.Sprintf("list sandboxes: %v", err))
	}
	if len(pods) >= s.maxSandboxes {
		return "", resourceExhausted(fmt.Sprintf("sandbox limit reached (%d)", s.maxSandboxes))
	}

	if image == "" {
		image = s.image
	}
	if ttl <= 0 {
		ttl = s.defaultTTL
	}

	id := "sb-" + randomHex(3)
	pod := s.buildSandboxPod(id, image, nil, nil)
	pod.Annotations[annoTTL] = strconv.FormatInt(ttl, 10)
	if err := s.store.CreatePod(ctx, pod); err != nil {
		return "", internal(fmt.Sprintf("create sandbox pod: %v", err))
	}
	log.Printf("created sandbox %s (image=%s, ttl=%ds)", id, image, ttl)
	return id, nil
}

// DeleteSandbox deletes the sandbox Pod with the given ID. It returns as soon
// as the delete is accepted, without waiting for the Pod to disappear.
func (s *Service) DeleteSandbox(ctx context.Context, id string) error {
	if id == "" {
		return invalidArgument("sandbox_id is required")
	}
	pods, err := s.store.ListPods(ctx, labelSandboxID+"="+id)
	if err != nil {
		return internal(fmt.Sprintf("find sandbox: %v", err))
	}
	if len(pods) == 0 {
		return notFound(fmt.Sprintf("sandbox %q not found", id))
	}
	if err := s.store.DeletePod(ctx, pods[0].Name); err != nil {
		return internal(fmt.Sprintf("delete sandbox: %v", err))
	}
	log.Printf("deleted sandbox %s", id)
	return nil
}

// RunReaper periodically deletes sandboxes that have outlived their TTL.
// It is a safety net for v1 callers that forget to DeleteSandbox. Pool pods
// (managed by the pool controller) are excluded.
func (s *Service) RunReaper() {
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.reapOnce()
	}
}

func (s *Service) reapOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pods, err := s.store.ListPods(ctx, labelApp+"="+labelAppValue+",!"+labelPool)
	if err != nil {
		log.Printf("reaper: list sandboxes: %v", err)
		return
	}
	for _, pod := range pods {
		created, err := time.Parse(time.RFC3339, pod.Annotations[annoCreatedAt])
		if err != nil {
			continue
		}
		ttl, err := strconv.ParseInt(pod.Annotations[annoTTL], 10, 64)
		if err != nil || ttl <= 0 {
			continue
		}
		if time.Since(created) > time.Duration(ttl)*time.Second {
			if err := s.store.DeletePod(ctx, pod.Name); err != nil {
				log.Printf("reaper: delete %s: %v", pod.Name, err)
			} else {
				log.Printf("reaper: deleted expired sandbox %s", pod.Name)
			}
		}
	}
}
