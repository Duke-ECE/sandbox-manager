package main

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"k8s.io/client-go/kubernetes"

	"github.com/Duke-ECE/sandbox-manager/internal/executor"
	"github.com/Duke-ECE/sandbox-manager/internal/infrastructure/k8s"
	"github.com/Duke-ECE/sandbox-manager/internal/sandbox"
	transportgrpc "github.com/Duke-ECE/sandbox-manager/internal/transport/grpc"
)

// Configuration (environment):
//
//	SANDBOX_NAMESPACE     (default "pi-sandbox")
//	DEFAULT_IMAGE         (default "busybox:1.36" until a sandbox-worker image exists)
//	MAX_SANDBOXES         (default 10)
//	DEFAULT_TTL_SECONDS   (default 900)
//	POOL_SIZE             (default 3)   ready sandboxes kept warm for v2 Execute
//	EXEC_TIMEOUT_SECONDS  (default 60)  default per-call timeout, capped at 300
//	MAX_OUTPUT_BYTES      (default 1048576) per-stream output truncation
//	TOOLS                 (JSON, default {"shell":["/bin/sh","-c"],"python":["python3","-c"]})
func main() {
	restCfg, err := k8s.LoadRESTConfig()
	if err != nil {
		log.Fatalf("init manager: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("init manager: build k8s client: %v", err)
	}

	namespace := envOr("SANDBOX_NAMESPACE", "pi-sandbox")
	image := envOr("DEFAULT_IMAGE", "busybox:1.36")
	maxSandboxes := envInt("MAX_SANDBOXES", 10)

	tools := maps.Clone(executor.DefaultTools)
	if v := os.Getenv("TOOLS"); v != "" {
		if err := json.Unmarshal([]byte(v), &tools); err != nil {
			log.Fatalf("init manager: parse TOOLS: %v", err)
		}
	}

	store := k8s.NewClient(clientset, restCfg, namespace)
	svc := sandbox.NewService(store, sandbox.Config{
		Namespace:    namespace,
		Image:        image,
		MaxSandboxes: maxSandboxes,
		DefaultTTL:   int64(envInt("DEFAULT_TTL_SECONDS", 900)),
		PoolSize:     int64(envInt("POOL_SIZE", 3)),
	})
	execSvc := executor.New(svc, store.Exec, executor.Config{
		MaxSandboxes: maxSandboxes,
		ExecTimeout:  int64(envInt("EXEC_TIMEOUT_SECONDS", 60)),
		MaxOutput:    int64(envInt("MAX_OUTPUT_BYTES", 1<<20)),
		Tools:        tools,
	})

	addr := ":50051"
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	s := transportgrpc.NewServer(svc, execSvc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go svc.RunReaper()
	go svc.RunPoolController(ctx)

	go func() {
		log.Printf("sandbox-manager gRPC listening on %s (namespace=%s)", addr, namespace)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")
	s.GracefulStop()
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
