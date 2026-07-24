package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	v1 "github.com/Duke-ECE/protos/gen/go/sandbox/v1"
	v2 "github.com/Duke-ECE/protos/gen/go/sandbox/v2"
	"github.com/Duke-ECE/sandbox-manager/internal/manager"
)

func main() {
	mgr, err := manager.New()
	if err != nil {
		log.Fatalf("init manager: %v", err)
	}

	addr := ":50051"
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	s := grpc.NewServer()
	v1.RegisterSandboxManagerServiceServer(s, mgr)
	v2.RegisterSandboxExecutorServiceServer(s, mgr)
	reflection.Register(s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.RunReaper()
	go mgr.RunPoolController(ctx)

	go func() {
		log.Printf("sandbox-manager gRPC listening on %s (namespace=%s)", addr, mgr.Namespace())
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
