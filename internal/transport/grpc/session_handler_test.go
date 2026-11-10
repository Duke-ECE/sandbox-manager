package grpc

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	v3 "github.com/Duke-ECE/protos/gen/go/sandbox/v3"
	"github.com/Duke-ECE/sandbox-manager/internal/sessioncube"
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type sessionTestClient struct{ sb *sessionTestSandbox }

func (c *sessionTestClient) Create(context.Context, string, string, time.Duration) (sessioncube.Sandbox, error) {
	return c.sb, nil
}
func (c *sessionTestClient) Connect(context.Context, string) (sessioncube.Sandbox, error) {
	return c.sb, nil
}
func (c *sessionTestClient) List(context.Context) (map[string]string, error) { return nil, nil }

type sessionTestSandbox struct {
	mu    sync.Mutex
	files map[string][]byte
}

func (s *sessionTestSandbox) ID() string { return "cube-test" }
func (s *sessionTestSandbox) Run(context.Context, string, time.Duration) (sessioncube.CommandResult, error) {
	return sessioncube.CommandResult{Stdout: "ok", ExitCode: 0}, nil
}
func (s *sessionTestSandbox) Read(_ context.Context, p string, _ int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.files[p]...), nil
}
func (s *sessionTestSandbox) Write(_ context.Context, p string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[p] = append([]byte(nil), data...)
	return nil
}
func (s *sessionTestSandbox) Kill(context.Context) error { return nil }

func TestSessionV3OverGRPC(t *testing.T) {
	ctx := context.Background()
	svc, err := sessioncube.New(&sessionTestClient{sb: &sessionTestSandbox{files: make(map[string][]byte)}}, sessioncube.Config{TemplateID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	server := googlegrpc.NewServer()
	v3.RegisterSessionSandboxServiceServer(server, NewSessionHandler(svc))
	go func() { _ = server.Serve(lis) }()
	defer server.Stop()
	conn, err := googlegrpc.NewClient("passthrough:///bufnet", googlegrpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }), googlegrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := v3.NewSessionSandboxServiceClient(conn)
	if _, err := client.EnsureSessionSandbox(ctx, &v3.EnsureSessionSandboxRequest{SessionId: "sess-test"}); err != nil {
		t.Fatal(err)
	}
	put, err := client.PutResource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := put.Send(&v3.PutResourceRequest{SessionId: "sess-test", Path: "hello.txt", Chunk: []byte("hello ")}); err != nil {
		t.Fatal(err)
	}
	if err := put.Send(&v3.PutResourceRequest{Chunk: []byte("world")}); err != nil {
		t.Fatal(err)
	}
	putResult, err := put.CloseAndRecv()
	if err != nil || putResult.GetBytesWritten() != 11 {
		t.Fatalf("put: %v %v", putResult, err)
	}
	if _, err := client.ExecuteTool(ctx, &v3.ExecuteToolRequest{SessionId: "sess-test", Tool: &v3.ExecuteToolRequest_Edit{Edit: &v3.EditTool{Path: "hello.txt", Edits: []*v3.EditReplacement{{OldText: "hello", NewText: "hi"}, {OldText: "world", NewText: "Cube"}}}}}); err != nil {
		t.Fatal(err)
	}
	get, err := client.GetResource(ctx, &v3.GetResourceRequest{SessionId: "sess-test", Path: "hello.txt"})
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	for {
		reply, err := get.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, reply.GetChunk()...)
	}
	if string(got) != "hi Cube" {
		t.Fatalf("get: %q", got)
	}
	if _, err := client.CloseSessionSandbox(ctx, &v3.CloseSessionSandboxRequest{SessionId: "sess-test"}); err != nil {
		t.Fatal(err)
	}
}
