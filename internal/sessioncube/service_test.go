package sessioncube

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeSandbox struct {
	id    string
	mu    sync.Mutex
	files map[string][]byte
	kills int
}

func (s *fakeSandbox) ID() string { return s.id }
func (s *fakeSandbox) Run(_ context.Context, command string, _ time.Duration) (CommandResult, error) {
	return CommandResult{Stdout: command, ExitCode: 0}, nil
}
func (s *fakeSandbox) Read(_ context.Context, p string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[p]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), data...), nil
}
func (s *fakeSandbox) Write(_ context.Context, p string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[p] = append([]byte(nil), data...)
	return nil
}
func (s *fakeSandbox) Kill(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kills++
	return nil
}

type fakeClient struct {
	mu      sync.Mutex
	creates int
	sb      *fakeSandbox
}

func (c *fakeClient) Create(_ context.Context, _, _ string, _ time.Duration) (Sandbox, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creates++
	return c.sb, nil
}
func (c *fakeClient) Connect(_ context.Context, _ string) (Sandbox, error) { return c.sb, nil }
func (c *fakeClient) List(context.Context) (map[string]string, error)      { return nil, nil }

func TestSessionLifecycleAndFiles(t *testing.T) {
	ctx := context.Background()
	client := &fakeClient{sb: &fakeSandbox{id: "cube-1", files: make(map[string][]byte)}}
	svc, err := New(client, Config{TemplateID: "test", MaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	const session = "sess-1"
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			state, err := svc.Ensure(ctx, session, "")
			if err != nil || state != StateReady {
				t.Errorf("Ensure: %v %v", state, err)
			}
		}()
	}
	wg.Wait()
	if client.creates != 1 {
		t.Fatalf("created %d times", client.creates)
	}
	if err := svc.Write(ctx, session, "note.txt", []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	if err := svc.Edit(ctx, session, "note.txt", "world", "Cube"); err != nil {
		t.Fatal(err)
	}
	if err := svc.EditMany(ctx, session, "note.txt", []EditChange{{Old: "hello", New: "hi"}, {Old: "Cube", New: "sandbox"}}); err != nil {
		t.Fatal(err)
	}
	data, err := svc.Read(ctx, session, "/workspace/note.txt")
	if err != nil || string(data) != "hi sandbox" {
		t.Fatalf("read %q: %v", data, err)
	}
	if err := svc.EditMany(ctx, session, "note.txt", []EditChange{{Old: "hi", New: "x"}, {Old: "hi sandbox", New: "y"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("overlap: %v", err)
	}
	if err := svc.Write(ctx, session, "../outside", []byte("bad")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("path escape: %v", err)
	}
	if err := svc.Close(ctx, session); err != nil {
		t.Fatal(err)
	}
	if err := svc.Close(ctx, session); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	if client.sb.kills != 1 {
		t.Fatalf("killed %d times", client.sb.kills)
	}
	state, err := svc.Status(session)
	if err != nil || state != StateAbsent {
		t.Fatalf("status %v: %v", state, err)
	}
}
