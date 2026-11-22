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
func (s *fakeSandbox) Read(_ context.Context, p string, _ int) ([]byte, error) {
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
	mu        sync.Mutex
	creates   int
	sb        *fakeSandbox
	listed    map[string]string
	createErr error
}

type fakeWorkspace struct {
	restores      int
	checkpoints   int
	restoreErr    error
	checkpointErr error
}

func (w *fakeWorkspace) Restore(context.Context, string, Sandbox) error {
	w.restores++
	return w.restoreErr
}
func (w *fakeWorkspace) Checkpoint(context.Context, string, Sandbox) error {
	w.checkpoints++
	return w.checkpointErr
}

func TestWorkspaceRestoreAndCheckpointGateToolSuccess(t *testing.T) {
	ctx := context.Background()
	client := &fakeClient{sb: &fakeSandbox{id: "cube-1", files: map[string][]byte{}}}
	ws := &fakeWorkspace{}
	svc, err := New(client, Config{TemplateID: "test", Workspace: ws})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ensure(ctx, "sess-1", ""); err != nil {
		t.Fatal(err)
	}
	if ws.restores != 1 {
		t.Fatalf("restore count = %d", ws.restores)
	}
	if err := svc.Write(ctx, "sess-1", "file.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if ws.checkpoints != 0 {
		t.Fatal("tool published workspace before turn completed")
	}
	if err := svc.Checkpoint(ctx, "sess-1"); err != nil {
		t.Fatal(err)
	}
	if ws.checkpoints != 1 {
		t.Fatalf("checkpoint count = %d", ws.checkpoints)
	}
	ws.checkpointErr = errors.New("R2 unavailable")
	if _, _, err := svc.Run(ctx, "sess-1", "echo x", time.Second); err != nil {
		t.Fatal(err)
	}
	if err := svc.Checkpoint(ctx, "sess-1"); err == nil {
		t.Fatal("turn succeeded without checkpoint")
	}
	if ws.checkpoints != 2 {
		t.Fatalf("checkpoint count = %d", ws.checkpoints)
	}
	if client.sb.kills != 1 {
		t.Fatalf("failed checkpoint left sandbox running: kills=%d", client.sb.kills)
	}
	ws.checkpointErr = nil
	if _, err := svc.Ensure(ctx, "sess-1", ""); err != nil {
		t.Fatalf("restore after failed checkpoint: %v", err)
	}
	if ws.restores != 2 {
		t.Fatalf("restore count = %d", ws.restores)
	}
}

func TestRestoreFailureDoesNotExposeEmptyWorkspace(t *testing.T) {
	ctx := context.Background()
	client := &fakeClient{sb: &fakeSandbox{id: "cube-1", files: map[string][]byte{}}}
	ws := &fakeWorkspace{restoreErr: errors.New("R2 unavailable")}
	svc, err := New(client, Config{TemplateID: "test", Workspace: ws})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ensure(ctx, "sess-1", ""); err == nil {
		t.Fatal("restore failure ignored")
	}
	if client.sb.kills != 1 {
		t.Fatalf("sandbox cleanup count = %d", client.sb.kills)
	}
	ws.restoreErr = nil
	if _, err := svc.Ensure(ctx, "sess-1", ""); err != nil {
		t.Fatalf("retry after R2 recovery: %v", err)
	}
}

func (c *fakeClient) Create(_ context.Context, _, _ string, _ time.Duration) (Sandbox, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creates++
	if c.createErr != nil {
		return nil, c.createErr
	}
	return c.sb, nil
}
func (c *fakeClient) Connect(_ context.Context, _ string) (Sandbox, error) { return c.sb, nil }
func (c *fakeClient) List(context.Context) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.listed, nil
}

func TestEnsureRecoversCreateThatTimedOutAfterCubeStarted(t *testing.T) {
	client := &fakeClient{
		sb:        &fakeSandbox{id: "cube-1"},
		createErr: context.DeadlineExceeded,
		listed:    map[string]string{"sess-1": "cube-1"},
	}
	svc, err := New(client, Config{TemplateID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		state, err := svc.Ensure(context.Background(), "sess-1", "")
		if err != nil || state != StateReady {
			t.Fatalf("Ensure: %s %v", state, err)
		}
	}
	if client.creates != 1 {
		t.Fatalf("created %d times", client.creates)
	}
}

func TestEnsureDoesNotDuplicateUncertainCreate(t *testing.T) {
	client := &fakeClient{createErr: context.DeadlineExceeded, listed: map[string]string{}}
	svc, err := New(client, Config{TemplateID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ensure(context.Background(), "sess-1", ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Ensure: %v", err)
	}
	if _, err := svc.Ensure(context.Background(), "sess-1", ""); !errors.Is(err, ErrUnready) {
		t.Fatalf("second Ensure: %v", err)
	}
	if client.creates != 1 {
		t.Fatalf("created %d times", client.creates)
	}
}

func TestReconcileReleasesCapacityWithoutRecreatingLostWorkspace(t *testing.T) {
	ctx := context.Background()
	client := &fakeClient{sb: &fakeSandbox{id: "cube-1"}}
	svc, err := New(client, Config{TemplateID: "test", MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ensure(ctx, "old", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ensure(ctx, "new", ""); !errors.Is(err, ErrLimit) {
		t.Fatalf("limit: %v", err)
	}
	if err := svc.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	state, err := svc.Status("old")
	if err != nil || state != StateLost {
		t.Fatalf("old status: %s %v", state, err)
	}
	if _, err := svc.Ensure(ctx, "old", ""); !errors.Is(err, ErrUnready) {
		t.Fatalf("lost workspace: %v", err)
	}
	if _, err := svc.Ensure(ctx, "new", ""); err != nil {
		t.Fatalf("released capacity: %v", err)
	}
	if client.creates != 2 {
		t.Fatalf("created %d times", client.creates)
	}
}

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
