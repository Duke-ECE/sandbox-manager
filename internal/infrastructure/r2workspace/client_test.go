package r2workspace

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	v2 "github.com/Duke-ECE/protos/gen/go/session/v2"
	"github.com/Duke-ECE/sandbox-manager/internal/sessioncube"
	"github.com/Duke-ECE/sandbox-manager/internal/workspace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type localSandbox struct{}

func (localSandbox) ID() string { return "local" }
func (localSandbox) Run(ctx context.Context, command string, timeout time.Duration) (sessioncube.CommandResult, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	result := sessioncube.CommandResult{Stdout: stdout.String(), Stderr: stderr.String()}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		result.ExitCode = exit.ExitCode()
		return result, nil
	}
	return result, err
}
func (localSandbox) Read(context.Context, string, int) ([]byte, error) { panic("unused") }
func (localSandbox) Write(context.Context, string, []byte) error       { panic("unused") }
func (localSandbox) Kill(context.Context) error                        { return nil }

type fakeLedger struct {
	mu       sync.Mutex
	ws       *v2.WorkspaceRevision
	cleanup  []*v2.WorkspaceCleanupTarget
	finished []string
}

func (l *fakeLedger) ListWorkspacesForCleanup(context.Context, *v2.ListWorkspacesForCleanupRequest, ...grpc.CallOption) (*v2.ListWorkspacesForCleanupResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return &v2.ListWorkspacesForCleanupResponse{Workspaces: l.cleanup}, nil
}
func (l *fakeLedger) FinishWorkspaceDeletion(_ context.Context, req *v2.FinishWorkspaceDeletionRequest, _ ...grpc.CallOption) (*v2.FinishWorkspaceDeletionResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.finished = append(l.finished, req.SessionId)
	return &v2.FinishWorkspaceDeletionResponse{}, nil
}

func (l *fakeLedger) GetWorkspace(_ context.Context, req *v2.GetWorkspaceRequest, _ ...grpc.CallOption) (*v2.GetWorkspaceResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ws == nil {
		return &v2.GetWorkspaceResponse{Workspace: &v2.WorkspaceRevision{SessionId: req.SessionId}}, nil
	}
	return &v2.GetWorkspaceResponse{Workspace: proto.Clone(l.ws).(*v2.WorkspaceRevision)}, nil
}
func (l *fakeLedger) CommitWorkspace(_ context.Context, req *v2.CommitWorkspaceRequest, _ ...grpc.CallOption) (*v2.CommitWorkspaceResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	current := int64(0)
	if l.ws != nil {
		current = l.ws.Revision
	}
	if req.ExpectedRevision != current {
		return nil, errors.New("revision conflict")
	}
	l.ws = &v2.WorkspaceRevision{SessionId: req.SessionId, Revision: current + 1, ObjectKey: req.ObjectKey, Sha256: req.Sha256, SizeBytes: req.SizeBytes}
	return &v2.CommitWorkspaceResponse{Workspace: l.ws}, nil
}

type fakeObjects struct {
	server *httptest.Server
	mu     sync.Mutex
	data   map[string][]byte
	sha    map[string]string
}

func newFakeObjects(t *testing.T) *fakeObjects {
	t.Helper()
	o := &fakeObjects{data: make(map[string][]byte), sha: make(map[string]string)}
	o.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		o.mu.Lock()
		defer o.mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(io.LimitReader(r.Body, maxArchiveBytes+1))
			sum := md5.Sum(body)
			if base64.StdEncoding.EncodeToString(sum[:]) != r.Header.Get("Content-Md5") {
				http.Error(w, "md5 mismatch", http.StatusBadRequest)
				return
			}
			o.data[key], o.sha[key] = body, r.Header.Get("X-Amz-Meta-Sha256")
		case http.MethodGet:
			body, ok := o.data[key]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Write(body)
		default:
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(o.server.Close)
	return o
}

func (o *fakeObjects) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	return o.server.URL + "/" + key, nil
}
func (o *fakeObjects) PresignPut(_ context.Context, key, md5, sha string, _ time.Duration) (workspace.UploadURL, error) {
	return workspace.UploadURL{URL: o.server.URL + "/" + key, Headers: map[string]string{"Content-Md5": md5, "X-Amz-Meta-Sha256": sha}}, nil
}
func (o *fakeObjects) Head(_ context.Context, key string) (workspace.ObjectInfo, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	data, ok := o.data[key]
	if !ok {
		return workspace.ObjectInfo{}, errors.New("missing object")
	}
	return workspace.ObjectInfo{Size: int64(len(data)), SHA256: o.sha[key]}, nil
}
func (o *fakeObjects) ListSession(_ context.Context, sessionID string) ([]workspace.ListedObject, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []workspace.ListedObject
	for key := range o.data {
		if strings.HasPrefix(key, "sessions/"+sessionID+"/revisions/") {
			out = append(out, workspace.ListedObject{Key: key, LastModified: time.Now().Add(-48 * time.Hour)})
		}
	}
	return out, nil
}
func (o *fakeObjects) Delete(_ context.Context, key string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.data, key)
	delete(o.sha, key)
	return nil
}

func TestDirectCheckpointAndRestore(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "src"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "src", "hello.txt"), []byte("hello R2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	objects, ledger := newFakeObjects(t), &fakeLedger{}
	client, err := New(objects, ledger, "token")
	if err != nil {
		t.Fatal(err)
	}
	client.root = root
	if err := client.Checkpoint(context.Background(), "sess-test", localSandbox{}); err != nil {
		t.Fatal(err)
	}
	if ledger.ws == nil || ledger.ws.Revision != 1 {
		t.Fatalf("workspace not committed: %+v", ledger.ws)
	}
	if err := client.Checkpoint(context.Background(), "sess-test", localSandbox{}); err != nil {
		t.Fatal(err)
	}
	if ledger.ws.Revision != 1 {
		t.Fatalf("unchanged workspace made revision %d", ledger.ws.Revision)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := client.Restore(context.Background(), "sess-test", localSandbox{}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "src", "hello.txt"))
	if err != nil || string(got) != "hello R2\n" {
		t.Fatalf("restored file = %q, %v", got, err)
	}
	objects.mu.Lock()
	objects.data[ledger.ws.ObjectKey] = []byte("corrupt")
	objects.mu.Unlock()
	if err := client.Restore(context.Background(), "sess-test", localSandbox{}); err == nil {
		t.Fatal("corrupt archive accepted")
	}
}

func TestCleanupKeepsCurrentAndDeletesExpiredSessionObjects(t *testing.T) {
	objects, ledger := newFakeObjects(t), &fakeLedger{}
	client, err := New(objects, ledger, "token")
	if err != nil {
		t.Fatal(err)
	}
	current := "sessions/active/revisions/current.tar.gz"
	stale := "sessions/active/revisions/stale.tar.gz"
	deleted := "sessions/deleted/revisions/old.tar.gz"
	objects.mu.Lock()
	objects.data[current] = []byte("current")
	objects.data[stale] = []byte("stale")
	objects.data[deleted] = []byte("deleted")
	objects.mu.Unlock()
	ledger.cleanup = []*v2.WorkspaceCleanupTarget{
		{SessionId: "active", CurrentObjectKey: current},
		{SessionId: "deleted", CurrentObjectKey: deleted, DeletionRequestedAt: timestamppb.New(time.Now().Add(-11 * time.Minute))},
	}
	if err := client.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	objects.mu.Lock()
	_, hasCurrent := objects.data[current]
	_, hasStale := objects.data[stale]
	_, hasDeleted := objects.data[deleted]
	objects.mu.Unlock()
	if !hasCurrent || hasStale || hasDeleted {
		t.Fatalf("cleanup contents: current=%v stale=%v deleted=%v", hasCurrent, hasStale, hasDeleted)
	}
	if len(ledger.finished) != 1 || ledger.finished[0] != "deleted" {
		t.Fatalf("deletion receipts: %v", ledger.finished)
	}
}
