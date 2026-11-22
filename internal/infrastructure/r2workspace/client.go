// Package r2workspace coordinates direct sandbox-to-R2 archive transfers.
package r2workspace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	v2 "github.com/Duke-ECE/protos/gen/go/session/v2"
	"github.com/Duke-ECE/sandbox-manager/internal/sessioncube"
	"github.com/Duke-ECE/sandbox-manager/internal/workspace"
	"google.golang.org/grpc/metadata"
)

const (
	maxArchiveBytes = 256 << 20
	transferTimeout = 5 * time.Minute
)

type ObjectStore interface {
	PresignGet(context.Context, string, time.Duration) (string, error)
	PresignPut(context.Context, string, string, string, time.Duration) (workspace.UploadURL, error)
	Head(context.Context, string) (workspace.ObjectInfo, error)
	ListSession(context.Context, string) ([]workspace.ListedObject, error)
	Delete(context.Context, string) error
}

type Client struct {
	objects ObjectStore
	ledger  v2.WorkspaceServiceClient
	token   string
	root    string
}

func New(objects ObjectStore, ledger v2.WorkspaceServiceClient, token string) (*Client, error) {
	if objects == nil || ledger == nil || token == "" {
		return nil, errors.New("R2 store, workspace ledger and service token are required")
	}
	return &Client{objects: objects, ledger: ledger, token: token, root: "/workspace"}, nil
}

func (c *Client) auth(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-service-token", c.token)
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }

func runPython(ctx context.Context, sb sessioncube.Sandbox, script string, args ...string) (string, error) {
	command := "python3 -c " + quote(script)
	for _, arg := range args {
		command += " " + quote(arg)
	}
	result, err := sb.Run(ctx, command, transferTimeout)
	if err != nil {
		return "", errors.New("sandbox transfer command failed")
	}
	if result.ExitCode != 0 {
		// The script never prints the signed URL or credentials on failure.
		return "", fmt.Errorf("sandbox transfer failed: %s", strings.TrimSpace(result.Stderr))
	}
	return strings.TrimSpace(result.Stdout), nil
}

func (c *Client) Restore(ctx context.Context, sessionID string, sb sessioncube.Sandbox) error {
	res, err := c.ledger.GetWorkspace(c.auth(ctx), &v2.GetWorkspaceRequest{SessionId: sessionID})
	if err != nil {
		return fmt.Errorf("read committed workspace: %w", err)
	}
	ws := res.GetWorkspace()
	if ws == nil || ws.GetRevision() == 0 {
		return nil
	}
	if ws.GetSizeBytes() <= 0 || ws.GetSizeBytes() > maxArchiveBytes || len(ws.GetSha256()) != 64 ||
		!strings.HasPrefix(ws.GetObjectKey(), "sessions/"+sessionID+"/revisions/") {
		return errors.New("committed workspace metadata is invalid")
	}
	url, err := c.objects.PresignGet(ctx, ws.GetObjectKey(), 5*time.Minute)
	if err != nil {
		return fmt.Errorf("sign workspace download: %w", err)
	}
	_, err = runPython(ctx, sb, restoreScript, url, ws.GetSha256(), fmt.Sprint(ws.GetSizeBytes()), c.root)
	if err != nil {
		return fmt.Errorf("restore committed workspace: %w", err)
	}
	return nil
}

type snapshot struct {
	Path   string `json:"path"`
	MD5    string `json:"md5"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

func (c *Client) Checkpoint(ctx context.Context, sessionID string, sb sessioncube.Sandbox) error {
	res, err := c.ledger.GetWorkspace(c.auth(ctx), &v2.GetWorkspaceRequest{SessionId: sessionID})
	if err != nil {
		return fmt.Errorf("read workspace revision: %w", err)
	}
	current := res.GetWorkspace()
	if current == nil {
		return errors.New("workspace ledger returned no revision")
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return err
	}
	id := hex.EncodeToString(random)
	localPath := "/tmp/polyagent-" + id + ".tar.gz"
	infoJSON, err := runPython(ctx, sb, snapshotScript, c.root, localPath, fmt.Sprint(maxArchiveBytes))
	if err != nil {
		return fmt.Errorf("archive workspace: %w", err)
	}
	var info snapshot
	if err := json.Unmarshal([]byte(infoJSON), &info); err != nil {
		return fmt.Errorf("parse archive result: %w", err)
	}
	if info.Path != localPath || info.Size <= 0 || info.Size > maxArchiveBytes || len(info.SHA256) != 64 {
		return errors.New("sandbox returned invalid archive metadata")
	}
	if current.GetRevision() > 0 && current.GetSha256() == info.SHA256 && current.GetSizeBytes() == info.Size {
		_, err := runPython(ctx, sb, removeArchiveScript, localPath)
		return err
	}
	key := "sessions/" + sessionID + "/revisions/" + id + ".tar.gz"
	put, err := c.objects.PresignPut(ctx, key, info.MD5, info.SHA256, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("sign workspace upload: %w", err)
	}
	headersJSON, err := json.Marshal(put.Headers)
	if err != nil {
		return err
	}
	if _, err := runPython(ctx, sb, uploadScript, put.URL, localPath, string(headersJSON), fmt.Sprint(info.Size)); err != nil {
		return fmt.Errorf("upload workspace: %w", err)
	}
	remote, err := c.objects.Head(ctx, key)
	if err != nil {
		return fmt.Errorf("verify uploaded workspace: %w", err)
	}
	if remote.Size != info.Size || remote.SHA256 != info.SHA256 {
		return errors.New("uploaded workspace metadata differs from archive")
	}
	_, err = c.ledger.CommitWorkspace(c.auth(ctx), &v2.CommitWorkspaceRequest{
		SessionId: sessionID, ExpectedRevision: current.GetRevision(),
		ObjectKey: key, Sha256: info.SHA256, SizeBytes: info.Size, MutationId: id,
	})
	if err != nil {
		return fmt.Errorf("commit workspace revision: %w", err)
	}
	return nil
}

var _ sessioncube.Workspace = (*Client)(nil)

// Cleanup removes abandoned revisions after a recovery window and all files
// for sessions whose deletion intent has outlived every signed URL. The ledger
// row is removed only after R2 deletion succeeds, so failures are retried.
func (c *Client) Cleanup(ctx context.Context) error {
	var targets []*v2.WorkspaceCleanupTarget
	for offset := int32(0); ; offset += 100 {
		page, err := c.ledger.ListWorkspacesForCleanup(c.auth(ctx), &v2.ListWorkspacesForCleanupRequest{Limit: 100, Offset: offset})
		if err != nil {
			return fmt.Errorf("list workspace cleanup targets: %w", err)
		}
		targets = append(targets, page.GetWorkspaces()...)
		if !page.GetHasMore() {
			break
		}
	}
	now := time.Now()
	for _, target := range targets {
		deleting := target.GetDeletionRequestedAt() != nil
		if deleting && now.Sub(target.GetDeletionRequestedAt().AsTime()) < 10*time.Minute {
			continue
		}
		objects, err := c.objects.ListSession(ctx, target.GetSessionId())
		if err != nil {
			return err
		}
		for _, object := range objects {
			if !deleting {
				if object.Key == target.GetCurrentObjectKey() || now.Sub(object.LastModified) < 24*time.Hour {
					continue
				}
			}
			if err := c.objects.Delete(ctx, object.Key); err != nil {
				return fmt.Errorf("delete stale workspace object: %w", err)
			}
		}
		if deleting {
			if _, err := c.ledger.FinishWorkspaceDeletion(c.auth(ctx), &v2.FinishWorkspaceDeletionRequest{SessionId: target.GetSessionId()}); err != nil {
				return fmt.Errorf("finish workspace deletion: %w", err)
			}
		}
	}
	return nil
}
