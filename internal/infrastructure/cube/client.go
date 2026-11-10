package cube

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/Duke-ECE/sandbox-manager/internal/sessioncube"
	cubesandbox "github.com/tencentcloud/CubeSandbox/sdk/go"
)

const sessionMetadataKey = "managed_agents_session_id"

type Client struct {
	sdk    *cubesandbox.Client
	domain string
}

func New(config cubesandbox.Config) *Client {
	return &Client{sdk: cubesandbox.NewClient(config), domain: config.SandboxDomain}
}

func (c *Client) Close() error { return c.sdk.Close() }

func (c *Client) Create(ctx context.Context, sessionID, templateID string, ttl time.Duration) (sessioncube.Sandbox, error) {
	sb, err := c.sdk.Create(ctx, cubesandbox.CreateOptions{
		TemplateID: templateID,
		Timeout:    cubesandbox.DurationPtr(ttl),
		Metadata:   map[string]string{sessionMetadataKey: sessionID},
	})
	if err != nil {
		return nil, err
	}
	if c.domain != "" {
		sb.Domain = c.domain
	}
	return &sandbox{sdk: sb}, nil
}

func (c *Client) Connect(ctx context.Context, id string) (sessioncube.Sandbox, error) {
	sb, err := c.sdk.Connect(ctx, id)
	if err != nil {
		if errors.Is(err, cubesandbox.ErrSandboxNotFound) {
			return nil, sessioncube.ErrNotFound
		}
		return nil, err
	}
	if c.domain != "" {
		sb.Domain = c.domain
	}
	return &sandbox{sdk: sb}, nil
}

func (c *Client) List(ctx context.Context) (map[string]string, error) {
	list, err := c.sdk.List(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string)
	for _, sb := range list {
		id := sb.Metadata[sessionMetadataKey]
		if id == "" || sb.SandboxID == "" {
			continue
		}
		if prior := result[id]; prior != "" && prior != sb.SandboxID {
			return nil, fmt.Errorf("multiple Cube sandboxes for session %q", id)
		}
		result[id] = sb.SandboxID
	}
	return result, nil
}

type sandbox struct{ sdk *cubesandbox.Sandbox }

func (s *sandbox) ID() string { return s.sdk.SandboxID }

func (s *sandbox) Run(ctx context.Context, command string, timeout time.Duration) (sessioncube.CommandResult, error) {
	res, err := s.sdk.Commands().Run(ctx, command, cubesandbox.CommandOptions{Timeout: timeout, Cwd: "/workspace", User: "root"})
	if err != nil {
		return sessioncube.CommandResult{}, err
	}
	return sessioncube.CommandResult{ExitCode: res.ExitCode, Stdout: res.Stdout, Stderr: res.Stderr}, nil
}

func (s *sandbox) Read(ctx context.Context, p string) ([]byte, error) {
	data, err := s.sdk.Files().Read(ctx, p)
	return []byte(data), err
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }

func (s *sandbox) Write(ctx context.Context, p string, data []byte) error {
	parent := path.Dir(p)
	result, err := s.sdk.Commands().Run(ctx, "mkdir -p -- "+shellQuote(parent), cubesandbox.CommandOptions{Timeout: 10 * time.Second, Cwd: "/workspace", User: "root"})
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("create parent directory: %s", result.Stderr)
	}
	return s.sdk.Files().Write(ctx, p, data)
}

func (s *sandbox) Kill(ctx context.Context) error {
	err := s.sdk.Kill(ctx)
	if errors.Is(err, cubesandbox.ErrSandboxNotFound) {
		return sessioncube.ErrNotFound
	}
	return err
}
