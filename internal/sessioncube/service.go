package sessioncube

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalid  = errors.New("invalid session sandbox request")
	ErrNotFound = errors.New("session sandbox not found")
	ErrLimit    = errors.New("session sandbox limit reached")
	ErrUnready  = errors.New("session sandbox is not ready")
)

type State string

const (
	StateAbsent   State = "absent"
	StateCreating State = "creating"
	StateReady    State = "ready"
	StateLost     State = "lost"
	StateClosing  State = "closing"
)

type CommandResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

// Sandbox is the subset of Cube operations owned by this slice.
type Sandbox interface {
	ID() string
	Run(context.Context, string, time.Duration) (CommandResult, error)
	Read(context.Context, string, int) ([]byte, error)
	Write(context.Context, string, []byte) error
	Kill(context.Context) error
}

type Client interface {
	Create(context.Context, string, string, time.Duration) (Sandbox, error)
	Connect(context.Context, string) (Sandbox, error)
	List(context.Context) (map[string]string, error) // session ID -> sandbox ID
}

type Config struct {
	TemplateID  string
	MaxSessions int
	TTL         time.Duration
	MaxBytes    int
	MaxOutput   int
	MaxRun      time.Duration
}

type binding struct {
	mu        sync.Mutex
	sandboxID string
	state     State
	ready     chan struct{}
}

type Service struct {
	client Client
	cfg    Config
	mu     sync.Mutex
	byID   map[string]*binding
	lost   map[string]struct{}
}

func New(client Client, cfg Config) (*Service, error) {
	if client == nil || cfg.TemplateID == "" {
		return nil, fmt.Errorf("%w: Cube client and template are required", ErrInvalid)
	}
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = 10
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 30 * time.Minute
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 1 << 20
	}
	if cfg.MaxOutput <= 0 {
		cfg.MaxOutput = 1 << 20
	}
	if cfg.MaxRun <= 0 {
		cfg.MaxRun = 60 * time.Second
	}
	return &Service{client: client, cfg: cfg, byID: make(map[string]*binding), lost: make(map[string]struct{})}, nil
}

// Recover rebuilds the single-replica pilot index from Cube sandbox metadata.
// Durable CAS-based bindings are required before multiple manager replicas.
func (s *Service) Recover(ctx context.Context) error {
	list, err := s.client.List(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for sessionID, sandboxID := range list {
		if sessionID == "" || sandboxID == "" {
			continue
		}
		if _, exists := s.byID[sessionID]; !exists {
			s.byID[sessionID] = &binding{sandboxID: sandboxID, state: StateReady}
		}
	}
	return nil
}

// Reconcile marks bindings lost when Cube no longer lists their sandbox.
// Keep the tombstone so a subsequent tool call cannot silently create an
// empty workspace for a transcript that still refers to the old files.
func (s *Service) Reconcile(ctx context.Context) error {
	list, err := s.client.List(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	bindings := make(map[string]*binding, len(s.byID))
	for id, b := range s.byID {
		bindings[id] = b
	}
	s.mu.Unlock()
	for sessionID, b := range bindings {
		if b.ready != nil {
			select {
			case <-b.ready:
			default:
				continue
			}
		}
		b.mu.Lock()
		missing := b.state == StateLost || (b.state == StateReady && list[sessionID] != b.sandboxID)
		if missing {
			b.state = StateLost
		}
		b.mu.Unlock()
		if missing {
			s.mu.Lock()
			if s.byID[sessionID] == b {
				delete(s.byID, sessionID)
				s.lost[sessionID] = struct{}{}
			}
			s.mu.Unlock()
		}
	}
	return nil
}

func validID(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func (s *Service) Ensure(ctx context.Context, sessionID, profileID string) (State, error) {
	if !validID(sessionID) || (profileID != "" && profileID != "default") {
		return StateAbsent, ErrInvalid
	}
	s.mu.Lock()
	if _, lost := s.lost[sessionID]; lost {
		s.mu.Unlock()
		return StateLost, ErrUnready
	}
	if b := s.byID[sessionID]; b != nil {
		s.mu.Unlock()
		if b.ready != nil {
			select {
			case <-ctx.Done():
				return StateAbsent, ctx.Err()
			case <-b.ready:
			}
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.state != StateReady {
			return b.state, ErrUnready
		}
		return b.state, nil
	}
	if len(s.byID) >= s.cfg.MaxSessions {
		s.mu.Unlock()
		return StateAbsent, ErrLimit
	}
	b := &binding{state: StateCreating, ready: make(chan struct{})}
	s.byID[sessionID] = b
	s.mu.Unlock()

	sb, err := s.client.Create(ctx, sessionID, s.cfg.TemplateID, s.cfg.TTL)
	b.mu.Lock()
	if err == nil {
		if sb == nil || sb.ID() == "" {
			err = fmt.Errorf("Cube returned no sandbox ID")
		} else {
			b.sandboxID, b.state = sb.ID(), StateReady
		}
	}
	close(b.ready)
	b.mu.Unlock()
	if err != nil {
		s.mu.Lock()
		delete(s.byID, sessionID)
		s.mu.Unlock()
		return StateAbsent, err
	}
	return StateReady, nil
}

func (s *Service) withSandbox(ctx context.Context, sessionID string, fn func(Sandbox) error) error {
	if !validID(sessionID) {
		return ErrInvalid
	}
	s.mu.Lock()
	b := s.byID[sessionID]
	_, lost := s.lost[sessionID]
	s.mu.Unlock()
	if b == nil {
		if lost {
			return ErrUnready
		}
		return ErrNotFound
	}
	if b.ready != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.ready:
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateReady {
		return ErrUnready
	}
	sb, err := s.client.Connect(ctx, b.sandboxID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			b.state = StateLost
		}
		return err
	}
	return fn(sb)
}

func (s *Service) Run(ctx context.Context, sessionID, command string, timeout time.Duration) (CommandResult, bool, error) {
	if strings.TrimSpace(command) == "" {
		return CommandResult{}, false, ErrInvalid
	}
	if timeout <= 0 || timeout > s.cfg.MaxRun {
		timeout = s.cfg.MaxRun
	}
	var result CommandResult
	err := s.withSandbox(ctx, sessionID, func(sb Sandbox) (err error) {
		result, err = sb.Run(ctx, command, timeout)
		return err
	})
	if err != nil {
		return CommandResult{}, false, err
	}
	truncated := false
	if len(result.Stdout) > s.cfg.MaxOutput {
		result.Stdout = result.Stdout[:s.cfg.MaxOutput]
		truncated = true
	}
	if len(result.Stderr) > s.cfg.MaxOutput {
		result.Stderr = result.Stderr[:s.cfg.MaxOutput]
		truncated = true
	}
	return result, truncated, nil
}

func workspacePath(p string) (string, error) {
	if p == "" || strings.ContainsRune(p, '\x00') {
		return "", ErrInvalid
	}
	if strings.HasPrefix(p, "/") {
		if p != "/workspace" && !strings.HasPrefix(p, "/workspace/") {
			return "", ErrInvalid
		}
	} else {
		p = "/workspace/" + p
	}
	clean := path.Clean(p)
	if clean == "/workspace" || !strings.HasPrefix(clean, "/workspace/") {
		return "", ErrInvalid
	}
	return clean, nil
}

func (s *Service) Read(ctx context.Context, sessionID, p string) ([]byte, error) {
	p, err := workspacePath(p)
	if err != nil {
		return nil, err
	}
	var data []byte
	err = s.withSandbox(ctx, sessionID, func(sb Sandbox) (err error) { data, err = sb.Read(ctx, p, s.cfg.MaxBytes); return err })
	if err != nil {
		return nil, err
	}
	if len(data) > s.cfg.MaxBytes {
		return nil, ErrLimit
	}
	return data, nil
}

func (s *Service) Write(ctx context.Context, sessionID, p string, data []byte) error {
	p, err := workspacePath(p)
	if err != nil {
		return err
	}
	if len(data) > s.cfg.MaxBytes {
		return ErrLimit
	}
	return s.withSandbox(ctx, sessionID, func(sb Sandbox) error { return sb.Write(ctx, p, data) })
}

type EditChange struct{ Old, New string }

func (s *Service) Edit(ctx context.Context, sessionID, p, old, replacement string) error {
	return s.EditMany(ctx, sessionID, p, []EditChange{{Old: old, New: replacement}})
}

func (s *Service) EditMany(ctx context.Context, sessionID, p string, changes []EditChange) error {
	if len(changes) == 0 {
		return ErrInvalid
	}
	p, err := workspacePath(p)
	if err != nil {
		return err
	}
	return s.withSandbox(ctx, sessionID, func(sb Sandbox) error {
		data, err := sb.Read(ctx, p, s.cfg.MaxBytes)
		if err != nil {
			return err
		}
		if len(data) > s.cfg.MaxBytes {
			return ErrLimit
		}
		original := string(data)
		type located struct {
			start, end  int
			replacement string
		}
		positions := make([]located, 0, len(changes))
		for _, change := range changes {
			if change.Old == "" || strings.Count(original, change.Old) != 1 {
				return ErrInvalid
			}
			start := strings.Index(original, change.Old)
			positions = append(positions, located{start: start, end: start + len(change.Old), replacement: change.New})
		}
		sort.Slice(positions, func(i, j int) bool { return positions[i].start < positions[j].start })
		for i := 1; i < len(positions); i++ {
			if positions[i].start < positions[i-1].end {
				return ErrInvalid
			}
		}
		updatedText := original
		for i := len(positions) - 1; i >= 0; i-- {
			p := positions[i]
			updatedText = updatedText[:p.start] + p.replacement + updatedText[p.end:]
		}
		updated := []byte(updatedText)
		if len(updated) > s.cfg.MaxBytes {
			return ErrLimit
		}
		return sb.Write(ctx, p, updated)
	})
}

func (s *Service) Close(ctx context.Context, sessionID string) error {
	if !validID(sessionID) {
		return ErrInvalid
	}
	s.mu.Lock()
	b := s.byID[sessionID]
	if b == nil {
		delete(s.lost, sessionID)
	}
	s.mu.Unlock()
	if b == nil {
		return nil
	}
	if b.ready != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.ready:
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateClosing {
		return nil
	}
	b.state = StateClosing
	sb, err := s.client.Connect(ctx, b.sandboxID)
	if err == nil {
		err = sb.Kill(ctx)
	}
	if errors.Is(err, ErrNotFound) {
		err = nil
	}
	if err != nil {
		b.state = StateReady
		return err
	}
	s.mu.Lock()
	delete(s.byID, sessionID)
	s.mu.Unlock()
	return nil
}

func (s *Service) Status(sessionID string) (State, error) {
	if !validID(sessionID) {
		return StateAbsent, ErrInvalid
	}
	s.mu.Lock()
	b := s.byID[sessionID]
	_, lost := s.lost[sessionID]
	s.mu.Unlock()
	if b == nil {
		if lost {
			return StateLost, nil
		}
		return StateAbsent, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state, nil
}

func (s *Service) MaxResourceBytes() int { return s.cfg.MaxBytes }
